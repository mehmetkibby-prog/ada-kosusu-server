import asyncio
import json
import os
import random
import string
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from urllib.parse import unquote

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Ada Kosusu Online Server", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

MAX_HITS = 3
ROOM_TTL_SECONDS = 60 * 60 * 2
RECONNECT_GRACE_SECONDS = 15
STATE_BROADCAST_INTERVAL = 0.20
BASE_TRAVEL_MS = 2800
MIN_OBSTACLE_INTERVAL_MS = 620
MAX_OBSTACLE_INTERVAL_MS = 980


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_room(code: str) -> str:
    return "".join(ch for ch in code.upper() if ch.isalnum())[:8]


def room_code(length: int = 5) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    for _ in range(100):
        candidate = "".join(random.choices(alphabet, k=length))
        if candidate not in rooms:
            return candidate
    return uuid.uuid4().hex[:length].upper()


@dataclass
class Obstacle:
    id: int
    lane: int
    kind: str
    spawn_at_ms: int
    impact_at_ms: int
    resolved_players: Set[str] = field(default_factory=set)

    def payload(self) -> dict:
        return {
            "id": self.id,
            "lane": self.lane,
            "kind": self.kind,
            "spawnAt": self.spawn_at_ms,
            "impactAt": self.impact_at_ms,
        }


@dataclass
class Player:
    id: str
    token: str
    name: str
    slot: int
    lane: int = 1
    hits: int = 0
    connected: bool = True
    ready: bool = False
    ws: Optional[WebSocket] = None
    disconnected_at: Optional[float] = None
    last_lane_change_at: float = 0.0
    joined_at: float = field(default_factory=time.time)

    def public(self, room: "Room") -> dict:
        distance = room.distance_m()
        return {
            "id": self.id,
            "name": self.name,
            "slot": self.slot,
            "lane": self.lane,
            "hits": self.hits,
            "maxHits": MAX_HITS,
            "connected": self.connected,
            "ready": self.ready,
            "isHost": self.slot == 1,
            "distance": distance,
        }


@dataclass
class Room:
    code: str
    players: Dict[str, Player] = field(default_factory=dict)
    status: str = "waiting"  # waiting/countdown/running/finished
    winner_id: Optional[str] = None
    finish_reason: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    start_at_ms: Optional[int] = None
    seed: int = field(default_factory=lambda: random.randint(1, 2_000_000_000))
    obstacle_index: int = 0
    next_spawn_at_ms: Optional[int] = None
    obstacles: List[Obstacle] = field(default_factory=list)
    loop_task: Optional[asyncio.Task] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def active_players(self) -> List[Player]:
        return [p for p in self.players.values() if p.connected and p.ws is not None]

    def distance_m(self) -> int:
        if not self.start_at_ms:
            return 0
        elapsed = max(0, now_ms() - self.start_at_ms) / 1000.0
        return int(elapsed * 9.5)

    def payload(self) -> dict:
        return {
            "type": "state",
            "serverNow": now_ms(),
            "roomCode": self.code,
            "status": self.status,
            "startAt": self.start_at_ms,
            "players": [p.public(self) for p in sorted(self.players.values(), key=lambda x: x.slot)],
            "winnerId": self.winner_id,
            "finishReason": self.finish_reason,
            "maxHits": MAX_HITS,
        }


rooms: Dict[str, Room] = {}
rooms_lock = asyncio.Lock()


@app.get("/")
async def root():
    return {
        "name": "Ada Kosusu Online Server",
        "ok": True,
        "websocket": "/ws?mode=quick|create|join&name=...&room=...&token=...",
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "rooms": len(rooms),
        "players": sum(len(r.players) for r in rooms.values()),
        "time": now_ms(),
    }


async def safe_send(player: Player, payload: dict) -> bool:
    if not player.ws or not player.connected:
        return False
    try:
        await player.ws.send_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return True
    except Exception:
        player.connected = False
        player.disconnected_at = time.time()
        player.ws = None
        return False


async def broadcast(room: Room, payload: dict):
    await asyncio.gather(*(safe_send(p, payload) for p in list(room.players.values())), return_exceptions=True)


async def broadcast_state(room: Room):
    room.updated_at = time.time()
    await broadcast(room, room.payload())


def difficulty_for(room: Room) -> tuple[int, int]:
    """Returns obstacle interval and travel time. Difficulty grows gradually."""
    if not room.start_at_ms:
        return MAX_OBSTACLE_INTERVAL_MS, BASE_TRAVEL_MS
    elapsed = max(0, now_ms() - room.start_at_ms) / 1000.0
    interval = int(MAX_OBSTACLE_INTERVAL_MS - min(360, elapsed * 4.0))
    interval = max(MIN_OBSTACLE_INTERVAL_MS, interval)
    travel = int(BASE_TRAVEL_MS - min(650, elapsed * 5.0))
    travel = max(2050, travel)
    return interval, travel


def choose_lane(room: Room) -> int:
    rng = random.Random(room.seed + room.obstacle_index * 7919)
    lane = rng.randint(0, 2)
    # Avoid excessively repetitive sequences.
    if len(room.obstacles) >= 2 and room.obstacles[-1].lane == room.obstacles[-2].lane == lane:
        lane = (lane + 1 + rng.randint(0, 1)) % 3
    return lane


def choose_kind(room: Room) -> str:
    kinds = ["crate", "rock", "barrel", "fallen_palm"]
    rng = random.Random(room.seed ^ (room.obstacle_index * 104729))
    return kinds[rng.randrange(len(kinds))]


async def start_match(room: Room):
    async with room.lock:
        if room.status in {"countdown", "running"}:
            return
        if len(room.active_players()) < 2:
            return
        room.status = "countdown"
        room.winner_id = None
        room.finish_reason = None
        room.start_at_ms = now_ms() + 3200
        room.seed = random.randint(1, 2_000_000_000)
        room.obstacle_index = 0
        room.next_spawn_at_ms = room.start_at_ms + 900
        room.obstacles.clear()
        for p in room.players.values():
            p.lane = 1
            p.hits = 0
            p.ready = False
        await broadcast(room, {
            "type": "match_start",
            "serverNow": now_ms(),
            "startAt": room.start_at_ms,
            "seed": room.seed,
            "roomCode": room.code,
        })
        await broadcast_state(room)

    if room.loop_task is None or room.loop_task.done():
        room.loop_task = asyncio.create_task(game_loop(room.code))


async def finish_match(room: Room, winner_id: Optional[str], reason: str):
    if room.status == "finished":
        return
    room.status = "finished"
    room.winner_id = winner_id
    room.finish_reason = reason
    await broadcast(room, {
        "type": "game_over",
        "winnerId": winner_id,
        "reason": reason,
        "serverNow": now_ms(),
    })
    await broadcast_state(room)


async def process_impacts(room: Room, current_ms: int):
    # Resolve all obstacle impacts that have reached the player line.
    for obstacle in list(room.obstacles):
        if obstacle.impact_at_ms > current_ms:
            continue
        for player in list(room.players.values()):
            if player.id in obstacle.resolved_players:
                continue
            obstacle.resolved_players.add(player.id)
            if room.status != "running":
                continue
            if player.lane == obstacle.lane:
                player.hits += 1
                await broadcast(room, {
                    "type": "hit",
                    "playerId": player.id,
                    "hits": player.hits,
                    "maxHits": MAX_HITS,
                    "obstacleId": obstacle.id,
                    "serverNow": current_ms,
                })
                if player.hits >= MAX_HITS:
                    opponents = [p for p in room.players.values() if p.id != player.id]
                    winner_id = opponents[0].id if opponents else None
                    await finish_match(room, winner_id, "three_hits")
                    return

        # Keep a little history then release memory.
        if current_ms - obstacle.impact_at_ms > 2500:
            try:
                room.obstacles.remove(obstacle)
            except ValueError:
                pass


async def process_disconnects(room: Room):
    if room.status != "running":
        return
    now = time.time()
    for player in room.players.values():
        if player.connected or player.disconnected_at is None:
            continue
        if now - player.disconnected_at >= RECONNECT_GRACE_SECONDS:
            opponents = [p for p in room.players.values() if p.id != player.id and p.connected]
            winner_id = opponents[0].id if opponents else None
            await finish_match(room, winner_id, "opponent_disconnected")
            return


async def game_loop(code: str):
    last_state = 0.0
    while True:
        room = rooms.get(code)
        if not room:
            return

        async with room.lock:
            current_ms = now_ms()
            if room.status == "countdown" and room.start_at_ms and current_ms >= room.start_at_ms:
                room.status = "running"
                await broadcast(room, {"type": "go", "serverNow": current_ms})

            if room.status == "running":
                await process_disconnects(room)
                if room.status != "running":
                    pass
                else:
                    interval_ms, travel_ms = difficulty_for(room)
                    while room.next_spawn_at_ms and current_ms >= room.next_spawn_at_ms:
                        lane = choose_lane(room)
                        kind = choose_kind(room)
                        obstacle = Obstacle(
                            id=room.obstacle_index,
                            lane=lane,
                            kind=kind,
                            spawn_at_ms=room.next_spawn_at_ms,
                            impact_at_ms=room.next_spawn_at_ms + travel_ms,
                        )
                        room.obstacles.append(obstacle)
                        room.obstacle_index += 1
                        room.next_spawn_at_ms += interval_ms
                        await broadcast(room, {
                            "type": "obstacle",
                            "serverNow": current_ms,
                            **obstacle.payload(),
                        })
                    await process_impacts(room, current_ms)

            monotonic = time.monotonic()
            if monotonic - last_state >= STATE_BROADCAST_INTERVAL:
                await broadcast_state(room)
                last_state = monotonic

            if room.status == "finished":
                return

        await asyncio.sleep(0.045)


async def find_or_create_room(mode: str, requested: str) -> Room:
    async with rooms_lock:
        if mode == "create":
            code = room_code()
            room = Room(code=code)
            rooms[code] = room
            return room

        if mode == "join":
            code = normalize_room(requested)
            if not code or code not in rooms:
                raise ValueError("Oda bulunamadı.")
            return rooms[code]

        # quick match: oldest waiting room with one player, otherwise create.
        candidates = [
            r for r in rooms.values()
            if r.status == "waiting" and len(r.players) == 1 and len(r.active_players()) == 1
        ]
        if candidates:
            return sorted(candidates, key=lambda r: r.created_at)[0]
        code = room_code()
        room = Room(code=code)
        rooms[code] = room
        return room


async def add_or_reconnect_player(room: Room, websocket: WebSocket, name: str, token: str) -> Player:
    async with room.lock:
        if token:
            for p in room.players.values():
                if p.token == token:
                    p.ws = websocket
                    p.connected = True
                    p.disconnected_at = None
                    p.name = name or p.name
                    return p

        if len(room.players) >= 2:
            raise ValueError("Bu oda dolu.")

        used_slots = {p.slot for p in room.players.values()}
        slot = 1 if 1 not in used_slots else 2
        player = Player(
            id=uuid.uuid4().hex,
            token=uuid.uuid4().hex,
            name=(name or f"Oyuncu {slot}")[:18],
            slot=slot,
            ws=websocket,
        )
        room.players[player.id] = player
        return player


@app.websocket("/ws")
async def websocket_game(websocket: WebSocket):
    await websocket.accept()

    mode = (websocket.query_params.get("mode") or "quick").lower()
    requested_room = websocket.query_params.get("room") or ""
    name = unquote(websocket.query_params.get("name") or "Oyuncu").strip()[:18]
    token = websocket.query_params.get("token") or ""

    if mode not in {"quick", "create", "join"}:
        await websocket.send_json({"type": "error", "message": "Geçersiz bağlantı modu."})
        await websocket.close(code=1008)
        return

    try:
        room = await find_or_create_room(mode, requested_room)
        player = await add_or_reconnect_player(room, websocket, name, token)
    except ValueError as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=1008)
        return

    await safe_send(player, {
        "type": "welcome",
        "playerId": player.id,
        "playerToken": player.token,
        "roomCode": room.code,
        "slot": player.slot,
        "serverNow": now_ms(),
    })
    await broadcast_state(room)


    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            action = msg.get("action")

            if action == "lane":
                requested_lane = int(msg.get("lane", player.lane))
                requested_lane = max(0, min(2, requested_lane))
                current = time.monotonic()
                if current - player.last_lane_change_at < 0.075:
                    continue
                # Prevent teleporting from far left to far right in one network input.
                if abs(requested_lane - player.lane) > 1:
                    requested_lane = player.lane + (1 if requested_lane > player.lane else -1)
                player.lane = max(0, min(2, requested_lane))
                player.last_lane_change_at = current

            elif action == "ready":
                if room.status == "waiting":
                    player.ready = bool(msg.get("ready", not player.ready))
                    await broadcast_state(room)

            elif action == "start":
                if room.status == "waiting":
                    if player.slot != 1:
                        await safe_send(player, {"type": "error", "message": "Oyunu yalnızca oda sahibi başlatabilir."})
                    elif len(room.active_players()) < 2:
                        await safe_send(player, {"type": "error", "message": "Başlamak için iki oyuncu gerekli."})
                    elif not all(p.ready for p in room.active_players()):
                        await safe_send(player, {"type": "error", "message": "İki oyuncunun da Hazırım demesi gerekiyor."})
                    else:
                        await start_match(room)

            elif action == "ping":
                await safe_send(player, {
                    "type": "pong",
                    "clientTs": msg.get("ts"),
                    "serverNow": now_ms(),
                })

            elif action == "rematch":
                async with room.lock:
                    if room.status == "finished" and len(room.active_players()) >= 2:
                        room.status = "waiting"
                        room.winner_id = None
                        room.finish_reason = None
                        room.start_at_ms = None
                        for p in room.players.values():
                            p.hits = 0
                            p.lane = 1
                            p.ready = False
                        await broadcast_state(room)
                await start_match(room)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        async with room.lock:
            if player.ws is websocket:
                player.connected = False
                player.disconnected_at = time.time()
                player.ws = None
                await broadcast_state(room)


async def cleanup_loop():
    while True:
        await asyncio.sleep(60)
        cutoff = time.time() - ROOM_TTL_SECONDS
        async with rooms_lock:
            stale = [code for code, room in rooms.items() if room.updated_at < cutoff]
            for code in stale:
                task = rooms[code].loop_task
                if task and not task.done():
                    task.cancel()
                rooms.pop(code, None)


@app.on_event("startup")
async def startup():
    asyncio.create_task(cleanup_loop())
