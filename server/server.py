import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from urllib.parse import unquote

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Ada Kosusu Online Server", version="3.0.0")
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
STATE_BROADCAST_INTERVAL = 0.18
JUMP_DURATION_MS = 900
SLIDE_DURATION_MS = 820
ACTION_COOLDOWN_MS = 390
INPUT_TIME_CLAMP_MS = 220


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
    wave_id: int
    lane: int
    kind: str
    avoid: str
    spawn_at_ms: int
    impact_at_ms: int
    resolved_players: Set[str] = field(default_factory=set)

    def payload(self) -> dict:
        return {
            "id": self.id,
            "waveId": self.wave_id,
            "lane": self.lane,
            "kind": self.kind,
            "avoid": self.avoid,
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
    last_action_at_ms: int = 0
    jump_started_ms: int = 0
    jump_until_ms: int = 0
    slide_started_ms: int = 0
    slide_until_ms: int = 0
    joined_at: float = field(default_factory=time.time)

    def pose_at(self, at_ms: int) -> str:
        if self.jump_started_ms <= at_ms <= self.jump_until_ms:
            return "jump"
        if self.slide_started_ms <= at_ms <= self.slide_until_ms:
            return "slide"
        return "run"

    def public(self, room: "Room") -> dict:
        server_now = now_ms()
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
            "distance": room.distance_m(),
            "pose": self.pose_at(server_now),
        }


@dataclass
class Room:
    code: str
    players: Dict[str, Player] = field(default_factory=dict)
    status: str = "waiting"
    winner_id: Optional[str] = None
    finish_reason: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    start_at_ms: Optional[int] = None
    seed: int = field(default_factory=lambda: random.randint(1, 2_000_000_000))
    obstacle_index: int = 0
    wave_index: int = 0
    next_spawn_at_ms: Optional[int] = None
    obstacles: List[Obstacle] = field(default_factory=list)
    last_wave_style: str = ""
    loop_task: Optional[asyncio.Task] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def active_players(self) -> List[Player]:
        return [p for p in self.players.values() if p.connected and p.ws is not None]

    def elapsed_s(self) -> float:
        if not self.start_at_ms:
            return 0.0
        return max(0.0, (now_ms() - self.start_at_ms) / 1000.0)

    def distance_m(self) -> int:
        e = self.elapsed_s()
        return int(e * 10.0 + min(180.0, e * e * 0.025))

    def difficulty_stage(self) -> int:
        e = self.elapsed_s()
        if e < 18:
            return 1
        if e < 38:
            return 2
        if e < 62:
            return 3
        return 4

    def speed_multiplier(self) -> float:
        e = self.elapsed_s()
        return min(1.85, 1.0 + e * 0.0105)

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
            "difficulty": self.difficulty_stage(),
            "speedMultiplier": self.speed_multiplier(),
        }


rooms: Dict[str, Room] = {}
rooms_lock = asyncio.Lock()


@app.get("/")
async def root():
    return {
        "name": "Ada Kosusu Online Server V3",
        "version": "3.0.0",
        "ok": True,
        "websocket": "/ws?mode=quick|create|join&name=...&room=...&token=...",
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "version": "3.0.0",
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


def difficulty_for(room: Room) -> tuple[int, int, int]:
    """wave interval ms, obstacle travel ms, stage"""
    stage = room.difficulty_stage()
    e = room.elapsed_s()
    if stage == 1:
        interval = int(max(900, 1120 - e * 7))
        travel = int(max(2450, 2820 - e * 15))
    elif stage == 2:
        interval = int(max(720, 880 - (e - 18) * 7))
        travel = int(max(2050, 2400 - (e - 18) * 14))
    elif stage == 3:
        interval = int(max(580, 700 - (e - 38) * 5))
        travel = int(max(1720, 2020 - (e - 38) * 12))
    else:
        interval = int(max(490, 570 - (e - 62) * 1.2))
        travel = int(max(1480, 1700 - (e - 62) * 4.0))
    return interval, travel, stage


def rng_for(room: Room, salt: int) -> random.Random:
    return random.Random(room.seed ^ (room.wave_index * 104729) ^ salt)


def kind_for_avoid(avoid: str, rng: random.Random) -> str:
    if avoid == "jump":
        return rng.choice(["crate", "fallen_palm", "stone_hurdle"])
    if avoid == "slide":
        return rng.choice(["low_gate", "hanging_vines"])
    return rng.choice(["barrel", "totem", "boulder"])


def build_wave(room: Room, spawn_at: int, travel_ms: int, stage: int) -> tuple[List[Obstacle], int]:
    rng = rng_for(room, 991)
    roll = rng.random()
    entries: List[tuple[int, str, str]] = []
    style = "single"

    # V3 difficulty: readable at first, then two-lane traps and compulsory jump/slide gates.
    if stage == 1 or roll < (0.46 if stage == 2 else 0.28 if stage == 3 else 0.18):
        lane = rng.randrange(3)
        avoid = rng.choices(["jump", "slide", "dodge"], weights=[44, 23, 33])[0]
        entries.append((lane, kind_for_avoid(avoid, rng), avoid))
    elif roll < (0.90 if stage == 2 else 0.78 if stage == 3 else 0.68):
        style = "double"
        safe_lane = rng.randrange(3)
        blocked = [lane for lane in range(3) if lane != safe_lane]
        for lane in blocked:
            avoid = rng.choices(["jump", "slide", "dodge"], weights=[38, 26, 36])[0]
            entries.append((lane, kind_for_avoid(avoid, rng), avoid))
    else:
        # Full-width skill gate. It cannot be solved by lane switching; player must jump/slide.
        style = "gate"
        avoid = rng.choice(["jump", "slide"])
        kind = "stone_hurdle" if avoid == "jump" else "low_gate"
        for lane in range(3):
            entries.append((lane, kind, avoid))

    # Don't fire two compulsory gates almost back-to-back.
    extra_gap = 0
    if style == "gate":
        extra_gap = 300 if stage <= 2 else 210
        if room.last_wave_style == "gate":
            extra_gap += 260
    room.last_wave_style = style

    obstacles: List[Obstacle] = []
    wave_id = room.wave_index
    for lane, kind, avoid in entries:
        obstacles.append(Obstacle(
            id=room.obstacle_index,
            wave_id=wave_id,
            lane=lane,
            kind=kind,
            avoid=avoid,
            spawn_at_ms=spawn_at,
            impact_at_ms=spawn_at + travel_ms,
        ))
        room.obstacle_index += 1
    room.wave_index += 1
    return obstacles, extra_gap


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
        room.wave_index = 0
        room.last_wave_style = ""
        room.next_spawn_at_ms = room.start_at_ms + 850
        room.obstacles.clear()
        for p in room.players.values():
            p.lane = 1
            p.hits = 0
            p.ready = False
            p.jump_started_ms = p.jump_until_ms = 0
            p.slide_started_ms = p.slide_until_ms = 0
            p.last_action_at_ms = 0
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


def obstacle_hits_player(obstacle: Obstacle, player: Player) -> bool:
    if player.lane != obstacle.lane:
        return False
    pose = player.pose_at(obstacle.impact_at_ms)
    if obstacle.avoid == "jump" and pose == "jump":
        return False
    if obstacle.avoid == "slide" and pose == "slide":
        return False
    return True


async def process_impacts(room: Room, current_ms: int):
    for obstacle in list(room.obstacles):
        if obstacle.impact_at_ms > current_ms:
            continue
        for player in list(room.players.values()):
            if player.id in obstacle.resolved_players:
                continue
            obstacle.resolved_players.add(player.id)
            if room.status != "running":
                continue
            if obstacle_hits_player(obstacle, player):
                player.hits += 1
                await broadcast(room, {
                    "type": "hit",
                    "playerId": player.id,
                    "hits": player.hits,
                    "maxHits": MAX_HITS,
                    "obstacleId": obstacle.id,
                    "avoid": obstacle.avoid,
                    "serverNow": current_ms,
                })
                if player.hits >= MAX_HITS:
                    opponents = [p for p in room.players.values() if p.id != player.id]
                    winner_id = opponents[0].id if opponents else None
                    await finish_match(room, winner_id, "three_hits")
                    return

        if current_ms - obstacle.impact_at_ms > 2500:
            try:
                room.obstacles.remove(obstacle)
            except ValueError:
                pass


async def process_disconnects(room: Room):
    if room.status != "running":
        return
    stamp = time.time()
    for player in room.players.values():
        if player.connected or player.disconnected_at is None:
            continue
        if stamp - player.disconnected_at >= RECONNECT_GRACE_SECONDS:
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
                if room.status == "running":
                    interval_ms, travel_ms, stage = difficulty_for(room)
                    while room.next_spawn_at_ms and current_ms >= room.next_spawn_at_ms:
                        spawn_at = room.next_spawn_at_ms
                        wave, extra_gap = build_wave(room, spawn_at, travel_ms, stage)
                        room.obstacles.extend(wave)
                        room.next_spawn_at_ms += interval_ms + extra_gap
                        for obstacle in wave:
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

        await asyncio.sleep(0.035)


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


def clamped_input_time(message_value, server_now: int) -> int:
    try:
        claimed = int(message_value)
    except (TypeError, ValueError):
        return server_now
    return max(server_now - INPUT_TIME_CLAMP_MS, min(server_now + 40, claimed))


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
        "serverVersion": "3.0.0",
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
                if room.status != "running":
                    continue
                requested_lane = int(msg.get("lane", player.lane))
                requested_lane = max(0, min(2, requested_lane))
                current = time.monotonic()
                if current - player.last_lane_change_at < 0.070:
                    continue
                if abs(requested_lane - player.lane) > 1:
                    requested_lane = player.lane + (1 if requested_lane > player.lane else -1)
                player.lane = max(0, min(2, requested_lane))
                player.last_lane_change_at = current

            elif action in {"jump", "slide"}:
                if room.status != "running":
                    continue
                server_now = now_ms()
                event_ms = clamped_input_time(msg.get("eventTime"), server_now)
                if event_ms - player.last_action_at_ms < ACTION_COOLDOWN_MS:
                    continue
                player.last_action_at_ms = event_ms
                if action == "jump":
                    player.jump_started_ms = event_ms
                    player.jump_until_ms = event_ms + JUMP_DURATION_MS
                    player.slide_started_ms = player.slide_until_ms = 0
                else:
                    player.slide_started_ms = event_ms
                    player.slide_until_ms = event_ms + SLIDE_DURATION_MS
                    player.jump_started_ms = player.jump_until_ms = 0
                await broadcast(room, {
                    "type": "pose",
                    "playerId": player.id,
                    "pose": action,
                    "startedAt": event_ms,
                    "until": player.jump_until_ms if action == "jump" else player.slide_until_ms,
                    "serverNow": server_now,
                })

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
                            p.jump_started_ms = p.jump_until_ms = 0
                            p.slide_started_ms = p.slide_until_ms = 0
                        await broadcast_state(room)

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
