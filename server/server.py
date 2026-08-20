import asyncio
import json
import random
import string
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from urllib.parse import unquote

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title='Ada Serveti Online Server', version='1.0.0')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

START_MONEY = 1500
PASS_START_BONUS = 200
MAX_PLAYERS = 4
ROOM_TTL = 7200

BOARD = [
    {'id':0,'name':'BAŞLANGIÇ','type':'start'},
    {'id':1,'name':'Girne Limanı','type':'property','price':180,'rent':35},
    {'id':2,'name':'ŞANS','type':'chance'},
    {'id':3,'name':'Lefkoşa Çarşı','type':'property','price':200,'rent':40},
    {'id':4,'name':'Ada Vergisi','type':'tax','amount':100},
    {'id':5,'name':'Bellapais','type':'property','price':220,'rent':45},
    {'id':6,'name':'ŞANS','type':'chance'},
    {'id':7,'name':'Karpaz Sahili','type':'property','price':240,'rent':50},
    {'id':8,'name':'Sahil Molası','type':'rest'},
    {'id':9,'name':'Mağusa Surları','type':'property','price':260,'rent':55},
    {'id':10,'name':'ŞANS','type':'chance'},
    {'id':11,'name':'Salamis','type':'property','price':280,'rent':60},
    {'id':12,'name':'Ada Festivali','type':'bonus','amount':100},
    {'id':13,'name':'Güzelyurt Bahçeleri','type':'property','price':300,'rent':65},
    {'id':14,'name':'Bakım Vergisi','type':'tax','amount':120},
    {'id':15,'name':'ŞANS','type':'chance'},
    {'id':16,'name':'Lefke Sahili','type':'property','price':320,'rent':70},
    {'id':17,'name':'Fırtınalı Liman','type':'penalty'},
    {'id':18,'name':'St. Hilarion','type':'property','price':340,'rent':75},
    {'id':19,'name':'ŞANS','type':'chance'},
    {'id':20,'name':'Akdeniz Koyu','type':'property','price':360,'rent':80},
    {'id':21,'name':'Turizm Sezonu','type':'bonus','amount':120},
    {'id':22,'name':'Koruçam','type':'property','price':380,'rent':90},
    {'id':23,'name':'ŞANS','type':'chance'},
]

CHANCE_CARDS = [
    ('Ada Festivali', 'Konser organizasyonundan 200 ₳ kazandın!', 'money', 200),
    ('Ani Fırtına', 'Teknen hasar gördü. 150 ₳ öde.', 'money', -150),
    ('Vergi İadesi', 'Belediyeden 100 ₳ iade aldın.', 'money', 100),
    ('Turist Akını', 'Her rakip sana 75 ₳ öder.', 'collect', 75),
    ('Hızlı Feribot', '3 kare ileri git!', 'move', 3),
    ('Kira Sigortası', 'Bir sonraki kira ödemen ücretsiz.', 'shield', 1),
    ('Liman Cezası', 'Bir sonraki turunu kaçırırsın.', 'skip', 1),
    ('Şanslı Zar', 'Bu turdan sonra bir kez daha zar at!', 'extra', 1),
    ('Tamirat Masrafı', 'Sahip olduğun her mülk için 50 ₳ öde.', 'repair', 50),
    ('Ada Hibesi', 'Yerel kalkınma hibesi: 180 ₳ kazandın.', 'money', 180),
]


def now_ms(): return int(time.time()*1000)

def make_code():
    alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    for _ in range(100):
        c=''.join(random.choices(alphabet,k=5))
        if c not in rooms: return c
    return uuid.uuid4().hex[:5].upper()

@dataclass
class Player:
    id: str
    token: str
    name: str
    slot: int
    ws: Optional[WebSocket] = None
    ready: bool = False
    connected: bool = True
    money: int = START_MONEY
    position: int = 0
    skip_turns: int = 0
    rent_shields: int = 0
    extra_roll: bool = False
    eliminated: bool = False

@dataclass
class Room:
    code: str
    players: Dict[str,Player] = field(default_factory=dict)
    status: str = 'waiting'
    host_id: Optional[str] = None
    turn_order: List[str] = field(default_factory=list)
    current_turn: int = 0
    last_roll: Optional[int] = None
    last_roller: Optional[str] = None
    pending_purchase_player: Optional[str] = None
    pending_purchase_tile: Optional[int] = None
    owners: Dict[int,str] = field(default_factory=dict)
    winner_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

rooms: Dict[str,Room] = {}
rooms_lock=asyncio.Lock()

@app.get('/')
async def root():
    return {'name':'Ada Serveti Online Server','ok':True,'version':'1.0.0','websocket':'/ws'}

@app.get('/health')
async def health():
    return {'ok':True,'rooms':len(rooms),'players':sum(len(r.players) for r in rooms.values()),'time':now_ms()}

async def send(player:Player,payload:dict):
    if not player.ws or not player.connected: return
    try:
        await player.ws.send_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')))
    except Exception:
        player.connected=False; player.ws=None

async def broadcast(room:Room,payload:dict):
    await asyncio.gather(*(send(p,payload) for p in list(room.players.values())),return_exceptions=True)

async def event(room:Room,title:str,text:str,kind:str='info'):
    await broadcast(room,{'type':'event','title':title,'text':text,'kind':kind,'serverNow':now_ms()})


def player_payload(room:Room,p:Player):
    props=[tid for tid,owner in room.owners.items() if owner==p.id]
    return {
        'id':p.id,'name':p.name,'slot':p.slot,'ready':p.ready,'connected':p.connected,
        'money':p.money,'position':p.position,'skipTurns':p.skip_turns,'rentShields':p.rent_shields,
        'eliminated':p.eliminated,'properties':props,'isHost':p.id==room.host_id,
    }


def state_payload(room:Room):
    current_id=None
    if room.status=='playing' and room.turn_order:
        current_id=room.turn_order[room.current_turn % len(room.turn_order)]
    return {
        'type':'state','roomCode':room.code,'status':room.status,'hostId':room.host_id,
        'players':[player_payload(room,p) for p in sorted(room.players.values(),key=lambda x:x.slot)],
        'currentPlayerId':current_id,'lastRoll':room.last_roll,'lastRollerId':room.last_roller,
        'pendingPurchasePlayerId':room.pending_purchase_player,'pendingPurchaseTileId':room.pending_purchase_tile,
        'owners':{str(k):v for k,v in room.owners.items()},'winnerId':room.winner_id,'board':BOARD,'serverNow':now_ms()
    }

async def broadcast_state(room:Room):
    room.updated_at=time.time(); await broadcast(room,state_payload(room))


def alive_ids(room:Room):
    return [pid for pid in room.turn_order if pid in room.players and not room.players[pid].eliminated]

async def check_bankruptcy(room:Room,p:Player):
    if p.money>0 or p.eliminated: return
    p.eliminated=True
    for tid,owner in list(room.owners.items()):
        if owner==p.id: del room.owners[tid]
    await event(room,'İflas!',f'{p.name} parasını kaybetti ve oyundan elendi.','bad')
    alive=alive_ids(room)
    if len(alive)<=1:
        room.status='finished'; room.winner_id=alive[0] if alive else None
        if room.winner_id:
            await event(room,'Oyun Bitti',f'{room.players[room.winner_id].name} Ada Serveti’nin kazananı!','win')
        await broadcast_state(room)

async def advance_turn(room:Room):
    if room.status!='playing': return
    if room.pending_purchase_player: return
    room.last_roll=None; room.last_roller=None
    alive=alive_ids(room)
    if len(alive)<=1:
        await check_bankruptcy(room, room.players[alive[0]] if alive else next(iter(room.players.values())))
        return
    # move at least once, skip eliminated players; consume skip-turns automatically
    for _ in range(len(room.turn_order)*2):
        room.current_turn=(room.current_turn+1)%len(room.turn_order)
        pid=room.turn_order[room.current_turn]
        p=room.players[pid]
        if p.eliminated: continue
        if p.skip_turns>0:
            p.skip_turns-=1
            await event(room,'Tur Kaçırıldı',f'{p.name} Fırtınalı Liman nedeniyle bu turu kaçırdı.','bad')
            continue
        break
    await broadcast_state(room)

async def resolve_tile(room:Room,p:Player, depth:int=0):
    if room.status!='playing' or p.eliminated: return
    tile=BOARD[p.position]
    t=tile['type']
    if t=='property':
        owner=room.owners.get(p.position)
        if owner is None:
            if p.money>=tile['price']:
                room.pending_purchase_player=p.id; room.pending_purchase_tile=p.position
                await event(room,'Satılık Mülk',f"{tile['name']} {tile['price']} ₳. {p.name} satın alabilir.",'property')
                await broadcast_state(room); return
            await event(room,'Yetersiz Para',f"{p.name}, {tile['name']} için yeterli paraya sahip değil.",'bad')
        elif owner==p.id:
            await event(room,'Kendi Mülkün',f"{p.name} kendi mülkü {tile['name']} üzerine geldi.",'info')
        else:
            landlord=room.players.get(owner)
            rent=tile['rent']
            if p.rent_shields>0:
                p.rent_shields-=1
                await event(room,'Kira Sigortası',f"{p.name}, {tile['name']} kirasını ödemedi.",'good')
            elif landlord and not landlord.eliminated:
                p.money-=rent; landlord.money+=rent
                await event(room,'Kira!',f"{p.name}, {landlord.name} oyuncusuna {rent} ₳ kira ödedi.",'bad')
                await check_bankruptcy(room,p)
    elif t=='chance':
        title,text,effect,value=random.choice(CHANCE_CARDS)
        await event(room,'ŞANS — '+title,text,'chance')
        if effect=='money': p.money+=value
        elif effect=='collect':
            for other in room.players.values():
                if other.id!=p.id and not other.eliminated:
                    take=min(value,max(0,other.money))
                    other.money-=take; p.money+=take
                    await check_bankruptcy(room,other)
        elif effect=='move' and depth<2:
            old=p.position; p.position=(p.position+value)%len(BOARD)
            if p.position<old: p.money+=PASS_START_BONUS
            await resolve_tile(room,p,depth+1)
            if room.pending_purchase_player: return
        elif effect=='shield': p.rent_shields+=1
        elif effect=='skip': p.skip_turns+=1
        elif effect=='extra': p.extra_roll=True
        elif effect=='repair':
            count=sum(1 for owner in room.owners.values() if owner==p.id); p.money-=count*value
            await check_bankruptcy(room,p)
    elif t=='tax':
        p.money-=tile['amount']; await event(room,'Vergi',f"{p.name} {tile['amount']} ₳ ödedi.",'bad'); await check_bankruptcy(room,p)
    elif t=='bonus':
        p.money+=tile['amount']; await event(room,tile['name'],f"{p.name} {tile['amount']} ₳ kazandı!",'good')
    elif t=='penalty':
        p.skip_turns+=1; await event(room,'Fırtınalı Liman',f'{p.name} bir sonraki turunu kaçıracak.','bad')
    elif t=='rest':
        await event(room,'Sahil Molası',f'{p.name} biraz dinlendi. Para değişmedi.','info')
    elif t=='start':
        await event(room,'Başlangıç',f'{p.name} başlangıç karesinde.','good')

    if room.status!='playing' or room.pending_purchase_player: return
    if p.extra_roll:
        p.extra_roll=False
        await event(room,'Ekstra Zar!',f'{p.name} bir kez daha zar atacak.','good')
        await broadcast_state(room)
    else:
        await advance_turn(room)

async def do_roll(room:Room,p:Player):
    if room.status!='playing' or room.pending_purchase_player: return
    current=room.turn_order[room.current_turn]
    if current!=p.id or p.eliminated: return
    roll=random.randint(1,6); old=p.position
    p.position=(p.position+roll)%len(BOARD)
    if p.position<old:
        p.money+=PASS_START_BONUS
        await event(room,'Başlangıç Bonusu',f'{p.name} başlangıcı geçti ve {PASS_START_BONUS} ₳ aldı.','good')
    room.last_roll=roll; room.last_roller=p.id
    await broadcast(room,{'type':'dice','playerId':p.id,'roll':roll,'position':p.position})
    await asyncio.sleep(0.35)
    await resolve_tile(room,p)
    await broadcast_state(room)

async def create_or_get_room(mode:str,requested:str):
    async with rooms_lock:
        if mode=='create':
            code=make_code(); r=Room(code=code); rooms[code]=r; return r
        code=''.join(ch for ch in requested.upper() if ch.isalnum())[:8]
        if mode=='join':
            if code not in rooms: raise ValueError('Oda bulunamadı.')
            return rooms[code]
        raise ValueError('Geçersiz mod.')

async def add_player(room:Room,ws:WebSocket,name:str,token:str):
    async with room.lock:
        if token:
            for p in room.players.values():
                if p.token==token:
                    p.ws=ws;p.connected=True;p.name=name or p.name;return p
        if room.status!='waiting': raise ValueError('Maç başlamış.')
        if len(room.players)>=MAX_PLAYERS: raise ValueError('Oda dolu.')
        slot=next(i for i in range(1,MAX_PLAYERS+1) if all(x.slot!=i for x in room.players.values()))
        p=Player(id=uuid.uuid4().hex,token=uuid.uuid4().hex,name=(name or f'Oyuncu {slot}')[:16],slot=slot,ws=ws)
        room.players[p.id]=p
        if not room.host_id: room.host_id=p.id
        return p

@app.websocket('/ws')
async def websocket_endpoint(ws:WebSocket):
    await ws.accept()
    mode=(ws.query_params.get('mode') or 'create').lower()
    code=ws.query_params.get('room') or ''
    name=unquote(ws.query_params.get('name') or 'Oyuncu').strip()[:16]
    token=ws.query_params.get('token') or ''
    try:
        room=await create_or_get_room(mode,code); player=await add_player(room,ws,name,token)
    except ValueError as e:
        await ws.send_json({'type':'error','message':str(e)}); await ws.close(code=1008); return
    await send(player,{'type':'welcome','playerId':player.id,'playerToken':player.token,'roomCode':room.code,'slot':player.slot})
    await broadcast_state(room)
    try:
        while True:
            raw=await ws.receive_text()
            try: msg=json.loads(raw)
            except json.JSONDecodeError: continue
            action=msg.get('action')
            async with room.lock:
                if action=='ready' and room.status=='waiting':
                    player.ready=bool(msg.get('ready',not player.ready)); await broadcast_state(room)
                elif action=='start' and player.id==room.host_id and room.status=='waiting':
                    active=[p for p in room.players.values() if p.connected]
                    if len(active)<2:
                        await send(player,{'type':'error','message':'En az 2 oyuncu gerekli.'}); continue
                    if not all(p.ready for p in active):
                        await send(player,{'type':'error','message':'Herkes hazır olmalı.'}); continue
                    room.status='playing'; room.turn_order=[p.id for p in sorted(active,key=lambda x:x.slot)]
                    room.current_turn=0; room.last_roll=None; room.winner_id=None
                    for p in active:
                        p.money=START_MONEY;p.position=0;p.skip_turns=0;p.rent_shields=0;p.extra_roll=False;p.eliminated=False;p.ready=False
                    room.owners.clear(); room.pending_purchase_player=None;room.pending_purchase_tile=None
                    await event(room,'Oyun Başladı',f'İlk sıra {room.players[room.turn_order[0]].name} oyuncusunda!','good'); await broadcast_state(room)
                elif action=='roll':
                    asyncio.create_task(do_roll(room,player))
                elif action in {'buy','pass'} and room.pending_purchase_player==player.id:
                    tid=room.pending_purchase_tile
                    if tid is None: continue
                    tile=BOARD[tid]
                    if action=='buy' and room.owners.get(tid) is None and player.money>=tile.get('price',10**9):
                        player.money-=tile['price']; room.owners[tid]=player.id
                        await event(room,'Mülk Satın Alındı',f"{player.name}, {tile['name']} mülkünü {tile['price']} ₳ karşılığında aldı.",'property')
                    else:
                        await event(room,'Satın Alınmadı',f"{player.name}, {tile['name']} mülkünü pas geçti.",'info')
                    room.pending_purchase_player=None;room.pending_purchase_tile=None
                    await advance_turn(room)
                elif action=='ping':
                    await send(player,{'type':'pong','clientTime':msg.get('clientTime'),'serverNow':now_ms()})
    except WebSocketDisconnect:
        player.connected=False;player.ws=None
        await broadcast_state(room)
    except Exception:
        player.connected=False;player.ws=None
        await broadcast_state(room)

async def cleanup_loop():
    while True:
        await asyncio.sleep(300)
        cutoff=time.time()-ROOM_TTL
        async with rooms_lock:
            for code,r in list(rooms.items()):
                if r.updated_at<cutoff: rooms.pop(code,None)

@app.on_event('startup')
async def startup():
    asyncio.create_task(cleanup_loop())
