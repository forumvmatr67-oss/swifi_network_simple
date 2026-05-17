import asyncio
import random
import uuid
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from starlette.requests import Request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ---------------------- Константы ----------------------
IPYBYTE = 2 ** 90
HPYBYTE = 1000 * IPYBYTE
GPYBYTE = 1000 * HPYBYTE

def bytes_to_ipyb(b: int) -> float:
    return b / IPYBYTE

# ---------------------- Пакет (исправленный порядок полей) ----------------------
@dataclass
class Packet:
    src: str
    dst: str
    size: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ttl: int = 30
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    payload: dict = field(default_factory=dict)

    def expired(self, now: float) -> bool:
        return self.ttl <= 0 or (now - self.timestamp) > 10.0

# ---------------------- Линк ----------------------
class Link:
    def __init__(self, node_a, node_b, delay_ms=20, bandwidth_mbps=100, loss_rate=0.05):
        self.node_a = node_a
        self.node_b = node_b
        self.delay = delay_ms / 1000.0
        self.bandwidth = bandwidth_mbps * 1e6 / 8
        self.loss_rate = loss_rate

    async def send(self, packet: Packet, src_node):
        if random.random() < self.loss_rate:
            return
        tx_time = packet.size / self.bandwidth
        await asyncio.sleep(self.delay + tx_time)
        dst_node = self.node_b if src_node == self.node_a else self.node_a
        await dst_node.receive(packet)

# ---------------------- Узел ----------------------
class Node:
    def __init__(self, node_id: str, name: str, quota_ipyb: float = 10.0):
        self.id = node_id
        self.name = name
        self.quota_bytes = int(quota_ipyb * IPYBYTE)
        self.tx_bytes = 0
        self.rx_bytes = 0
        self.links: List[Link] = []
        self.routing_table: Dict[str, str] = {}
        self.queues = {0: asyncio.Queue(), 1: asyncio.Queue(), 2: asyncio.Queue()}
        self._running = True

    def add_link(self, other, **kwargs):
        link = Link(self, other, **kwargs)
        self.links.append(link)
        other.links.append(link)

    async def start(self):
        asyncio.create_task(self._process_outgoing())
        asyncio.create_task(self._update_routes())

    async def _process_outgoing(self):
        while self._running:
            packet = None
            for prio in (2, 1, 0):
                if not self.queues[prio].empty():
                    packet = await self.queues[prio].get()
                    break
            if packet is None:
                await asyncio.sleep(0.01)
                continue
            if self.tx_bytes + packet.size > self.quota_bytes:
                continue
            self.tx_bytes += packet.size
            next_hop = self.routing_table.get(packet.dst)
            if not next_hop:
                continue
            for link in self.links:
                neighbor = link.node_b if link.node_a == self else link.node_a
                if neighbor.id == next_hop:
                    asyncio.create_task(link.send(packet, self))
                    break

    async def _update_routes(self):
        while self._running:
            await asyncio.sleep(5)
            for link in self.links:
                neighbor = link.node_b if link.node_a == self else link.node_a
                route_packet = Packet(
                    src=self.id, dst=neighbor.id, size=256, priority=2,
                    payload={"routing_table": self.routing_table}
                )
                await self.queues[2].put(route_packet)

    async def receive(self, packet: Packet):
        self.rx_bytes += packet.size
        packet.ttl -= 1
        now = time.time()
        if packet.expired(now):
            return
        if packet.dst == self.id:
            await self._deliver(packet)
        else:
            await self.forward(packet)

    async def forward(self, packet: Packet):
        await self.queues[packet.priority].put(packet)

    async def _deliver(self, packet: Packet):
        # Можно добавить логирование
        pass

    async def stop(self):
        self._running = False

    def get_stats(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "tx_bytes": self.tx_bytes,
            "rx_bytes": self.rx_bytes,
            "tx_ipyb": bytes_to_ipyb(self.tx_bytes),
            "rx_ipyb": bytes_to_ipyb(self.rx_bytes),
            "quota_ipyb": self.quota_bytes / IPYBYTE,
            "remaining_ipyb": (self.quota_bytes - self.tx_bytes) / IPYBYTE,
        }

    async def send_user_packet(self, dst_id: str, size: int, priority: int = 0):
        if dst_id not in self.routing_table and dst_id != self.id:
            raise ValueError(f"Нет маршрута до {dst_id}")
        pkt = Packet(src=self.id, dst=dst_id, size=size, priority=priority)
        await self.queues[priority].put(pkt)

# ---------------------- Топологии ----------------------
def create_star_topology(hub_name: str, leaf_names: List[str], link_params=None) -> Dict[str, Node]:
    nodes = {hub_name: Node(hub_name, hub_name)}
    for name in leaf_names:
        nodes[name] = Node(name, name)
    hub = nodes[hub_name]
    for name in leaf_names:
        hub.add_link(nodes[name], **(link_params or {}))
    return nodes

def create_mesh_topology(names: List[str], link_params=None) -> Dict[str, Node]:
    nodes = {name: Node(name, name) for name in names}
    for i, a in enumerate(names):
        for b in names[i+1:]:
            nodes[a].add_link(nodes[b], **(link_params or {}))
    return nodes

# ---------------------- FastAPI приложение ----------------------
app = FastAPI(title="SWIFI Network Simulator", description="Симулятор сети с ipybyte, MIT license")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

network_nodes: Dict[str, Node] = {}

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
    async def broadcast(self, message: dict):
        for ws in self.active:
            try:
                await ws.send_json(message)
            except:
                pass

manager = ConnectionManager()

@app.on_event("startup")
async def startup():
    global network_nodes
    # Звезда с хабом "Hub" и листьями
    network_nodes = create_star_topology(
        hub_name="Hub",
        leaf_names=["Alpha", "Beta", "Gamma"],
        link_params={"delay_ms": 10, "bandwidth_mbps": 50, "loss_rate": 0.02}
    )
    # Для mesh раскомментируйте:
    # network_nodes = create_mesh_topology(["NodeA", "NodeB", "NodeC", "NodeD"])
    for node in network_nodes.values():
        await node.start()
    asyncio.create_task(broadcast_stats())

async def broadcast_stats():
    while True:
        await asyncio.sleep(1)
        stats = [node.get_stats() for node in network_nodes.values()]
        await manager.broadcast({"type": "stats", "data": stats})

@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/topology")
async def get_topology():
    nodes_list = [{"id": n.id, "name": n.name} for n in network_nodes.values()]
    links_list = []
    seen = set()
    for node in network_nodes.values():
        for link in node.links:
            neighbor = link.node_b if link.node_a == node else link.node_a
            key = tuple(sorted([node.id, neighbor.id]))
            if key not in seen:
                seen.add(key)
                links_list.append({"source": node.id, "target": neighbor.id})
    return {"nodes": nodes_list, "links": links_list}

@app.post("/api/send")
async def send_packet(src: str, dst: str, size: int, priority: int = 0):
    if src not in network_nodes or dst not in network_nodes:
        return {"error": "Узел не найден"}, 400
    try:
        await network_nodes[src].send_user_packet(dst, size, priority)
        return {"message": f"Пакет {size} байт от {src} к {dst} отправлен"}
    except Exception as e:
        return {"error": str(e)}, 400

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
