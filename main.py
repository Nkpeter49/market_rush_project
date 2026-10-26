import asyncio
import json
import random
import string
import secrets
from pathlib import Path
from typing import Any
import urllib.request

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
INITIAL_CAPITAL = 1_000
TOTAL_ROUNDS = 5
ASSETS = ("SOLAR", "NOVA", "QUANT")
ROOM_CODE_LENGTH = 6

rooms: dict[str, dict[str, Any]] = {}
state_lock = asyncio.Lock()


class ConnectionManager:
    """Tracks live WebSocket connections and broadcasts room events."""

    def __init__(self) -> None:
        self.active: dict[str, dict[str, WebSocket]] = {}

    async def connect(self, room_code: str, player_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active.setdefault(room_code, {})[player_id] = websocket

    def disconnect(self, room_code: str, player_id: str) -> None:
        players = self.active.get(room_code)
        if players:
            players.pop(player_id, None)
            if not players:
                self.active.pop(room_code, None)

    async def send_player(self, room_code: str, player_id: str, message: dict[str, Any]) -> None:
        websocket = self.active.get(room_code, {}).get(player_id)
        if websocket:
            try:
                await websocket.send_json(message)
            except (RuntimeError, WebSocketDisconnect):
                self.disconnect(room_code, player_id)

    async def broadcast(self, room_code: str, message: dict[str, Any]) -> None:
        connections = list(self.active.get(room_code, {}).items())
        if not connections:
            return
        results = await asyncio.gather(
            *(ws.send_json(message) for _, ws in connections), return_exceptions=True
        )
        for (player_id, _), result in zip(connections, results):
            if isinstance(result, BaseException):
                self.disconnect(room_code, player_id)


manager = ConnectionManager()


def make_room_code() -> str:
    alphabet = string.ascii_uppercase + string.digits
    for _ in range(100):
        code = "".join(secrets.choice(alphabet) for _ in range(ROOM_CODE_LENGTH))
        if code not in rooms:
            return code
    raise RuntimeError("Could not generate a unique room code")


def generate_local_ollama_news(asset_name: str, change_pct: float) -> str:
    """Queries local Ollama instance for a quick market flavor headline."""
    prompt = (
        f"Write a short, punchy 1-sentence financial news headline explaining why "
        f"asset {asset_name} moved by {change_pct:+.1f}% in a high-stakes trading game."
    )
    payload = json.dumps({
        "model": "llama3.2:1b",
        "prompt": prompt,
        "stream": False
    }).encode("utf-8")

    req = urllib.request.Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=2) as response:
            res_data = json.loads(response.read().decode("utf-8"))
            return res_data.get("response", "").strip()
    except Exception:
        return f"Market volatility spiked for {asset_name} following heavy trading volume."


def public_state(room: dict[str, Any]) -> dict[str, Any]:
    return {
        "room_code": room["code"],
        "status": room["status"],
        "round": room["round"],
        "total_rounds": TOTAL_ROUNDS,
        "assets": list(ASSETS),
        "prices": room["prices"].copy(),
        "news": room.get("news", "Markets open. Allocate your capital wisely."),
        "players": [
            {
                "player_id": p["id"],
                "name": p["name"],
                "balance": round(p["balance"], 2),
                "round_submitted": p["round_submitted"],
                "history": p["history"][-TOTAL_ROUNDS:],
            }
            for p in room["players"].values()
        ],
    }


app = FastAPI(title="Market Rush — Handshake AI Edition")


@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.websocket("/ws/{room_code}/{player_id}")
async def websocket_endpoint(websocket: WebSocket, room_code: str, player_id: str):
    async with state_lock:
        if room_code not in rooms:
            rooms[room_code] = {
                "code": room_code,
                "status": "waiting",
                "round": 1,
                "prices": {asset: round(random.uniform(50.0, 150.0), 2) for asset in ASSETS},
                "players": {},
                "news": "Welcome to Market Rush. Submit your allocations to trigger round calculations."
            }
        room = rooms[room_code]

        if player_id not in room["players"]:
            room["players"][player_id] = {
                "id": player_id,
                "name": f"Trader_{player_id[:4]}",
                "balance": INITIAL_CAPITAL,
                "allocations": {a: 0.0 for a in ASSETS},
                "round_submitted": False,
                "history": []
            }

    await manager.connect(room_code, player_id, websocket)

    async with state_lock:
        if len(room["players"]) >= 2 and room["status"] == "waiting":
            room["status"] = "playing"

    await manager.broadcast(room_code, {"type": "state_update", "room": public_state(room)})

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            async with state_lock:
                player = room["players"].get(player_id)
                if not player:
                    continue

                if action == "set_name":
                    player["name"] = data.get("name", player["name"])

                elif action == "submit_allocation" and room["status"] == "playing":
                    allocs = data.get("allocations", {})
                    total_allocated = sum(float(v) for v in allocs.values() if isinstance(v, (int, float, str)) and str(v).replace('.','',1).isdigit())

                    if total_allocated <= player["balance"]:
                        for a in ASSETS:
                            player["allocations"][a] = float(allocs.get(a, 0.0))
                        player["round_submitted"] = True

                active_players = list(room["players"].values())
                if active_players and all(p["round_submitted"] for p in active_players):
                    target_asset = random.choice(ASSETS)
                    change_pct = round(random.uniform(-12.0, 15.0), 2)
                    old_price = room["prices"][target_asset]
                    new_price = max(5.0, round(old_price * (1 + change_pct / 100.0), 2))
                    room["prices"][target_asset] = new_price

                    room["news"] = generate_local_ollama_news(target_asset, change_pct)

                    for p in active_players:
                        round_cost = sum(p["allocations"].values())
                        p["balance"] -= round_cost
                        asset_return = p["allocations"].get(target_asset, 0.0) * (1 + change_pct / 100.0)
                        p["balance"] += (round_cost - p["allocations"].get(target_asset, 0.0)) + asset_return
                        p["balance"] = round(p["balance"], 2)

                        p["history"].append({"round": room["round"], "balance": p["balance"]})
                        p["round_submitted"] = False
                        p["allocations"] = {a: 0.0 for a in ASSETS}

                    room["round"] += 1
                    if room["round"] > TOTAL_ROUNDS:
                        room["status"] = "finished"
                        room["news"] = "Simulation complete. Final rankings locked."

            await manager.broadcast(room_code, {"type": "state_update", "room": public_state(room)})

    except WebSocketDisconnect:
        async with state_lock:
            manager.disconnect(room_code, player_id)
            room = rooms.get(room_code)
            if room:
                room["players"].pop(player_id, None)
                if not room["players"]:
                    rooms.pop(room_code, None)
                else:
                    room["status"] = "waiting"
                    await manager.broadcast(room_code, {"type": "state_update", "room": public_state(room)})
