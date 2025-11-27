import asyncio
import json
import os
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

SOURCE_API_URL = os.getenv("SOURCE_API_URL", "http://localhost:8000")


class Item(BaseModel):
    id: int
    title: str
    body: str
    updated_at: float


class MirrorState:
    def __init__(self) -> None:
        self._items: Dict[int, Item] = {}
        self._lock = asyncio.Lock()

    async def replace_all(self, items: List[Item]) -> None:
        async with self._lock:
            self._items = {item.id: item for item in items}

    async def apply_event(self, event: Dict[str, Any]) -> None:
        async with self._lock:
            item = Item.model_validate(event["item"])
            self._items[item.id] = item

    async def list_items(self) -> List[Item]:
        async with self._lock:
            return list(self._items.values())


state = MirrorState()
app = FastAPI(title="Mirror site (read-only)")


async def _initial_sync(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"{SOURCE_API_URL}/items", timeout=5.0)
    resp.raise_for_status()
    items = [Item.model_validate(item) for item in resp.json()]
    await state.replace_all(items)


async def _follow_stream(client: httpx.AsyncClient) -> None:
    while True:
        try:
            async with client.stream("GET", f"{SOURCE_API_URL}/stream", timeout=None) as response:
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    payload = json.loads(line.replace("data: ", ""))
                    if "item" in payload:
                        await state.apply_event(payload)
        except (httpx.HTTPError, json.JSONDecodeError):
            await asyncio.sleep(1)


def _start_sync_task() -> None:
    async def runner() -> None:
        async with httpx.AsyncClient() as client:
            await _initial_sync(client)
            await _follow_stream(client)

    asyncio.create_task(runner())


@app.on_event("startup")
async def startup_event() -> None:
    _start_sync_task()


@app.get("/items", response_model=List[Item])
async def list_items() -> List[Item]:
    return await state.list_items()


@app.get("/")
async def health() -> Dict[str, str]:
    return {"status": "mirroring", "source": SOURCE_API_URL}
