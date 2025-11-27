import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field


class ItemInput(BaseModel):
    title: str = Field(..., example="News headline")
    body: str = Field(..., example="Detailed content")


class Item(ItemInput):
    id: int
    updated_at: float


class EventBus:
    """Lightweight pub/sub queue for pushing changes to subscribers."""

    def __init__(self) -> None:
        self._subscribers: List[asyncio.Queue[Dict[str, Any]]] = []
        self._lock = asyncio.Lock()

    async def publish(self, event: Dict[str, Any]) -> None:
        async with self._lock:
            for queue in list(self._subscribers):
                await queue.put(event)

    async def subscribe(self) -> asyncio.Queue[Dict[str, Any]]:
        queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        async with self._lock:
            self._subscribers.append(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[Dict[str, Any]]) -> None:
        async with self._lock:
            if queue in self._subscribers:
                self._subscribers.remove(queue)


class ItemStore:
    def __init__(self) -> None:
        self._items: Dict[int, Item] = {}
        self._counter = 1
        self._lock = asyncio.Lock()

    async def list_items(self) -> List[Item]:
        async with self._lock:
            return list(self._items.values())

    async def upsert(self, item_input: ItemInput, item_id: Optional[int] = None) -> Item:
        async with self._lock:
            item_id = item_id or self._counter
            now = time.time()
            item = Item(id=item_id, updated_at=now, **item_input.model_dump())
            self._items[item_id] = item
            if item_id == self._counter:
                self._counter += 1
            return item

    async def get(self, item_id: int) -> Item:
        async with self._lock:
            if item_id not in self._items:
                raise HTTPException(status_code=404, detail="Item not found")
            return self._items[item_id]


event_bus = EventBus()
store = ItemStore()
app = FastAPI(title="Primary content source")


@app.post("/items", response_model=Item)
async def create_item(payload: ItemInput) -> Item:
    item = await store.upsert(payload)
    await event_bus.publish({"action": "created", "item": item.model_dump()})
    return item


@app.put("/items/{item_id}", response_model=Item)
async def update_item(item_id: int, payload: ItemInput) -> Item:
    await store.get(item_id)
    item = await store.upsert(payload, item_id=item_id)
    await event_bus.publish({"action": "updated", "item": item.model_dump()})
    return item


@app.get("/items", response_model=List[Item])
async def list_items() -> List[Item]:
    return await store.list_items()


async def _event_stream() -> Any:
    queue = await event_bus.subscribe()
    try:
        while True:
            event = await queue.get()
            payload = json.dumps(event)
            yield f"data: {payload}\n\n"
    finally:
        await event_bus.unsubscribe(queue)


@app.get("/stream")
async def stream_events() -> StreamingResponse:
    """Server-Sent Events endpoint for near real-time updates."""
    return StreamingResponse(_event_stream(), media_type="text/event-stream")


@app.get("/")
async def root() -> Dict[str, str]:
    return {"status": "ok", "items": "use /items and /stream"}
