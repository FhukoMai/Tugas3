import httpx
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class MessagePassing:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=2.0)

    async def send_message(self, peer_url: str, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{peer_url}/{endpoint}"
        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to send message to {url}: {e}")
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error communicating with {url}: {e}")
            return {"error": str(e)}
            
    async def get_request(self, peer_url: str, endpoint: str) -> Dict[str, Any]:
        url = f"{peer_url}/{endpoint}"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"GET request failed to {url}: {e}")
            return {"error": str(e)}

    async def close(self):
        await self.client.aclose()

messenger = MessagePassing()
