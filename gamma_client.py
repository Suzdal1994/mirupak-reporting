"""
Клиент для Gamma API — создание презентаций из Markdown.
"""

import requests
import time
import json
from typing import Optional


GAMMA_API_BASE = "https://public-api.gamma.app/v1.0"


class GammaClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": api_key
        }
    
    def create_presentation(
        self,
        markdown_text: str,
        title: str,
        language: str = "ru",
        theme_id: Optional[str] = None,
        num_cards: int = 8,
        folder_id: Optional[str] = None,
    ) -> dict:
        """
        Создаёт презентацию в Gamma.
        textMode=preserve — используем наш markdown как есть (без переписывания).
        """
        payload = {
            "inputText": markdown_text,
            "textMode": "preserve",
            "format": "presentation",
            "numCards": num_cards,
            "cardSplit": "auto",
            "textOptions": {
                "amount": "detailed",
                "tone": "professional",
                "audience": "management team",
                "language": language
            },
            "imageOptions": {
                "source": "noImages"
            },
            "cardOptions": {
                "dimensions": "16x9"
            },
            "sharingOptions": {
                "workspaceAccess": "view",
                "externalAccess": "view"
            }
        }
        
        if theme_id:
            payload["themeId"] = theme_id
        
        if folder_id:
            payload["folderIds"] = [folder_id]
        
        resp = requests.post(
            f"{GAMMA_API_BASE}/generations",
            headers=self.headers,
            json=payload,
            timeout=30
        )
        
        if resp.status_code not in (200, 201, 202):
            error_msg = resp.text
            try:
                error_data = resp.json()
                error_msg = error_data.get('message', error_msg)
            except:
                pass
            raise RuntimeError(f"Gamma API error {resp.status_code}: {error_msg}")
        
        data = resp.json()
        generation_id = data.get('generationId')
        if not generation_id:
            raise RuntimeError(f"No generationId in response: {data}")
        
        return generation_id
    
    def poll_generation(self, generation_id: str, max_wait: int = 180, poll_interval: int = 5) -> dict:
        """
        Ждёт завершения генерации. Возвращает dict с gammaUrl и credits.
        """
        start = time.time()
        while time.time() - start < max_wait:
            resp = requests.get(
                f"{GAMMA_API_BASE}/generations/{generation_id}",
                headers=self.headers,
                timeout=30
            )
            
            if resp.status_code != 200:
                raise RuntimeError(f"Poll error {resp.status_code}: {resp.text}")
            
            data = resp.json()
            status = data.get('status', '')
            
            if status == 'completed':
                return {
                    'url': data.get('gammaUrl', ''),
                    'credits': data.get('credits', {}),
                    'generation_id': generation_id
                }
            elif status == 'failed':
                raise RuntimeError(f"Generation failed: {data}")
            
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Generation {generation_id} did not complete in {max_wait}s")
    
    def create_and_wait(self, markdown_text: str, title: str, **kwargs) -> dict:
        """Создаёт презентацию и ждёт результата."""
        generation_id = self.create_presentation(markdown_text, title, **kwargs)
        return self.poll_generation(generation_id)
    
    def get_themes(self) -> list:
        """Возвращает список тем из воркспейса."""
        resp = requests.get(
            f"{GAMMA_API_BASE}/themes",
            headers=self.headers,
            timeout=30
        )
        if resp.status_code == 200:
            return resp.json()
        return []
    
    def get_folders(self) -> list:
        """Возвращает список папок."""
        resp = requests.get(
            f"{GAMMA_API_BASE}/folders",
            headers=self.headers,
            timeout=30
        )
        if resp.status_code == 200:
            return resp.json()
        return []
    
    def validate_key(self) -> bool:
        """Проверяет валидность API-ключа."""
        try:
            themes = self.get_themes()
            return True
        except:
            return False
