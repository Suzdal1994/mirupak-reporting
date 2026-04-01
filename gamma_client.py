"""
Клиент для Gamma API — создание презентаций из Markdown.
textMode: "preserve" — сохраняет текст как есть
additionalInstructions — передаём системный промпт
"""

import requests
import time
from typing import Optional


GAMMA_API_BASE = "https://public-api.gamma.app/v1.0"

# Инструкции передаются через additionalInstructions (max 5000 символов)
GAMMA_ADDITIONAL_INSTRUCTIONS = """ТЫ — ГЕНЕРАТОР ПРЕЗЕНТАЦИИ В GAMMA.

ИСТОЧНИК ИСТИНЫ: ТОЛЬКО МАРКДАУН, КОТОРЫЙ Я ДАЛ НИЖЕ.
ЗАПРЕЩЕНО:
- добавлять любые новые факты, цифры, выводы или примеры,
- "дорисовывать" контекст, причины, интерпретации, которых нет в тексте,
- создавать/вставлять картинки, иконки, иллюстрации, диаграммы, графики, схемы,
- заменять таблицы графиками,
- использовать внешние источники.

РАЗРЕШЕНО:
- только аккуратно отформатировать слайды,
- привести заголовки и буллеты к единому стилю (без изменения смысла),
- сохранить структуру и порядок слайдов.

ФОРМАТ ВЫХОДА:
- Каждый раздел, отделённый '---', = один слайд.
- Сохраняй таблицы как таблицы (не превращай в графики).
- Ничего не добавляй от себя. Пустые столбцы и таблицы — оставь пустыми.

СТИЛЬ: минималистично, деловой стиль, без изображений."""


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
    ) -> str:
        """
        Создаёт презентацию в Gamma.
        textMode=preserve — текст сохраняется точно.
        additionalInstructions — системный промпт с правилами.
        """
        payload = {
            "inputText": markdown_text,
            "textMode": "preserve",
            "additionalInstructions": GAMMA_ADDITIONAL_INSTRUCTIONS,
            "format": "presentation",
            "numCards": num_cards,
            "cardSplit": "auto",
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
            except Exception:
                pass
            raise RuntimeError(f"Gamma API error {resp.status_code}: {error_msg}")

        data = resp.json()
        generation_id = data.get('generationId')
        if not generation_id:
            raise RuntimeError(f"No generationId in response: {data}")

        return generation_id

    def poll_generation(self, generation_id: str, max_wait: int = 300, poll_interval: int = 5) -> dict:
        """Ждёт завершения генерации. Возвращает dict с gammaUrl."""
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
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get('items', data.get('themes', data.get('data', [])))
        return []

    def get_folders(self) -> list:
        """Возвращает список папок."""
        resp = requests.get(
            f"{GAMMA_API_BASE}/folders",
            headers=self.headers,
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return data.get('items', data.get('folders', data.get('data', [])))
        return []

    def validate_key(self) -> bool:
        """Проверяет валидность API-ключа."""
        try:
            self.get_themes()
            return True
        except Exception:
            return False
