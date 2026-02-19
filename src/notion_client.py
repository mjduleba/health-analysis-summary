import os
import time
from typing import Any, Dict, Iterable, List, Optional
from dataclasses import dataclass

import requests
from dotenv import load_dotenv

# Store base URL for Notion API
NOTION_API_BASE = 'https://api.notion.com/v1'

@dataclass(frozen=True)
class NotionClientConfig:
    '''
    Configuration for NotionClient

    Attributes:
        token: Notion integration token
        notion_version: Notion API version
        timeout: HTTP timeout in seconds
        max_retries: maximum retry attempts
        backoff_base: base for exponential backoff in seconds
        backoff_max: maximum backoff in seconds
    '''
    token: str
    notion_version: str = '2022-06-28'
    timeout: int = 30
    max_retries: int = 5
    backoff_base: float = 1.0
    backoff_max: float = 20.0

class NotionClient:

    def __init__(self, config: NotionClientConfig) -> None:
        '''
        NotionClient Initialization

        Args:
            token (str): API token
            notion_version (str, optional): Notion version. Defaults to "2022-06-28".
        '''
        self._config = config
        self._session = requests.session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._config.token}",
                "Notion-Version": self._config.notion_version,
                "Content-Type": "application/json",
            }
        )

   