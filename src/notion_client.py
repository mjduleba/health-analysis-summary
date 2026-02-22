import os
import time
from typing import Any, Dict, Iterable, List, Optional
from dataclasses import dataclass
import json

import requests
from dotenv import load_dotenv

# Store base URL for Notion API
NOTION_API_BASE = 'https://api.notion.com/v1'

class NotionAPIError(Exception):
    """Custom exception for Notion API errors."""
    pass

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
    
    def query_database(
        self,
        database_id: str,
        *,
        filter_obj: Optional[Dict[str, Any]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 100,
        start_cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''
        Query Notion database with pagination, filtering, and sorting options.

        Args:
            database_id (str): ID of the Notion database to query
            filter_obj (Optional[Dict[str, Any]], optional): Filter object. Defaults to None.
            sorts (Optional[List[Dict[str, Any]]], optional): Sorting. Defaults to None.
            page_size (int, optional): Number of pages to query. Defaults to 100.
            start_cursor (Optional[str], optional): Pointer for continued database query. Defaults to None.

        Raises:
            ValueError: Page size must be between 1 and 100

        Returns:
            Dict[str, Any]: API resposnse from Notion API request
        '''
        # Validate page size
        if page_size < 1 or page_size > 100:
            raise ValueError("page_size must be between 1 and 100")
        
        # Store URL
        url = f'{NOTION_API_BASE}/databases/{database_id}/query'
        
        # Build payload
        payload = {
            "page_size": page_size,
        }
        
        # Add options payload parameters
        if filter_obj:
            payload["filter"] = filter_obj
        if sorts:
            payload["sorts"] = sorts
        if start_cursor:
            payload["start_cursor"] = start_cursor
        
        return self._request("POST", url, json=payload)

    def iter_database_pages(
        self,
        database_id: str,
        *,
        filter_obj: Optional[Dict[str, Any]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        
        # Cursor for pagination and page counter
        cursor: Optional[str] = None
        pages_fetched = 0

        while True:
            # Store formatted response for current page
            response = self.query_database(
                database_id,
                filter_obj=filter_obj,
                sorts=sorts,
                page_size=page_size,
                start_cursor=cursor,
            )

            # Extract results and yield each page
            results = response.get("results", [])
            for page in results:
                yield page

            # Update page counter and check max pages limit
            pages_fetched += 1
            if max_pages is not None and pages_fetched >= max_pages:
                return

            # Check for more pages, exit if no more
            if not response.get("has_more"):
                return

            # Update cursor for next page
            cursor = response.get("next_cursor")

    def close(self) -> None:
        '''
        Close HTTP session.
        '''
        self._session.close()

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        '''
        Internal method to perform HTTP requests with retry logic and error handling.

        Args:
            method (str): HTTP method (GET, POST, etc.)
            url (str): Full URL for the request

        Raises:
            NotionAPIError: Formatted error resposne from API
            NotionAPIError: Last error after retries exhausted

        Returns:
            Dict[str, Any]: Parsed JSON response from the API
        '''
        # Error storage for after retries
        last_error: Optional[str] = None

        # Retry loop with exponential backoff
        for attempt in range(self._config.max_retries + 1):
            try:
                # Make HTTP request
                response = self._session.request(
                    method,
                    url,
                    timeout=self._config.timeout_s,
                    **kwargs,
                )
            except requests.RequestException as exc:
                # Store last error and initiate backoff
                last_error = f"Request failed: {exc}"
                self._sleep_backoff(attempt)
                continue
            
            # Handle successful response
            if 200 <= response.status_code < 300:
                return response.json()

            # Handle retryable error responses
            if response.status_code in (429, 500, 502, 503, 504):
                # Store formatted last error
                last_error = self._format_error(response)
                
                # Check for Retry-After header and sleep accordingly
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except ValueError:
                        self._sleep_backoff(attempt)
                else:
                    self._sleep_backoff(attempt)

                continue

            raise NotionAPIError(self._format_error(response))

        raise NotionAPIError(last_error or "Request failed after retries.")

    def _sleep_backoff(self, attempt: int) -> None:
        '''
        Internal manual backoff method to handle retry delays.

        Args:
            attempt (int): Current retry attempt
        '''
        delay = min(
            self._config.backoff_max_s,
            self._config.backoff_base_s * (2 ** attempt),
        )
        time.sleep(delay)

    @staticmethod
    def _format_error(response: requests.Response) -> str:
        '''
        Internal method to format error responses from the Notion API.

        Args:
            response (requests.Response): HTTP error response

        Returns:
            str: Formatted error message with status code and response body
        '''
        try:
            body = response.json()
            body_str = json.dumps(body, ensure_ascii=False)
        except Exception:
            body_str = response.text.strip()

        return f"Notion API error {response.status_code}: {body_str}"