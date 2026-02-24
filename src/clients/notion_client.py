import time
from typing import Any, Dict, Iterable, List, Optional
from dataclasses import dataclass
import json

import requests

from src.shared.logger import get_logger

# Store base URL for Notion API
NOTION_API_BASE = 'https://api.notion.com/v1'

# Create global logger
logger = get_logger("notion_client")

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
        logger.debug(
            "Initialized NotionClient with notion_version=%s timeout=%ss max_retries=%s",
            self._config.notion_version,
            self._config.timeout,
            self._config.max_retries,
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

        logger.debug(
            "Querying database_id=%s page_size=%s has_filter=%s sorts_count=%s start_cursor_present=%s",
            database_id,
            page_size,
            bool(filter_obj),
            len(sorts) if sorts else 0,
            bool(start_cursor),
        )
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
        logger.debug(
            "Starting database iteration database_id=%s page_size=%s max_pages=%s",
            database_id,
            page_size,
            max_pages,
        )

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
            logger.debug(
                "Fetched page_index=%s rows=%s has_more=%s",
                pages_fetched + 1,
                len(results),
                bool(response.get("has_more")),
            )
            for page in results:
                yield page

            # Update page counter and check max pages limit
            pages_fetched += 1
            if max_pages is not None and pages_fetched >= max_pages:
                logger.debug("Stopping iteration: reached max_pages=%s", max_pages)
                return

            # Check for more pages, exit if no more
            if not response.get("has_more"):
                logger.debug("Stopping iteration: no more pages after page_index=%s", pages_fetched)
                return

            # Update cursor for next page
            cursor = response.get("next_cursor")
            logger.debug("Continuing iteration with next_cursor_present=%s", bool(cursor))

    def close(self) -> None:
        '''
        Close HTTP session.
        '''
        self._session.close()
        logger.debug("Closed NotionClient session")

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
            attempt_number = attempt + 1
            logger.debug(
                "HTTP request attempt=%s/%s method=%s url=%s",
                attempt_number,
                self._config.max_retries + 1,
                method,
                url,
            )
            try:
                # Make HTTP request
                response = self._session.request(
                    method,
                    url,
                    timeout=getattr(self._config, "timeout_s", self._config.timeout),
                    **kwargs,
                )
            except requests.RequestException as exc:
                # Store last error and initiate backoff
                last_error = f"Request failed: {exc}"
                logger.warning(
                    "Request exception on attempt=%s/%s: %s",
                    attempt_number,
                    self._config.max_retries + 1,
                    exc,
                )
                self._sleep_backoff(attempt)
                continue
            
            # Handle successful response
            if 200 <= response.status_code < 300:
                logger.debug(
                    "HTTP request succeeded status=%s attempt=%s/%s",
                    response.status_code,
                    attempt_number,
                    self._config.max_retries + 1,
                )
                return response.json()

            # Handle retryable error responses
            if response.status_code in (429, 500, 502, 503, 504):
                # Store formatted last error
                last_error = self._format_error(response)
                logger.warning(
                    "Retryable response status=%s attempt=%s/%s",
                    response.status_code,
                    attempt_number,
                    self._config.max_retries + 1,
                )
                
                # Check for Retry-After header and sleep accordingly
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        logger.debug("Sleeping for Retry-After=%s seconds", retry_after)
                        time.sleep(float(retry_after))
                    except ValueError:
                        self._sleep_backoff(attempt)
                else:
                    self._sleep_backoff(attempt)

                continue

            error = self._format_error(response)
            logger.error("Non-retryable response: %s", error)
            raise NotionAPIError(error)

        logger.error("Request retries exhausted: %s", last_error or "Request failed after retries.")
        raise NotionAPIError(last_error or "Request failed after retries.")

    def _sleep_backoff(self, attempt: int) -> None:
        '''
        Internal manual backoff method to handle retry delays.

        Args:
            attempt (int): Current retry attempt
        '''
        delay = min(
            getattr(self._config, "backoff_max_s", self._config.backoff_max),
            getattr(self._config, "backoff_base_s", self._config.backoff_base) * (2 ** attempt),
        )
        logger.debug("Applying exponential backoff delay=%ss attempt=%s", delay, attempt + 1)
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
