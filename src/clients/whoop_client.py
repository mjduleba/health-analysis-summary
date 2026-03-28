import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional
from urllib.parse import urlencode

import requests

from src.shared.logger import get_logger

# Create global logger
logger = get_logger(__name__)

# Store base URLs for Whoop API
WHOOP_OAUTH_BASE = 'https://api.prod.whoop.com/oauth/oauth2'
WHOOP_API_BASE = 'https://api.prod.whoop.com/developer/v2'


class WhoopAPIError(RuntimeError):
    '''Raised when the WHOOP API returns a non-success response.'''

  
@dataclass(frozen=True)
class WhoopClientConfig:
    '''
    Configuration for WhoopClient.

    Attributes:
        client_id: WHOOP OAuth client ID.
        client_secret: WHOOP OAuth client secret.
        redirect_uri: Redirect URI configured in the WHOOP developer app.
        access_token: Optional current access token.
        refresh_token: Optional current refresh token.
        timeout_s: HTTP timeout in seconds.
        max_retries: Retry attempts for transient failures.
        backoff_base_s: Base exponential backoff.
        backoff_max_s: Max exponential backoff.
    '''
    client_id: str
    client_secret: str
    redirect_uri: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    timeout_s: int = 30
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 20.0
    token_update_callback: Optional[Callable[[Dict[str, Any]], None]] = None
    

class WhoopClient:
    '''
    Client for interacting with the WHOOP API.
    '''
    def __init__(self, config: WhoopClientConfig) -> None:
        self._config = config
        self._access_token = config.access_token
        self._refresh_token = config.refresh_token
        self._session = requests.Session()

        logger.debug(
            'Initialized WhoopClient timeout=%ss max_retries=%s',
            self._config.timeout_s,
            self._config.max_retries,
        )
    
    def build_authorization_url(
        self,
        *,
        state: str,
        scope: str = 'offline read:cycles read:recovery read:sleep read:workout',
    ) -> str:
        '''
        Build the WHOOP authorization URL for the initial OAuth flow.

        Args:
            state (str): Unique state parameter for CSRF protection in OAuth flow.
            scope (str, optional): Scope of metrics we are requesting. 
            Defaults to 'offline read:cycles read:recovery read:sleep read:workout'.

        Returns:
            str: Full authorization URL
        '''
        params = {
            'response_type': 'code',
            'client_id': self._config.client_id,
            'redirect_uri': self._config.redirect_uri,
            'scope': scope,
            'state': state,
        }
        return f'{WHOOP_OAUTH_BASE}/auth?{urlencode(params)}'
    
    def exchange_code_for_tokens(self, code: str) -> Dict[str, Any]:
        '''
        Exchange an OAuth authorization code for access and refresh tokens.

        Args:
            code: Authorization code returned by WHOOP.

        Returns:
            Token response payload.
        '''
        # Store URL and data for request
        url = f'{WHOOP_OAUTH_BASE}/token'
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'client_id': self._config.client_id,
            'client_secret': self._config.client_secret,
            'redirect_uri': self._config.redirect_uri,
        }

        # Send request and store response
        response = self._request_with_retries('POST', url, data=data, auth_required=False)
        
        self._store_token_response(response)
        return response
    
    def refresh_access_token(self) -> Dict[str, Any]:
        '''
        Refreshes Whoop access token using refresh token, and 
        stores new access and refresh tokens in the client instance.

        Raises:
            WhoopAPIError: Error if no refresh token available

        Returns:
            Dict[str, Any]: Response from Whoop API
        '''
        # Check for refresh token
        if not self._refresh_token:
            raise WhoopAPIError('No Whoop refresh token available.')

        # Store URL and data for refresh token request
        url = f'{WHOOP_OAUTH_BASE}/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self._refresh_token,
            'client_id': self._config.client_id,
            'client_secret': self._config.client_secret,
            'scope': 'offline'
        }
        
        # Make request to refresh access token
        response = self._request_with_retries('POST', url, data=data, auth_required=False)
        
        self._store_token_response(response)
        logger.info('Whoop access token refreshed successfully')
        
        return response
    
    def get_cycle_collection(
        self,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 25,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''Fetch one page of WHOOP cycles.'''
        return self._get_collection_page(
            endpoint='/cycle',
            start=start,
            end=end,
            limit=limit,
            next_token=next_token,
        )

    def get_recovery_collection(
        self,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 25,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''Fetch one page of WHOOP recoveries.'''
        return self._get_collection_page(
            endpoint='/recovery',
            start=start,
            end=end,
            limit=limit,
            next_token=next_token,
        )

    def get_sleep_collection(
        self,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 25,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''Fetch one page of WHOOP sleeps.'''
        return self._get_collection_page(
            endpoint='/activity/sleep',
            start=start,
            end=end,
            limit=limit,
            next_token=next_token,
        )

    def get_workout_collection(
        self,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 25,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        '''Fetch one page of WHOOP workouts.'''
        return self._get_collection_page(
            endpoint='/activity/workout',
            start=start,
            end=end,
            limit=limit,
            next_token=next_token,
        )
    
    def iter_collection_records(
        self,
        collection_name: str,
        *,
        start: Optional[str] = None,
        end: Optional[str] = None,
        limit: int = 25,
        max_pages: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        '''
        Iterate all records from a WHOOP collection.

        Args:
            collection_name: 'cycle', 'recovery', 'sleep', 'workout'.
            start: Optional ISO 8601 datetime lower bound.
            end: Optional ISO 8601 datetime upper bound.
            limit: Page size.
            max_pages: Optional page cap for debugging.

        Yields:
            Individual collection records.
        '''
        # Create method map for fetching pages from different collections
        method_map = {
            'cycle': self.get_cycle_collection,
            'recovery': self.get_recovery_collection,
            'sleep': self.get_sleep_collection,
            'workout': self.get_workout_collection,
        }

        # Raise value error for invalid collection
        if collection_name not in method_map:
            raise ValueError(f'Unsupported collection_name: {collection_name}')

        get_page = method_map[collection_name]
        page_count = 0
        next_token: Optional[str] = None

        while True:
            # Fetch page of records from WHOOP API
            response = get_page(
                start=start,
                end=end,
                limit=limit,
                next_token=next_token,
            )

            # Extract records and next token, log response
            records = response.get('records', [])
            logger.debug(
                'Fetched WHOOP %s page_index=%s records=%s has_next=%s',
                collection_name,
                page_count + 1,
                len(records),
                bool(response.get('next_token')),
            )

            # Yield individual records
            for record in records:
                yield record

            # Increment page count and check for max pages if specified
            page_count += 1
            if max_pages is not None and page_count >= max_pages:
                return

            # Retrieve next token, break loop if no more pages
            next_token = response.get('next_token')
            if not next_token:
                return

    def close(self) -> None:
        '''Close the underlying HTTP session.'''
        self._session.close()

    def _store_token_response(self, response: Dict[str, Any]) -> None:
        '''
        Store access and refresh tokens returned by WHOOP and persist them if configured.
        '''
        access_token = response.get('access_token')
        refresh_token = response.get('refresh_token')

        if access_token:
            self._access_token = access_token
        if refresh_token:
            self._refresh_token = refresh_token

        if self._config.token_update_callback:
            self._config.token_update_callback(response)
        
    def _get_collection_page(
        self,
        *,
        endpoint: str,
        start: Optional[str],
        end: Optional[str],
        limit: int,
        next_token: Optional[str],
    ) -> Dict[str, Any]:
        '''
        Fetches a single page of records from a WHOOP collection endpoint with
        optional parameters for filtering and pagination.

        Args:
            endpoint (str): Whoop API endpoint for collection.
            start (Optional[str]): ISO 8601 datetime for start of filter range.
            end (Optional[str]): ISO 8601 datetime for end of filter range.
            limit (int): Number of records to fetch in the page.
            next_token (Optional[str]): Token for fetching the next page of results.

        Returns:
            Dict[str, Any]: Response from Whoop API
        '''
        # Store parameters for Whoop collection request
        params: Dict[str, Any] = {
            'limit': limit
        }
        
        # Add optional parameters if provided
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        if next_token:
            params['next_token'] = next_token
            
        # Create request URL
        url = f'{WHOOP_API_BASE}{endpoint}'
        
        # Make request to Whoop API with retries
        return self._request_with_retries(
            'GET', 
            url, 
            params=params,
            auth_required=True,
        )
    
    def _request_with_retries(
        self,
        method: str,
        url: str,
        *,
        auth_required: bool,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        '''
        Send an HTTP request to the Whoop API with retries and exponential backoff
        for transient errors, and automatic token refresh for unauthorized errors 
        when auth is required.

        Args:
            method (str): HTTP method (e.g. 'GET', 'POST').
            url (str): Full URL for the request.
            auth_required (bool): Requires authentication.

        Raises:
            WhoopAPIError: No access token configured when auth is required.
            WhoopAPIError: WHOOP API returned non-success status code after retries.
            WhoopAPIError: Request failed after retries with no response.

        Returns:
            Dict[str, Any]: Parsed JSON response from Whoop API on success.
        '''
        # Store last error 
        last_error: Optional[str] = None
        
        # Loop through attmepts with exponential backoff
        for attempt in range(self._config.max_retries + 1):
            # Store headers for request
            headers = kwargs.pop('headers', {})
            if auth_required:
                if not self._access_token:
                    raise WhoopAPIError('No WHOOP access token is configured.')
                headers['Authorization'] = f'Bearer {self._access_token}'
            
            try:
                # Send request to Whoop API
                response = self._session.request(
                    method,
                    url,
                    headers=headers,
                    timeout=self._config.timeout_s,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_error = f'Request failed: {exc}'
                self._sleep_backoff(attempt)
                continue
            
            # Check for successful response
            if 200 <= response.status_code < 300:
                return response.json()
            
            # Handle unauthorized error by attempting token refresh if auth is required
            if response.status_code == 401 and auth_required and self._refresh_token:
                logger.warning('WHOOP access token unauthorized; attempting refresh')
                self.refresh_access_token()
                continue
            
            # Handle rate limit and server errors with backoff and retry
            if response.status_code in (429, 500, 502, 503, 504):
                last_error = self._format_error(response)
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except ValueError:
                        self._sleep_backoff(attempt)
                else:
                    self._sleep_backoff(attempt)
                continue

            raise WhoopAPIError(self._format_error(response))

        raise WhoopAPIError(last_error or 'WHOOP API request failed after retries.')
        
    def _sleep_backoff(self, attempt: int) -> None:
        '''Sleep using exponential backoff.'''
        delay = min(
            self._config.backoff_max_s,
            self._config.backoff_base_s * (2**attempt),
        )
        time.sleep(delay)

    @staticmethod
    def _format_error(response: requests.Response) -> str:
        '''Format WHOOP API error response.'''
        try:
            body = response.json()
        except Exception:
            body = response.text.strip()
        return f'WHOOP API error {response.status_code}: {body}'
        
    
    
