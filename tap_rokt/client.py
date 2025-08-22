import requests
import logging
from datetime import datetime, timedelta


class RoktClient:
    """
    OAuth2 client_credentials for Rokt Reporting API
    """

    def __init__(self, client_id: str, client_secret: str, token_url: str, api_base: str):
        self.logger = logging.getLogger(__name__)

        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.api_base = api_base.rstrip('/')
        self.session = requests.Session()
        self._token = None
        self._expires_at = datetime.utcnow()

    def get_access_token(self) -> str:
        if self._token and datetime.utcnow() < self._expires_at:
            return self._token

        resp = self.session.post(
            self.token_url,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("access_token") is not None:
            self.logger.info("Successfully got access token")
        self._token = data["access_token"]
        expires = data.get("expires_in", 3600)
        self._expires_at = datetime.utcnow() + timedelta(seconds=expires - 60)
        return self._token

    def post(self, path: str, body: dict = None) -> dict:
        token = self.get_access_token()
        
        # Use simple Bearer token authentication as per Rokt docs
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Fix the API path to include /v1/ as shown in the docs
        if not path.startswith('/v1/'):
            path = f"/v1{path}"
        
        url = f"{self.api_base}{path}"
        self.logger.info(f"Making POST request to: {url}")
        self.logger.debug(f"Headers: {headers}")
        
        resp = self.session.post(url, headers=headers, json=body)
        self.logger.info(f"Response: {resp.json()}")
        resp.raise_for_status()
        return resp.json()
