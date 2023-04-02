from typing import Dict
import requests

class APIConnection:
    def __init__(self, url: str, header: Dict) -> None:
        self.url = url
        self.header = header

    def get_request(self, endpoint: str, parameters: Dict) -> str:
        response = requests.get(
            url=self.url + endpoint,
            headers=self.header,
            params=parameters
        )
        content = response.content().json()
        return content
