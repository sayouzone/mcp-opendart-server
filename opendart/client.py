# Copyright (c) 2025, Sayouzone
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
"""
OpenDart API 클라이언트
"""

import requests
import time

from .utils import API_URL

class OpenDartClient:
    """OpenDart API 클라이언트"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36'
        })
        self._rate_limit_delay = 0.1  # OpenDart 요청 제한 준수

    def _rate_limit(self):
        """API 호출 제한"""
        time.sleep(self._rate_limit_delay)

    def _get(self, url: str, params: dict = None, headers: dict = None, referer: str = None) -> requests.Response:
        """GET 요청 (rate limit 적용)"""
        self._rate_limit()
        
        if referer:
            self.session.headers.update({'Referer': referer})
        
        response = self.session.get(url, params=params, headers=headers)

        response.raise_for_status()
        response.encoding = 'utf-8'

        return response
