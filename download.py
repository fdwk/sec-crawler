import datetime as dt
import io
import requests
import sys
import time
from urllib.request import urlopen

class SEC_downloader:
    def __init__(self, user_email):
        self.HEADER =  {'User-Agent': user_email}
        print(self.HEADER)

    def download_to_file(self, url, file_name):
        self._download(url, type=1)

    def get_file(self, url):
        return self._download(url, type=2)

    def download_to_list(self, url):
        return self._download(url, type=3)

    def _download(self, url, type=None, file_name=None):
        # type = 1:to file, 2: return content , 3: to list
        for i in range(2):
            try:
                response = requests.get(url, headers=HEADER)
                if response.status_code == 200:
                    if type == 1:
                        with open(f_name, 'wb') as f:
                            f.write(response.content)
                        return True
                    elif type == 2:
                        return response.content.decode(encoding="UTF-8", errors='ignore' )
                    elif type == 3:
                        file_list = io.StringIO(response.content.decode(encoding="UTF-8", errors='ignore' )).readlines()
                        return file_list
                else:
                    print(f'Error in try #{i} downloader : URL = {url} , status_code = {response.status_code}')
                    if i == 2:
                        print(f'FAILED  DOWNLOAD: URL = {url}')
                    
            except Exception as exc:
                if '404' in str(exc):
                    break
                time.sleep(0.1)

downloader = SEC_downloader("test@email.com")
