import httpx
import json

class APIRequest:

    _client = httpx.AsyncClient(timeout=httpx.Timeout(1800.0),follow_redirects=True,)

    @staticmethod
    async def send_request(method: str, url: str, headers: dict, payload = None,files=None,data=None):

        try:
            if files:
                 response = await APIRequest._client.request(method, url, headers=headers, files=files)
            elif payload:
                response = await APIRequest._client.request(method, url, headers=headers, json=payload)
            elif data:
                response = await APIRequest._client.request(method, url, headers=headers, data = data)
            else:
                response = await APIRequest._client.request(method,url,headers=headers)

            return response
        except httpx.ReadTimeout as e:
             error_details = repr(e) 
        except Exception as e:
            raise e