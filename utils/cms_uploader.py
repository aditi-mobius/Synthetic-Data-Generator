import os
from .send_request import APIRequest
import httpx
from typing import Optional
import uuid


# It's best practice to load sensitive data and URLs from environment variables.
CONTENT_SERVICE_URL = os.getenv("CONTENT_SERVICE_URL", "https://ig.gov-cloud.ai/mobius-content-service")


async def upload_to_cms(file_buffer, filename: str, token: Optional[str] = None):
    """Uploads a file-like object to the CMS."""
    try:
        if not token:
            print("  [WARNING] Authorization token not provided. Skipping upload.")
            return {"error": "Authorization token not provided."}

        upload_folder = uuid.uuid4()
        api_url = f"{CONTENT_SERVICE_URL}/v1.0/content/upload?filePath=spde_data/{upload_folder}"
        headers = {"Authorization": f"Bearer {token}"}
        files = {'file': (filename, file_buffer, 'application/octet-stream')}
        
        response = await APIRequest.send_request("POST", api_url, headers=headers, files=files)
        response.raise_for_status() # Raise an exception for 4xx/5xx responses
        
        response_json = response.json()
        if response_json.get("status") == "OK" and "cdnUrl" in response_json:
            return response_json
        else:
            return {"error": "CMS upload failed or did not return a CDN URL.", "details": response_json}
    except httpx.HTTPStatusError as e:
        return {"error": f"CMS returned an error: {e.response.status_code}", "details": e.response.text}
    except Exception as e:
        raise e