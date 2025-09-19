# hubspot.py

import json
import secrets
from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse
import httpx
import asyncio
import base64
import requests
from integrations.integration_item import IntegrationItem
from pprint import pprint

from redis_client import add_key_value_redis, get_value_redis, delete_key_redis


from fastapi import Request


CLIENT_ID = "a7a4f3d2-6c2d-4f57-96f4-0ab78ff3cff8"
CLIENT_SECRET = "0041075b-3c52-44d2-af05-08012f3ef906"
REDIRECT_URI = "http://localhost:8000/integrations/hubspot/oauth2callback"
SCOPES = "crm.objects.contacts.read crm.objects.companies.read crm.objects.deals.read"

OBJECT_CONFIG = {
    "contacts": {
        "properties": ["firstname", "lastname", "email"],
        "name_fields": ["firstname", "lastname"],
        "type_name": "Contact",
    },
    "companies": {
        "properties": ["name", "domain", "industry"],
        "name_fields": ["name"],
        "type_name": "Company",
    },
    "deals": {
        "properties": ["dealname", "dealstage", "amount"],
        "name_fields": ["dealname"],
        "type_name": "Deal",
    },
}


async def authorize_hubspot(user_id, org_id):
    state_data = {
        "state": secrets.token_urlsafe(16),
        "user_id": user_id,
        "org_id": org_id,
    }
    encoded_state = json.dumps(state_data)

    await add_key_value_redis(
        f"hubspot_state:{org_id}:{user_id}", json.dumps(state_data), expire=600
    )

    auth_url = (
        f"https://app.hubspot.com/oauth/authorize"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES}"
        f"&state={encoded_state}"
    )
    return auth_url


async def oauth2callback_hubspot(request: Request):
    if request.query_params.get("error"):
        raise HTTPException(status_code=400, detail=request.query_params.get("error"))

    code = request.query_params.get("code")
    encoded_state = request.query_params.get("state")
    state_data = json.loads(encoded_state)

    original_state_token = state_data.get("state")
    user_id = state_data.get("user_id")
    org_id = state_data.get("org_id")

    saved_state_json = await get_value_redis(f"hubspot_state:{org_id}:{user_id}")

    if not saved_state_json:
        raise HTTPException(status_code=400, detail="State not found or expired.")

    saved_state_data = json.loads(saved_state_json)

    if original_state_token != saved_state_data.get("state"):
        raise HTTPException(status_code=400, detail="State mismatch error.")

    async with httpx.AsyncClient() as client:
        token_url = "https://api.hubapi.com/oauth/v1/token"
        payload = {
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri": REDIRECT_URI,
            "code": code,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response, _ = await asyncio.gather(
            client.post(token_url, data=payload, headers=headers),
            delete_key_redis(f"hubspot_state:{org_id}:{user_id}"),
        )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to get token: {response.text}",
        )

    await add_key_value_redis(
        f"hubspot_credentials:{org_id}:{user_id}",
        json.dumps(response.json()),
        expire=3600,
    )

    close_window_script = """
    <html>
        <script>
            window.close();
        </script>
    </html>
    """
    return HTMLResponse(content=close_window_script)


async def get_hubspot_credentials(user_id, org_id):
    credentials = await get_value_redis(f"hubspot_credentials:{org_id}:{user_id}")
    if not credentials:
        raise HTTPException(status_code=404, detail="Credentials not found.")

    await delete_key_redis(f"hubspot_credentials:{org_id}:{user_id}")
    return json.loads(credentials)


async def fetch_hubspot_object(object_type, access_token, client):
    config = OBJECT_CONFIG[object_type]
    headers = {"Authorization": f"Bearer {access_token}"}
    base_api_url = f"https://api.hubapi.com/crm/v3/objects/{object_type}"

    all_results = []
    after_cursor = None
    has_more = True

    while has_more:
        try:
            params = {"properties": ",".join(config["properties"])}
            if after_cursor:
                params["after"] = after_cursor

            response = await client.get(base_api_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if results:
                all_results.extend(results)

            next_page_info = data.get("paging", {}).get("next")
            if next_page_info and "after" in next_page_info:
                after_cursor = next_page_info["after"]
            else:
                has_more = False

        except httpx.HTTPStatusError:
            has_more = False

    return [
        create_integration_item_metadata_object(item, config) for item in all_results
    ]


def create_integration_item_metadata_object(response_json, config):
    properties = response_json.get("properties", {})
    name_parts = [properties.get(field, "") for field in config["name_fields"]]
    name = " ".join(filter(None, name_parts)).strip()
    return IntegrationItem(
        id=response_json.get("id"),
        type=config["type_name"],
        name=name,
        creation_time=response_json.get("createdAt"),
        last_modified_time=response_json.get("updatedAt"),
    )


async def get_items_hubspot(credentials):
    credentials_dict = json.loads(credentials)
    access_token = credentials_dict.get("access_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Missing access token.")

    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_hubspot_object(obj_type, access_token, client)
            for obj_type in OBJECT_CONFIG.keys()
        ]

        list_of_lists = await asyncio.gather(*tasks)

    all_items = []
    for sublist in list_of_lists:
        for item in sublist:
            all_items.append(item)

    for item in all_items:
        pprint(item.__dict__)

    print(f"Loaded a total of {len(all_items)} items from HubSpot.")
    return all_items

