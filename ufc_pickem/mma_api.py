from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime
from typing import Any

import aiohttp
from dotenv import load_dotenv


MMA_API_BASE_URL = "https://v1.mma.api-sports.io"
MOJIBAKE_MARKERS = ("Ã", "Å", "Ä", "Â")


def get_mma_api_key() -> str:
    load_dotenv()

    mma_api_key = os.getenv("MMA_API_KEY")

    if not mma_api_key:
        raise RuntimeError("MMA_API_KEY was not found in the .env file.")

    return mma_api_key


def repair_api_text(text: str) -> str:
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text

    try:
        repaired_text = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text

    return repaired_text


def repair_api_data(value: Any) -> Any:
    if isinstance(value, str):
        return repair_api_text(value)

    if isinstance(value, list):
        return [
            repair_api_data(list_item)
            for list_item in value
        ]

    if isinstance(value, dict):
        return {
            dictionary_key: repair_api_data(dictionary_value)
            for dictionary_key, dictionary_value in value.items()
        }

    return value


async def get_fights_for_season(season: int) -> list[dict]:
    response_data = await request_mma_api(
        endpoint="fights",
        request_parameters={"season": str(season)},
    )
    fight_rows = response_data.get("response", [])

    if not isinstance(fight_rows, list):
        raise RuntimeError("The MMA API returned an unexpected response.")

    return fight_rows


async def request_mma_api(
    endpoint: str,
    request_parameters: dict[str, str] | None = None,
) -> dict:
    request_url = f"{MMA_API_BASE_URL}/{endpoint.lstrip('/')}"
    request_headers = {
        "x-apisports-key": get_mma_api_key(),
    }
    request_timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=request_timeout) as session:
        async with session.get(
            request_url,
            headers=request_headers,
            params=request_parameters,
        ) as response:
            if response.status != 200:
                response_text = await response.text()
                raise RuntimeError(
                    f"MMA API returned HTTP {response.status}: {response_text}"
                )

            response_bytes = await response.read()
            response_data = json.loads(response_bytes.decode("utf-8"))

    response_data = repair_api_data(response_data)
    api_errors = response_data.get("errors")

    if api_errors:
        raise RuntimeError(f"MMA API error: {api_errors}")

    if not isinstance(response_data, dict):
        raise RuntimeError("The MMA API returned an unexpected response.")

    return response_data


async def get_odds_for_date(fight_date: date) -> list[dict]:
    return await get_all_mma_api_pages(
        endpoint="odds",
        request_parameters={"date": fight_date.isoformat()},
    )


async def get_odds_for_fight(api_fight_id: int) -> list[dict]:
    return await get_all_mma_api_pages(
        endpoint="odds",
        request_parameters={"fight": str(api_fight_id)},
    )


async def get_all_mma_api_pages(
    endpoint: str,
    request_parameters: dict[str, str],
) -> list[dict]:
    first_response_data = await request_mma_api(
        endpoint=endpoint,
        request_parameters=request_parameters,
    )
    odds_rows = first_response_data.get("response", [])

    if not isinstance(odds_rows, list):
        raise RuntimeError(
            "The MMA API returned an unexpected odds response."
        )

    all_odds_rows = list(odds_rows)
    paging_data = first_response_data.get("paging") or {}

    try:
        total_pages = int(paging_data.get("total") or 1)
    except (TypeError, ValueError):
        total_pages = 1

    for page_number in range(2, total_pages + 1):
        page_parameters = dict(request_parameters)
        page_parameters["page"] = str(page_number)
        page_response_data = await request_mma_api(
            endpoint=endpoint,
            request_parameters=page_parameters,
        )
        page_odds_rows = page_response_data.get("response", [])

        if not isinstance(page_odds_rows, list):
            raise RuntimeError(
                "The MMA API returned an unexpected paginated odds "
                "response."
            )

        all_odds_rows.extend(page_odds_rows)

    return all_odds_rows


async def main() -> None:
    current_season = datetime.now().year
    fight_rows = await get_fights_for_season(current_season)

    print("MMA API connection successful.")
    print(f"Season: {current_season}")
    print(f"Fight records returned: {len(fight_rows)}")

    if fight_rows:
        print("\nFirst two fight records:\n")
        print(
            json.dumps(
                fight_rows[:2],
                indent=2,
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
