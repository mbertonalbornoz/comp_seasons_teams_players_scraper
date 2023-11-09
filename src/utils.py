from typing import List

import requests
from bs4 import BeautifulSoup

# GENERAL
TRANSFERMARKT_BASE_URL = 'https://www.transfermarkt.com'
TRANSFERMARKT_REDIRECT_DEFAULT_PAGE = f"{TRANSFERMARKT_BASE_URL}/spieler-statistik/wertvollstespieler/marktwertetop"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
    "AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/64.0.3282.167 Safari/537.36"
}
# COMPETITION SCRAPER
NO_TIER = "Not Available"
NO_COUNTRY_URL = ""
INTERNATIONAL_COUNTRY_NAME = "International"
CUP_COMPETITION = "pokalwettbewerb"
DOMESTIC_TYPE = "Domestic"
COMPETITION = "wettbewerb"
TIER = "tier"
YOUTH = "youth"
DUMMY_ID_VALUE = 0
DUMMY_NAME_VALUE = ''


def get_souped_page(url: str) -> BeautifulSoup:
    """
    Takes a url and returns the souped page
    """
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code == 200 and resp.url != TRANSFERMARKT_REDIRECT_DEFAULT_PAGE:
        return BeautifulSoup(resp.content, "lxml")
    if resp.status_code == 200 and resp.url == TRANSFERMARKT_REDIRECT_DEFAULT_PAGE:
        raise Exception(
            f"TransferMarktDisabledPlayerException: {url} was redirected to {resp.url}. "
            f"This is probably because the player is disabled."
        )
    else:
        raise Exception(
            f"Could not get url: {url}. Status code: {resp.status_code}. " f"Response url:{resp.url}"
        )


def get_season_names_to_process_for_a_given_year(year: str, month: int = None) -> List[str]:
    if month is None:
        return [str(int(year) - 1) + "/" + year, year, year + "/" + str(int(year) + 1)]
    if month >= 7:
        return [year, year + "/" + str(int(year) + 1)]
    return [str(int(year) - 1) + "/" + year, year]


def get_season_name_to_build_a_url(season_name: str) -> str:
    if len(season_name.split("/")) == 1:
        season_name_for_url = str(int(season_name) - 1)
    else:
        season_name_for_url = season_name.split("/")[0]
    return season_name_for_url
