import json
import logging

import os

import pandas as pd
from selenium import webdriver
import urllib.parse
from selenium.webdriver.chrome.options import Options

from tenacity import retry, wait_exponential, retry_if_exception_type, stop_after_attempt
from src.utils import (
    get_souped_page,
    get_season_name_to_build_a_url,
    NO_TIER,
    NO_COUNTRY_URL,
    INTERNATIONAL_COUNTRY_NAME,
    CUP_COMPETITION,
    DOMESTIC_TYPE,
    COMPETITION,
    TIER,
    YOUTH,
    TRANSFERMARKT_BASE_URL,
    DUMMY_ID_VALUE,
)


class EmptySessionStorageException(Exception):
    def __init__(self, message):
        self.message = message


class TMScrapingException(Exception):
    def __init__(self, message):
        self.message = message


@retry(
    retry=retry_if_exception_type(EmptySessionStorageException),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
)
def check_session_storage_keys(driver):
    num_keys = driver.execute_script(f"return window.sessionStorage.length;")
    if num_keys == 0:
        raise EmptySessionStorageException("No keys found in session storage")
    return num_keys


class CompetitionScraper:
    COLS_IN_ORDER = [
        "competition_name",
        "competition_code",
        "competition_url",
        "competition_tier",
        "country_name",
        "country_id",
        "country_url",
    ]

    def __init__(self, url: str = TRANSFERMARKT_BASE_URL):
        super().__init__()
        self.url = url
        self._driver = None

    @property
    def driver(self):
        if self._driver is None:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            self._driver = webdriver.Chrome(options=chrome_options)
        return self._driver

    def get_countries_info_from_session_storage(self):
        with self.driver as drv:
            num_keys = self._get_num_of_keys(drv)
            for i in range(num_keys):
                js_code = f"window.sessionStorage.key({i})"
                key = drv.execute_script(f"return {js_code};")
                if "countries" in key:
                    df = self.get_df_from_key(key, drv)
                    df.rename(columns={"id": "country_id", "name": "country_name", "link": "country_url"}, inplace=True)
                    return df
            raise TMScrapingException(f"No data found for url: {self.url}.")

    def get_competitions_info_from_session_storage(self):
        with self.driver as drv:
            num_keys = self._get_num_of_keys(drv)
            for i in range(num_keys):
                js_code = f"window.sessionStorage.key({i})"
                key = drv.execute_script(f"return {js_code};")
                if "competitions" in key:
                    df_with_urls = self.get_df_from_key(key, drv)
                    if not df_with_urls.empty:
                        comp_info_list = []
                        for ix, row in df_with_urls.iterrows():
                            try:
                                comp_info_list.append(
                                    self.get_competition_info_from_competition_url(
                                        url=f"{TRANSFERMARKT_BASE_URL}{row['link']}"
                                    )
                                )
                            except Exception:
                                logging.info(f"Error while getting competition info for {row['link']}")
                                continue
                        df = pd.concat(comp_info_list, ignore_index=True)
                        return df[self.COLS_IN_ORDER]
            raise TMScrapingException(f"No data found for url: {self.url}")

    def _get_num_of_keys(self, driver):
        driver.get(self.url)
        num_keys = check_session_storage_keys(driver)
        return num_keys

    def get_df_from_key(self, key, driver):
        key_value = driver.execute_script(f"return window.sessionStorage.getItem('{key}');") or ""
        # parse the value string representation of a list of jsons into a list of dicts
        try:
            value = json.loads(key_value)
        except Exception:
            value = [dict.fromkeys(self.COLS_IN_ORDER)]
        data_from_key = pd.DataFrame(value)
        return data_from_key

    def get_competition_info_from_competition_url(self, url=None):
        url = url if url else self.url
        competition_code = url.rsplit("/saison_id")[0].rsplit("/", 1)[1]
        logging.info(f"Getting competition data for competition_code: {competition_code}.")
        souped_page = get_souped_page(url)
        competition_name = souped_page.find("h1").text.strip()
        # set default values for country_name, country_id and country_url that are obtained from the scraped page
        country_name = INTERNATIONAL_COUNTRY_NAME
        country_id = DUMMY_ID_VALUE
        country_url = NO_COUNTRY_URL
        competition_tier = NO_TIER
        # get competition header details
        cup_or_not = souped_page.find("li", {"class": "data-header__label"}).text
        if CUP_COMPETITION in url or "Type of cup" in cup_or_not:
            header_details = souped_page.find("div", {"class": "data-header__details"})
            # get remaining cup data considering if it's a domestic cup, else default values are kept
            type_of_cup = header_details.find("li", {"class": "data-header__label"}).text
            if DOMESTIC_TYPE in type_of_cup:
                country_name = header_details.find("img")["title"].strip()
                countries_df = self.get_countries_info_from_session_storage()
                country_id = countries_df[countries_df["country_name"] == country_name]["country_id"].iloc[0]
                country_url = countries_df[countries_df["country_name"] == country_name]["country_url"].iloc[0]
        elif COMPETITION in url:
            country_data = souped_page.find("div", {"class": "data-header__club-info"}).find("a")
            country_name = country_data.text.strip()
            country_url = country_data["href"]
            country_id = int(country_url.rsplit("/", 1)[1])
            data_header = souped_page.find("span", {"class": "data-header__label"})
            if TIER in data_header.text.lower() or YOUTH in data_header.text.lower():
                competition_tier = data_header.text.split("\n")[2].strip()
        else:
            raise TMScrapingException(
                f"Competition url didn't contain either {CUP_COMPETITION} nor {COMPETITION} in it. Take a look to see"
                f"what that page look like. The url is the following: {url}"
            )

        return pd.DataFrame(
            {
                "competition_name": competition_name,
                "competition_code": competition_code,
                "competition_url": url.split(".com")[1].split("/saison_id")[0],
                "competition_tier": competition_tier,
                "country_id": country_id,
                "country_name": country_name,
                "country_url": country_url,
            },
            index=[0],
        )

    @staticmethod
    def get_competition_url_when_is_missing_in_competitions(
            competition_code: str,
            country_id: int,
            season_name: str,
    ) -> str:
        season_name_for_url = get_season_name_to_build_a_url(season_name)
        country_url = f"{TRANSFERMARKT_BASE_URL}/wettbewerbe/national/wettbewerbe/{country_id}"
        url = f"{country_url}/plus/?saison_id={season_name_for_url}"
        souped_page = get_souped_page(url)
        competition_url = None
        a_tags = souped_page.find_all("a")
        for a in a_tags:
            if competition_code in a["href"]:
                competition_url = a["href"].split("/saison_id")[0]
                break
        return competition_url
