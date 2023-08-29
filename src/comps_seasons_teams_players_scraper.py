import logging
from datetime import datetime
from typing import List, Union
from tqdm import tqdm

import pandas as pd
import urllib.parse

from src.competition_scraper import CompetitionScraper
from utils import (
    get_souped_page,
    get_season_names_to_process_for_a_given_year,
    get_season_name_to_build_a_url,
    TRANSFERMARKT_BASE_URL
)


class CompetitionsSeasonsTeamsScraperException(Exception):
    pass


class CompetitionsSeasonsTeamsScraper:
    def __init__(
        self,
        url: str = TRANSFERMARKT_BASE_URL,
        season_name: str = None,
        country_id: Union[int, List[int]] = None,
        seasons_to_process: Union[str, List[str]] = None,
    ):
        self.url = url
        self.season_name = season_name
        self.seasons_to_process = seasons_to_process
        self.country_id = country_id

    @property
    def country_id(self):
        return self._country_id

    @country_id.setter
    def country_id(self, value):
        if isinstance(value, int):
            self._country_id = [value]
        else:
            self._country_id = value

    @staticmethod
    def _get_table_of_interest(souped_page, teams=True):
        word_to_find = "club"
        if not teams:
            word_to_find = "player"
        tables = souped_page.find_all("div", {"class": "responsive-table"})
        for t in tables:
            for th in t.find_all("th"):
                if word_to_find in th.text.lower():
                    return t

    @staticmethod
    def convert_id_cols_to_int(df):
        df["team_id"] = df["team_id"].astype(int)
        return df

    def get_competitions_to_update(self):
        competitions_to_update = []
        if self.url == TRANSFERMARKT_BASE_URL:
            countries_df = CompetitionScraper().get_countries_info_from_session_storage()
            countries_df = countries_df[countries_df["country_id"].isin(self.country_id)]
            for ix, row in countries_df.iterrows():
                country_url = row['country_url']
                competitions_to_update.append(
                    CompetitionScraper(url=f'{TRANSFERMARKT_BASE_URL}{country_url}').get_competitions_info_from_session_storage()
                )
            return pd.concat(competitions_to_update, ignore_index=True).dropna(subset=['competition_url'])
        else:
            return CompetitionScraper(url=self.url).get_competitions_info_from_session_storage().dropna(
                subset=['competition_url']
            )

    def _get_team_names_ids_and_urls(self, souped_page, full_url):
        teams_table = self._get_table_of_interest(souped_page)
        team_names = []
        team_ids = []
        team_urls = []
        try:
            if teams_table:
                even = teams_table.select("tbody")[0].find_all("tr", {"class": "even"})
                odd = teams_table.select("tbody")[0].find_all("tr", {"class": "odd"})
                for row in odd + even:
                    team_names.append(row.select("a")[0]["title"])
                    team_url = row.select("a")[0]["href"].split("/saison_id")[0]
                    team_urls.append(team_url)
                    team_ids.append(int(team_url.split("/")[-1]))
            return pd.DataFrame(
                {
                    "team_name": team_names,
                    "team_id": team_ids,
                    "team_url": team_urls,
                }
            )
        except IndexError:
            logging.error(
                f"Error while scraping teams. Tbody not found for clubs_table.\n"
                f"Probably something is different than expected in HTML structure. Url: {full_url}."
            )
            return pd.DataFrame()
        except CompetitionsSeasonsTeamsScraperException as e:
            logging.error(f"Error while scraping teams for url: {full_url}.\nException: {e}")
            return pd.DataFrame()

    def get_competitions_seasons_teams_data(self):
        logging.info("Executing get_competitions_seasons_teams_data.")
        competitions_to_update = self.get_competitions_to_update()
        if not competitions_to_update.empty:
            # teams_data_for_comps = pd.DataFrame()
            teams_data_for_comps = []
            for ix, row in competitions_to_update.iterrows():
                logging.info(f"Processing competition {row['competition_name']} for season {self.season_name}.")
                short_url = row["competition_url"]
                logging.info(f"Processing competition with url: {short_url}")
                season_name_for_url = get_season_name_to_build_a_url(self.season_name)
                full_url = f"{TRANSFERMARKT_BASE_URL}{short_url}/plus/?saison_id={season_name_for_url}"
                souped_page = get_souped_page(full_url)
                teams_data = self._get_team_names_ids_and_urls(souped_page, full_url)
                if teams_data.empty:
                    logging.info(
                        f"No data found for competition {row['competition_name']}, season {self.season_name} "
                        f"and competition_code {row['competition_code']}.\n"
                        f"Url: {full_url}"
                    )
                    continue
                teams_data["competition_name"] = row["competition_name"]
                teams_data["competition_code"] = row["competition_code"]
                teams_data["season_name"] = self.season_name
                teams_data_for_comps.append(teams_data)
            if teams_data_for_comps:
                return self.convert_id_cols_to_int(pd.concat(teams_data_for_comps))
        return pd.DataFrame()













class CompetitionsSeasonsTeamsPlayersScraper(CompetitionsSeasonsTeamsScraper):
    def __init__(self, season_name=None, competitions_seasons_teams=None):
        super().__init__(season_name=season_name)
        self._competitions_seasons_teams = competitions_seasons_teams
        self._player_updater = None

    @property
    def competitions_seasons_teams(self):
        if self._competitions_seasons_teams is None:
            self._competitions_seasons_teams = self.get_competitions_seasons_teams_data()
        return self._competitions_seasons_teams

    def get_competitions_seasons_teams_players_data(self):
        total_cstp_data = pd.DataFrame()
        if not self.competitions_seasons_teams.empty:
            for _, row in tqdm(
                self.competitions_seasons_teams.iterrows(),
                total=self.competitions_seasons_teams.shape[0],
            ):
                logging.info(
                    f"Processing team {row['team_name']} for competition {row['competition_name']} "
                    f"and season {row['season_name']}."
                )

                cstp_data = pd.DataFrame()
                season_name_for_url = get_season_name_to_build_a_url(row["season_name"])
                full_url = TRANSFERMARKT_BASE_URL + row["team_url"] + f"/plus/1?saison_id={season_name_for_url}"
                souped_page = get_souped_page(full_url)
                players_table = self._get_table_of_interest(souped_page, teams=False)

                try:
                    if players_table:
                        odd = players_table.select("tbody")[0].find_all("tr", {"class": "odd"})
                        even = players_table.select("tbody")[0].find_all("tr", {"class": "even"})
                        for player_row in tqdm(odd + even, total=len(odd + even)):
                            inline_tables = player_row.find_all("table", {"class": "inline-table"})
                            player_url = inline_tables[0].find_all("a")[0]["href"]
                            player_img = inline_tables[0].find("a").find("img") or inline_tables[0].find("img")
                            row["player_id"] = int(player_url.rsplit("/", 1)[1])
                            row["player_name"] = urllib.parse.unquote(player_img["alt"], encoding="utf-8")
                            row["player_url"] = player_url
                            cstp_data = cstp_data.append(row)

                except (Exception, IndexError) as e:
                    logging.error(f"Error while scraping player data for player_url: {full_url}.\n" f"Exception: {e}")
                    continue

                if not cstp_data.empty:
                    cstp_data = self.convert_id_cols_to_int(cstp_data)
                    total_cstp_data = total_cstp_data.append(cstp_data)

        return total_cstp_data


if __name__ == '__main__':
    s = CompetitionsSeasonsTeamsScraper(country_id=12, season_name='2023/2024')
    df = s.get_competitions_seasons_teams_data()
    print(df)
