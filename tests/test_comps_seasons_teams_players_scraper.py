import unittest
from datetime import datetime
from unittest.mock import patch, PropertyMock

import pandas as pd
import requests_mock

from src.utils import TRANSFERMARKT_BASE_URL
from config.paths import TEST_DATA_DIR


from src.comps_seasons_teams_players_scraper import (
    CompetitionsSeasonsTeamsScraper,
    CompetitionsSeasonsTeamsPlayersScraper,
)
from src.utils import get_souped_page
from src.schemas import CompetitionsSeasonsTeams, CompetitionsSeasonsTeamsPlayers
from tests.test_utils import get_html_text_from_a_test_data_zip_file


@patch("src.data_handling.extraction.transfermarkt.utils.get_tm_competitions")
@patch("src.config.data_loader.DataLoader")
@patch(
    "src.data_handling.extraction.transfermarkt.competition.competitions_seasons_teams_players_scraper."
    "CompetitionsSeasonsTeamsScraper.tm_competitions_seasons",
    new_callable=PropertyMock,
)
@patch(
    "src.data_handling.extraction.transfermarkt.competition.competitions_seasons_teams_players_scraper."
    "CompetitionsSeasonsTeamsScraper.tm_competitions",
    new_callable=PropertyMock,
)
class TestCompetitionsSeasonsTeamsScraper(unittest.TestCase):
    @patch("src.config.data_loader.DataLoader")
    def setUp(self, loader_mock) -> None:
        self.mls_url = f"{TRANSFERMARKT_BASE_URL}/major-league-soccer/startseite/wettbewerb/MLS1"
        self.mls_url_2022 = f"{TRANSFERMARKT_BASE_URL}/major-league-soccer/startseite/wettbewerb/MLS1/seaison_id/2021"
        self.italy_serie_b_url = f"{TRANSFERMARKT_BASE_URL}/serie-b/startseite/wettbewerb/IT2"
        self.obj = CompetitionsSeasonsTeamsScraper()
        # MLS response page
        html_mls = get_html_text_from_a_test_data_zip_file("mls_competition_page.html")
        # MLS 2022 response page
        self.html_mls_2022 = get_html_text_from_a_test_data_zip_file("mls_competition_page.html")
        # Italy Serie B response page
        self.html_serie_b = get_html_text_from_a_test_data_zip_file("italy_serie_b_competition_page.html")

    def test_expected_competition_codes_are_returned_when_instantiating_scraper_with_just_country_code(
        self, competitions_mock, competitions_seasons_mock, loader_mock, get_comp_mock
    ):
        competitions_seasons_mock.return_value = pd.read_parquet(
            f"{TEST_DATA_DIR}/competitions_seasons.parquet.gzip"
        )
        competitions_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/competitions.parquet.gzip")
        # case 1: seasons_to_process are given to constructor
        expected_combined_season_years = ["2021/2022", "2022", "2022/2023", "2023", "2023/2024"]
        obj = CompetitionsSeasonsTeamsScraper(seasons_to_process=["2022", "2023"])
        self.assertCountEqual(expected_combined_season_years, obj.seasons_to_process)

        year = str(datetime.now().year)
        month = datetime.now().month
        # case 2: year is given to constructor
        expected_seasons_to_process_1 = ["2019/2020", "2020", "2020/2021"]
        obj_1 = CompetitionsSeasonsTeamsScraper(season_name='2020')
        self.assertEqual(expected_seasons_to_process_1, obj_1.seasons_to_process)

        # case 3: nothing is given and seasons_to_process are derived from the month in which the scraper is run

        if month >= 7:
            expected_seasons_to_process_2 = [year, year + "/" + str(int(year) + 1)]
        else:
            expected_seasons_to_process_2 = [str(int(year) - 1) + "/" + year, year]
        obj_2 = CompetitionsSeasonsTeamsScraper()
        self.assertEqual(expected_seasons_to_process_2, obj_2.seasons_to_process)

    def test_get_team_names_ids_and_urls_for_a_competition_returns_the_expected_output(
        self, competitions_mock, competitions_seasons_mock, loader_mock, get_comp_mock
    ):
        competitions_seasons_mock.return_value = pd.read_parquet(
            f"{TEST_DATA_DIR}/competitions_seasons.parquet.gzip"
        )
        competitions_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/competitions.parquet.gzip")
        expected_team_df_shape = (29, 3)
        with requests_mock.Mocker() as m:
            m.get(self.mls_url, text=self.html_mls)
            souped_page = get_souped_page(self.mls_url)
            teams_in_mls = self.obj._get_team_names_ids_and_urls(souped_page, self.mls_url)

        self.assertEqual(expected_team_df_shape, teams_in_mls.shape)
        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_mls.columns))

    @patch(
        "src.data_handling.extraction.transfermarkt.competition.competitions_seasons_teams_players_scraper."
        "CompetitionsSeasonsTeamsScraper.tm_seasons",
        new_callable=PropertyMock,
    )
    def test_get_team_names_ids_urls_for_a_competition_and_season_returns_the_expected_output(
        self, seasons_mock, competitions_mock, competitions_seasons_mock, loader_mock, get_comp_mock
    ):
        seasons_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/seasons.parquet.gzip")
        expected_df_shape = (49, 3)
        with requests_mock.Mocker() as m:
            m.get(self.mls_url, text=self.html_mls)
            m.get(self.italy_serie_b_url, text=self.html_serie_b)
            mls_souped_page = get_souped_page(self.mls_url)
            italy_serie_b_souped_page = get_souped_page(self.italy_serie_b_url)
            teams_in_mls = self.obj._get_team_names_ids_and_urls(mls_souped_page, self.mls_url)
            teams_in_serie_b = self.obj._get_team_names_ids_and_urls(italy_serie_b_souped_page, self.italy_serie_b_url)

        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_mls.columns))
        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_serie_b.columns))
        self.assertEqual(expected_df_shape, (teams_in_mls.append(teams_in_serie_b)).shape)

    @patch(
        "src.data_handling.extraction.transfermarkt.competition.competitions_seasons_teams_players_scraper."
        "CompetitionsSeasonsTeamsScraper.tm_seasons",
        new_callable=PropertyMock,
    )
    def test_get_teams_with_their_data_for_a_competition_code_given_returns_the_expected_output(
        self, seasons_mock, competitions_mock, competitions_seasons_mock, loader_mock, get_comp_mock
    ):
        competitions_seasons_mock.return_value = pd.read_parquet(
            f"{TEST_DATA_DIR}/competitions_seasons.parquet.gzip"
        )
        competitions_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/competitions.parquet.gzip")
        seasons_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/seasons.parquet.gzip")
        expected_df_shape = (58, 8)
        expected_cols = [c.name for c in CompetitionsSeasonsTeams.__table__.columns][:-2]
        obj = CompetitionsSeasonsTeamsScraper(seasons_to_process=["2022", "2023"], country_id=184)
        with requests_mock.Mocker() as m:
            m.get(
                "https://www.transfermarkt.com/major-league-soccer/startseite/wettbewerb/MLS1/plus/?saison_id=2021",
                text=self.html_mls,
            )
            m.get(
                "https://www.transfermarkt.com/major-league-soccer/startseite/wettbewerb/MLS1/plus/?saison_id=2022",
                text=self.html_mls_2022,
            )
            teams_df = obj.get_competitions_seasons_teams_data()
        self.assertEqual(expected_df_shape, teams_df.shape)
        self.assertCountEqual(expected_cols, list(teams_df.columns))

    def test_get_competitions_seasons_teams_data_returns_empty_data_frame_when_competitions_to_update_is_empty(
        self, competitions_mock, competitions_seasons_mock, loader_mock, get_comp_mock
    ):
        competitions_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/competitions.parquet.gzip")
        competitions_seasons_mock.return_value = pd.DataFrame(columns=["competition_code", "season_name"])
        df = self.obj.get_competitions_seasons_teams_data()
        pd.testing.assert_frame_equal(pd.DataFrame(), df)


@patch("src.data_handling.extraction.transfermarkt.player.contract_status.get_tm_competitions")
@patch("src.config.data_loader.DataLoader")
class TestCompetitionsSeasonsTeamPlayersScraper(unittest.TestCase):
    C_S_T_DF = pd.DataFrame(
        {
            "competition_id": {0: 554, 1: 59},
            "competition_name": {0: "Major League Soccer", 1: "Serie B"},
            "competition_code": {0: "MLS1", 1: "IT2"},
            "season_id": {0: 1, 1: 9},
            "season_name": {0: "2023", 1: "2020/2021"},
            "team_id": {0: 45604, 1: 4172},
            "team_name": {0: "Orlando City SC", 1: "Pisa Sporting Club"},
            "team_url": {
                0: "/orlando-city-sc/startseite/verein/45604",
                1: "/pisa-sporting-club/startseite/verein/4172",
            },
        }
    )

    def setUp(self) -> None:
        # Orlando City response page
        self.html_orlando_city = get_html_text_from_a_test_data_zip_file("orlando_city_page.html")
        # Inter Miami response page
        self.html_pisa = get_html_text_from_a_test_data_zip_file("pisa_page.html")

    @patch(
        "src.data_handling.extraction.transfermarkt.competition.competitions_seasons_teams_players_scraper."
        "CompetitionsSeasonsTeamsScraper.get_competitions_seasons_teams_data"
    )
    def test_get_competition_season_team_players_data_returns_the_expected_output(
        self, c_s_t_mock, loader_mock, get_comp_mock
    ):
        obj = CompetitionsSeasonsTeamsPlayersScraper()

        c_s_t_mock.return_value = self.C_S_T_DF
        expected_n_players = 31 + 39
        expected_cols = [c.name for c in CompetitionsSeasonsTeamsPlayers.__table__.columns][:-2]

        with requests_mock.Mocker() as m:
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/orlando-city-sc/startseite/verein/45604/plus/1?saison_id=2022",
                text=self.html_orlando_city,
            )
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/pisa-sporting-club/startseite/verein/4172/plus/1?saison_id=2020",
                text=self.html_pisa,
            )
            players_data = obj.get_competitions_seasons_teams_players_data()

        self.assertEqual(expected_n_players, players_data.shape[0])
        self.assertCountEqual(expected_cols, list(players_data.columns))

    def test_get_competition_season_team_players_data_works_when_c_s_t_df_is_passed(self, loader_mock, get_comp_mock):
        expected_n_players = 31 + 39
        expected_cols = [c.name for c in CompetitionsSeasonsTeamsPlayers.__table__.columns][:-2]
        obj = CompetitionsSeasonsTeamsPlayersScraper(competitions_seasons_teams=self.C_S_T_DF)
        with requests_mock.Mocker() as m:
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/orlando-city-sc/startseite/verein/45604/plus/1?saison_id=2022",
                text=self.html_orlando_city,
            )
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/pisa-sporting-club/startseite/verein/4172/plus/1?saison_id=2020",
                text=self.html_pisa,
            )
            players_data = obj.get_competitions_seasons_teams_players_data()

        self.assertEqual(expected_n_players, players_data.shape[0])
        self.assertCountEqual(expected_cols, list(players_data.columns))

    def test_get_competitions_seasons_teams_players_data_returns_empty_data_frame_when_c_s_t_df_is_empty(
        self, loader_mock, get_comp_mock
    ):
        obj = CompetitionsSeasonsTeamsPlayersScraper(competitions_seasons_teams=pd.DataFrame())
        players_data = obj.get_competitions_seasons_teams_players_data()
        pd.testing.assert_frame_equal(pd.DataFrame(), players_data)


if __name__ == "__main__":
    unittest.main()
