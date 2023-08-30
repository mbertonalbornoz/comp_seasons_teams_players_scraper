import unittest
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import requests_mock

from config.paths import TEST_DATA_DIR
from src.utils import TRANSFERMARKT_BASE_URL, get_souped_page


from src.comps_seasons_teams_players_scraper import (
    CompetitionsSeasonsTeamsScraper,
    CompetitionsSeasonsTeamsPlayersScraper,
)
from src.schemas import CompetitionsSeasonsTeams, CompetitionsSeasonsTeamsPlayers
from tests.test_utils import get_html_text_from_a_test_data_zip_file


class TestCompetitionsSeasonsTeamsScraper(unittest.TestCase):
    def setUp(self) -> None:
        self.mls_url = f"{TRANSFERMARKT_BASE_URL}/major-league-soccer/startseite/wettbewerb/MLS1"
        self.mls_url_2022 = f"{TRANSFERMARKT_BASE_URL}/major-league-soccer/startseite/wettbewerb/MLS1/plus/?saison_id=2021"
        self.italy_serie_b_url = f"{TRANSFERMARKT_BASE_URL}/serie-b/startseite/wettbewerb/IT2"
        # MLS 2023 response page
        self.html_mls = get_html_text_from_a_test_data_zip_file("mls_comp_page")
        # MLS 2022 response page
        self.html_mls_2022 = get_html_text_from_a_test_data_zip_file("mls_2022_comp_page")
        # Italy Serie B response page
        self.html_serie_b = get_html_text_from_a_test_data_zip_file("serie_b_comp_page")

    def test_get_team_names_ids_and_urls_for_a_competition_returns_the_expected_output(self):
        expected_team_df_shape = (29, 3)
        with requests_mock.Mocker() as m:
            m.get(self.mls_url, text=self.html_mls)
            souped_page = get_souped_page(self.mls_url)
            obj = CompetitionsSeasonsTeamsScraper(season_name='2023')
            teams_in_mls = obj._get_team_names_ids_and_urls(souped_page, self.mls_url)

        self.assertEqual(expected_team_df_shape, teams_in_mls.shape)
        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_mls.columns))

    def test_get_team_names_ids_urls_for_a_competition_and_season_returns_the_expected_output(self):
        expected_df_shape = (49, 3)
        obj_mls = CompetitionsSeasonsTeamsScraper(season_name='2023')
        obj_serie_b = CompetitionsSeasonsTeamsScraper(season_name='2023/2024')
        with requests_mock.Mocker() as m:
            m.get(self.mls_url, text=self.html_mls)
            m.get(self.italy_serie_b_url, text=self.html_serie_b)
            mls_souped_page = get_souped_page(self.mls_url)
            italy_serie_b_souped_page = get_souped_page(self.italy_serie_b_url)
            teams_in_mls = obj_mls._get_team_names_ids_and_urls(mls_souped_page, self.mls_url)
            teams_in_serie_b = obj_serie_b._get_team_names_ids_and_urls(italy_serie_b_souped_page, self.italy_serie_b_url)

        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_mls.columns))
        self.assertEqual(["team_name", "team_id", "team_url"], list(teams_in_serie_b.columns))
        self.assertEqual(expected_df_shape, pd.concat([teams_in_mls, teams_in_serie_b]).shape)

    @patch('src.competition_scraper.CompetitionScraper.get_competitions_info_from_session_storage')
    @patch('src.competition_scraper.CompetitionScraper.get_countries_info_from_session_storage')
    def test_get_teams_with_their_data_for_a_competition_code_given_returns_the_expected_output(
            self, countries_df_mock, competitions_df_mock
    ):
        countries_df_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/countries_df.parquet.gzip")
        competitions_df_mock.return_value = pd.DataFrame(
            {
                'competition_name': {0: 'Major League Soccer'},
                'competition_code': {0: 'MLS1'},
                'competition_url': {0: '/major-league-soccer/startseite/wettbewerb/MLS1'},
                'competition_tier': {0: 'First Tier'}, 'country_id': {0: 184},
                'country_name': {0: 'United States'},
                'country_url': {0: '/wettbewerbe/national/wettbewerbe/184'}
            }
        )
        expected_df_shape = (57, 6)
        expected_cols = [c.name for c in CompetitionsSeasonsTeams.__table__.columns][:-2]
        obj = CompetitionsSeasonsTeamsScraper(season_name=["2022", "2023"], country_id=184)
        with requests_mock.Mocker() as m:
            m.get(
                "https://www.transfermarkt.com/major-league-soccer/startseite/wettbewerb/MLS1/plus/?saison_id=2022",
                text=self.html_mls,
            )
            m.get(
                "https://www.transfermarkt.com/major-league-soccer/startseite/wettbewerb/MLS1/plus/?saison_id=2021",
                text=self.html_mls_2022,
            )
            teams_df = obj.get_competitions_seasons_teams_data()
        self.assertEqual(expected_df_shape, teams_df.shape)
        self.assertCountEqual(expected_cols, list(teams_df.columns))

    @patch('src.comps_seasons_teams_players_scraper.CompetitionsSeasonsTeamsScraper.get_competitions_to_update')
    def test_get_competitions_seasons_teams_data_returns_empty_data_frame_when_competitions_to_update_is_empty(
            self, get_comp_mock
    ):
        get_comp_mock.return_value = pd.DataFrame()
        obj = CompetitionsSeasonsTeamsScraper(season_name="2023")
        df = obj.get_competitions_seasons_teams_data()
        pd.testing.assert_frame_equal(pd.DataFrame(), df)


class TestCompetitionsSeasonsTeamPlayersScraper(unittest.TestCase):
    C_S_T_DF = pd.DataFrame(
        {
            "competition_name": {0: "Major League Soccer", 1: "Serie B"},
            "competition_code": {0: "MLS1", 1: "IT2"},
            "season_name": {0: "2023", 1: "2020/2021"},
            "team_id": {0: 69261, 1: 4172},
            "team_name": {0: "Inter Miami CF", 1: "Pisa Sporting Club"},
            "team_url": {
                0: "/inter-miami-cf/startseite/verein/69261",
                1: "/pisa-sporting-club/startseite/verein/4172",
            },
        }
    )

    def setUp(self) -> None:
        # Orlando City response page
        self.html_orlando_city = get_html_text_from_a_test_data_zip_file("inter_miami_2023_page")
        # Inter Miami response page
        self.html_pisa = get_html_text_from_a_test_data_zip_file("pisa_2020_page")

    @patch(
        "src.comps_seasons_teams_players_scraper.CompetitionsSeasonsTeamsScraper.get_competitions_seasons_teams_data"
    )
    def test_get_competition_season_team_players_data_returns_the_expected_output(self, c_s_t_mock):
        c_s_t_mock.return_value = self.C_S_T_DF
        expected_n_players = 31 + 35
        expected_cols = [c.name for c in CompetitionsSeasonsTeamsPlayers.__table__.columns][:-2]

        obj = CompetitionsSeasonsTeamsPlayersScraper()
        with requests_mock.Mocker() as m:
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/inter-miami-cf/startseite/verein/69261/plus/1?saison_id=2022",
                text=self.html_orlando_city,
            )
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/pisa-sporting-club/startseite/verein/4172/plus/1?saison_id=2020",
                text=self.html_pisa,
            )
            players_data = obj.get_competitions_seasons_teams_players_data()

        self.assertEqual(expected_n_players, players_data.shape[0])
        self.assertCountEqual(expected_cols, list(players_data.columns))
        self.assertTrue('Lionel Messi' in players_data['player_name'].values)

    def test_get_competition_season_team_players_data_works_when_c_s_t_df_is_passed(self):
        expected_n_players = 31 + 35
        expected_cols = [c.name for c in CompetitionsSeasonsTeamsPlayers.__table__.columns][:-2]
        obj = CompetitionsSeasonsTeamsPlayersScraper(competitions_seasons_teams=self.C_S_T_DF)
        with requests_mock.Mocker() as m:
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/inter-miami-cf/startseite/verein/69261/plus/1?saison_id=2022",
                text=self.html_orlando_city,
            )
            m.get(
                f"{TRANSFERMARKT_BASE_URL}/pisa-sporting-club/startseite/verein/4172/plus/1?saison_id=2020",
                text=self.html_pisa,
            )
            players_data = obj.get_competitions_seasons_teams_players_data()

        self.assertEqual(expected_n_players, players_data.shape[0])
        self.assertCountEqual(expected_cols, list(players_data.columns))

    def test_get_competitions_seasons_teams_players_data_returns_empty_data_frame_when_c_s_t_df_is_empty(self):
        obj = CompetitionsSeasonsTeamsPlayersScraper(competitions_seasons_teams=pd.DataFrame())
        players_data = obj.get_competitions_seasons_teams_players_data()
        pd.testing.assert_frame_equal(pd.DataFrame(), players_data)


if __name__ == "__main__":
    unittest.main()
