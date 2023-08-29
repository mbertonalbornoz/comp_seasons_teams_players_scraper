import unittest
from unittest.mock import patch, PropertyMock

import pandas as pd
import requests_mock
from config.paths import TEST_DATA_DIR
from src.competition_scraper import CompetitionScraper
from tests.test_utils import get_html_text_from_a_test_data_zip_file


@patch(
    "src.competition_scraper.CompetitionScraper.get_countries_info_from_session_storage"
)
class TestCompetitionScraper(unittest.TestCase):
    def test_get_competition_info_returns_the_expected_output_for_domestic_cups(self, countries_df_mock):
        countries_df_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/countries_df.parquet.gzip")
        i_league_url = "https://www.transfermarkt.com/i-league/startseite/wettbewerb/IND1"
        expected_output_domestic = pd.DataFrame(
            {
                "competition_name": "Indian Super League",
                "competition_code": "IND1",
                "competition_url": i_league_url.split(".com")[1],
                "competition_tier": "First Tier",
                "country_id": 67,
                "country_name": "India",
                "country_url": "/wettbewerbe/national/wettbewerbe/67",
            },
            index=[0],
        )

        html_page = get_html_text_from_a_test_data_zip_file(file_name="india_super_league_page")

        with requests_mock.Mocker() as m:
            m.get(i_league_url, text=html_page)
            obj = CompetitionScraper(url=i_league_url)
            competition_info = obj.get_competition_info_from_competition_url()
        pd.testing.assert_frame_equal(expected_output_domestic, competition_info)

    def test_get_competition_info_returns_the_expected_output_for_international_competitions(
        self, countries_df_mock
    ):
        countries_df_mock.return_value = pd.read_parquet(f"{TEST_DATA_DIR}/countries_df.parquet.gzip")
        india_domestic_cup_url = "https://www.transfermarkt.com/hero-super-cup/startseite/pokalwettbewerb/INSC"
        expected_output_inter = pd.DataFrame(
            {
                "competition_name": "Hero Super Cup",
                "competition_code": "INSC",
                "competition_url": india_domestic_cup_url.split(".com")[1],
                "competition_tier": "Not Available",
                "country_id": 67,
                "country_name": "India",
                "country_url": "/wettbewerbe/national/wettbewerbe/67",
            },
            index=[0],
        )
        html_page = get_html_text_from_a_test_data_zip_file(file_name="india_domestic_cup_page")

        with requests_mock.Mocker() as m:
            m.get(india_domestic_cup_url, text=html_page)
            obj = CompetitionScraper(url=india_domestic_cup_url)
            competition_info = obj.get_competition_info_from_competition_url()
        pd.testing.assert_frame_equal(expected_output_inter, competition_info)

    def test_get_competition_info_from_country_url_and_season_returns_the_expected_output(
        self, countries_df_mock
    ):
        html_page_norway_2022 = get_html_text_from_a_test_data_zip_file(file_name="norway_2022_country_page")
        html_page_norway_2020 = get_html_text_from_a_test_data_zip_file(file_name="norway_2020_country_page")

        with requests_mock.Mocker() as m:
            m.get(
                "https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/125/plus/?saison_id=2021",
                text=html_page_norway_2022,
            )
            m.get(
                "https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/125/plus/?saison_id=2019",
                text=html_page_norway_2020,
            )
            obj = CompetitionScraper()
            norway_2023_url = obj.get_competition_url_when_is_missing_in_competitions(
                "RTIP",
                125,
                "2022",
            )
            norway_2020_url = obj.get_competition_url_when_is_missing_in_competitions(
                "RTIP",
                125,
                "2020",
            )

            self.assertEqual("/relegation-eliteserien/startseite/wettbewerb/RTIP", norway_2023_url)
            self.assertEqual("/relegation-eliteserien/startseite/wettbewerb/RTIP", norway_2020_url)


if __name__ == "__main__":
    unittest.main()
