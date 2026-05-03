import os
from dotenv import load_dotenv

load_dotenv()

from premier_league import PremierLeagueScraper
from postgre_loader import PostgresLoader


if __name__ == "__main__":

    print("Starting ETL Process...")

    scraper = PremierLeagueScraper(season=2025)

    teams_df = scraper.get_teams()
    players_df = scraper.get_all_squads()
    matches_df = scraper.get_matches(matchweeks=38)

    loader = PostgresLoader(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=5432,
        database=os.getenv("POSTGRES_DB")
    )

    loader.load_dataframe(teams_df, "raw_teams", schema="raw")
    loader.load_dataframe(players_df, "raw_players", schema="raw")
    loader.load_dataframe(matches_df, "raw_matches", schema="raw")

    print("ETL Process Completed Successfully!")