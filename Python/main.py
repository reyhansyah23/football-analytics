from premier_league import PremierLeagueScraper
from postgre_loader import PostgresLoader


if __name__ == "__main__":
    scraper = PremierLeagueScraper(season=2025)

    teams_df = scraper.get_teams()
    players_df = scraper.get_all_squads()
    matches_df = scraper.get_matches(matchweeks=2)

    loader = PostgresLoader(
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432,
        database="football_db"
    )

    loader.load_dataframe(teams_df, "raw_teams", schema="raw")
    loader.load_dataframe(players_df, "raw_players", schema="raw")
    loader.load_dataframe(matches_df, "raw_matches", schema="raw")