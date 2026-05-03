import requests
import pandas as pd
import time
import random
from datetime import datetime

now = datetime.now()

class PremierLeagueScraper:
    def __init__(self, season_start, season_end):
        self.season_start = season_start
        self.season_end = season_end
        self.base_api = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://www.premierleague.com",
            "Referer": "https://www.premierleague.com/"
        }
        
    def _get_json(self, url, params=None):
        response = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    def get_teams(self):
        """Extracts all clubs participating in the specified season."""
        df_season = pd.DataFrame()
        for season in range(self.season_start, self.season_end + 1):
            print(f"Fetching teams for season {season}...")
            url = f"{self.base_api}/v1/competitions/8/seasons/{season}/teams?_limit=20"
            try:
                data = self._get_json(url)
                
                teams = []
                for t in data.get('data', []):
                    stadium = t.get('stadium', {})
                    
                    record = {
                        "team_id": t.get("id"),
                        "season_id": season,
                        "team_name": t.get("name"),
                        "short_name": t.get("shortName"),
                        "abbreviation": t.get("abbr"),
                        "stadium_name": stadium.get("name"),
                        "city": stadium.get("city"),
                        "capacity": stadium.get("capacity"),
                        "country": stadium.get("country"),
                        "loaded_at": now
                    }
                    teams.append(record)

                print(f"Success loading teams in season {season}")
                df_season = pd.concat([df_season, pd.DataFrame(teams)], ignore_index=True)
            except Exception as e:
                print(f"Error fetching teams: {e}")
                return pd.DataFrame()

        return df_season

    def get_all_squads(self):
        """Loops through team IDs to get all players for the season."""
        
        all_players = []
        for season in range(self.season_start, self.season_end + 1):
            team_df = self.get_teams()
            team_ids = team_df[team_df['season_id'] == season]['team_id'].unique()
            for t_id in team_ids:
                print(f"Fetching Squad for Team ID: {t_id} in season {season}...")
                url = f"{self.base_api}/v2/competitions/8/seasons/{season}/teams/{t_id}/squad"
                try:
                    data = self._get_json(url)
                    team_info = data.get('team', {})

                    for p in data.get('players', []):
                        name_data = p.get('name', {})
                        date_data = p.get('dates', {})
                        country_data = p.get('country', {})
                        
                        record = {
                            "team_id": t_id,
                            "season_id": season,
                            "team_name": team_info.get("name"),
                            "player_id": p.get("id"),
                            "display_name": name_data.get("display"),
                            "first_name": name_data.get("first"),
                            "last_name": name_data.get("last"),
                            "position": p.get("position"),
                            "shirt_num": p.get("shirtNum"),
                            "birth_date": date_data.get("birth"),
                            "joined_club": date_data.get("joinedClub"),
                            "nationality": country_data.get("country"),
                            "height_cm": p.get("height"),
                            "weight_kg": p.get("weight"),
                            "preferred_foot": p.get("preferredFoot"),
                            "is_loan": p.get("loan"),
                            "loaded_at": now
                        }
                        all_players.append(record)
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    print(f"Error for team {t_id}: {e}")
            print(f"Success loading squads in season {season}")
        return pd.DataFrame(all_players)

    def get_matches(self, matchweeks=1):
        """Fetches match results for the entire season."""
        all_matches = []
        url = f"{self.base_api}/v2/matches"
        for season in range(self.season_start, self.season_end + 1):
            for mw in range(1, matchweeks + 1):
                print(f"Fetching Matchweek {mw} for season {season}...")
                params = {"competition": 8, "season": season, "matchweek": mw, "_limit": 20}
                try:
                    res = self._get_json(url, params=params)
                    matches = res.get('data', [])
                    for m in matches:
                        home, away = m.get('homeTeam', {}), m.get('awayTeam', {})
                        all_matches.append({
                        "match_id": m.get("matchId"),
                        "season_id": season,
                        "matchweek": m.get("matchWeek"),
                        "kickoff": m.get("kickoff"),
                        "status": m.get("period"), # e.g., FullTime, PreMatch
                        "result_type": m.get("resultType"), # e.g., NormalResult, Postponed
                        "ground": m.get("ground"),
                        
                        # Home Team Details
                        "home_team": home.get("name"),
                        "home_team_id": home.get("id"),
                        "home_abbr": home.get("abbr"),
                        "home_score_ht": home.get("halfTimeScore"),
                        "home_score": home.get("score") if m.get("period") == "FullTime" else None,
                        "home_redcard": home.get("redCards"),
                        
                        # Away Team Details
                        "away_team": away.get("name"),
                        "away_team_id": away.get("id"),
                        "away_abbr": away.get("abbr"),
                        "away_score_ht": away.get("halfTimeScore"),
                        "away_score": away.get("score") if m.get("period") == "FullTime" else None,
                        "away_redcard": away.get("redCards"),
                        
                        "attendance": m.get("attendance"),
                        "loaded_at": now
                        })
                    time.sleep(random.uniform(1, 2))
                except Exception as e:
                    print(f"Error for matchweek {mw}: {e}")
        return pd.DataFrame(all_matches)
    

if __name__ == "__main__":
    scraper = PremierLeagueScraper(season_start=2021, season_end=2022)
    scraper.get_all_squads()
    # df = scraper.get_all_squads()

    # print(df.sample(10))
    # print(df['season_id'].unique())