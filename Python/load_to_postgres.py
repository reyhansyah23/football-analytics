import psycopg2
from psycopg2 import sql
import pandas as pd
from datetime import datetime
import logging
from premier_league import PremierLeagueScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'premier_league',
    'user': 'postgres',
    'password': 'your_password_here',
    'port': 5432
}

# Scraper Configuration
SEASON = 2025
MATCHWEEKS = 38  # Full season

class PostgreSQLLoader:
    """Handles loading Premier League dataframes into PostgreSQL."""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Close PostgreSQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("PostgreSQL connection closed")
    
    def table_exists(self, table_name):
        """Check if table exists in PostgreSQL."""
        try:
            self.cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table_name,)
            )
            return self.cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def load_dataframe(self, df, table_name, if_exists='append'):
        """
        Load dataframe to PostgreSQL table.
        
        Args:
            df: pandas DataFrame to load
            table_name: Name of the target table
            if_exists: 'fail', 'replace', or 'append'
        """
        try:
            # Add timestamp column
            df['loaded_at'] = datetime.now()
            
            # Determine if table exists
            table_exists = self.table_exists(table_name)
            
            if table_exists:
                logger.info(f"Table '{table_name}' exists. Appending {len(df)} rows...")
                df.to_sql(table_name, self.conn, if_exists='append', index=False)
            else:
                logger.info(f"Table '{table_name}' does not exist. Creating and inserting {len(df)} rows...")
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)
            
            self.conn.commit()
            logger.info(f"Successfully loaded {len(df)} rows into '{table_name}'")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error loading data into {table_name}: {e}")
            raise
    
    def load_teams(self, scraper):
        """Extract and load teams data."""
        try:
            logger.info("Extracting teams data...")
            df_teams = scraper.get_teams()
            
            if df_teams.empty:
                logger.warning("Teams dataframe is empty")
                return
            
            self.load_dataframe(df_teams, 'teams')
            
        except Exception as e:
            logger.error(f"Error loading teams: {e}")
            raise
    
    def load_squads(self, scraper):
        """Extract and load players/squads data."""
        try:
            logger.info("Extracting squads/players data...")
            df_squads = scraper.get_all_squads()
            
            if df_squads.empty:
                logger.warning("Squads dataframe is empty")
                return
            
            self.load_dataframe(df_squads, 'players')
            
        except Exception as e:
            logger.error(f"Error loading squads: {e}")
            raise
    
    def load_matches(self, scraper, matchweeks):
        """Extract and load matches data."""
        try:
            logger.info(f"Extracting matches data for {matchweeks} matchweeks...")
            df_matches = scraper.get_matches(matchweeks=matchweeks)
            
            if df_matches.empty:
                logger.warning("Matches dataframe is empty")
                return
            
            self.load_dataframe(df_matches, 'matches')
            
        except Exception as e:
            logger.error(f"Error loading matches: {e}")
            raise
    
    def load_all(self, scraper, matchweeks=MATCHWEEKS):
        """Load all dataframes to PostgreSQL."""
        try:
            self.connect()
            
            logger.info("=" * 50)
            logger.info(f"Starting data load for Season: {SEASON}")
            logger.info("=" * 50)
            
            self.load_teams(scraper)
            self.load_squads(scraper)
            self.load_matches(scraper, matchweeks)
            
            logger.info("=" * 50)
            logger.info("All data loaded successfully!")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
        finally:
            self.close()


if __name__ == "__main__":
    try:
        # Initialize scraper
        scraper = PremierLeagueScraper(season=SEASON)
        
        # Initialize loader and load data
        loader = PostgreSQLLoader(DB_CONFIG)
        loader.load_all(scraper, matchweeks=MATCHWEEKS)
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        exit(1)
