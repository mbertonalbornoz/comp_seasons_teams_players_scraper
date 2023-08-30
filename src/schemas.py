from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.schema import MetaData
from sqlalchemy.ext.declarative import declarative_base

TRANSFERMARKT_SCHEMA_NAME = "tm"
TmDeclarativeBase = declarative_base(metadata=MetaData(schema=TRANSFERMARKT_SCHEMA_NAME))


class CompetitionsSeasonsTeams(TmDeclarativeBase):
    __tablename__ = "comps_seasons_teams"

    competition_name = Column(String)
    competition_code = Column(String)
    season_name = Column(String)
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String)
    team_url = Column(String)

    created = Column(DateTime, nullable=False, default=datetime.now())
    last_updated = Column(DateTime, nullable=False, default=datetime.now(), onupdate=datetime.now())


class CompetitionsSeasonsTeamsPlayers(TmDeclarativeBase):
    __tablename__ = "comps_seasons_teams_players"

    competition_name = Column(String)
    competition_code = Column(String)
    season_name = Column(String)
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String)
    team_url = Column(String)
    player_id = Column(Integer, primary_key=True)
    player_name = Column(String)
    player_url = Column(String)

    created = Column(DateTime, nullable=False, default=datetime.now())
    last_updated = Column(DateTime, nullable=False, default=datetime.now(), onupdate=datetime.now())