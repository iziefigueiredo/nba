# nba_simple_extract.py
from pathlib import Path
import polars as pl

# 1) importar helpers prontos da nba_api (sem dor de cabeça com endpoint)
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamelog

SEASON = "2023-24"  # mude aqui se quiser
OUTDIR = Path("data/raw") / SEASON
OUTDIR.mkdir(parents=True, exist_ok=True)

# 2) times (estático)
teams_df = pl.DataFrame(teams.get_teams())
teams_df.write_parquet(OUTDIR / "teams.parquet")
print(f"times: {teams_df.shape}")

# 3) jogadores (estático)
players_df = pl.DataFrame(players.get_players())
players_df.write_parquet(OUTDIR / "players.parquet")
print(f"jogadores: {players_df.shape}")

# 4) jogos da temporada (opcional, mas simples)
games = leaguegamelog.LeagueGameLog(
    season=SEASON,
    season_type_all_star="Regular Season",   # ou "Playoffs"
    player_or_team_abbreviation="T"          # "T" = times; "P" = jogadores
)
games_df = pl.DataFrame(games.get_data_frames()[0])  # pega o primeiro dataframe retornado
games_df.write_parquet(OUTDIR / "games_regular.parquet")
print(f"jogos (regular season): {games_df.shape}")

print(f"\nOK! Arquivos salvos em: {OUTDIR.resolve()}")
