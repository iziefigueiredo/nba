# NBA Player Database

This project aims to build a structured database of NBA player statistics using a full ETL pipeline. 
The objective is to extract data from the NBA API, transform it into a clean and consistent format, 
and load it into a relational PostgreSQL database for analysis and predictive modeling.

---

## Objectives
- Extract NBA player and game data directly from public APIs
- Transform and normalize the datasets using **Polars**
- Design a **star schema** database model (fact table: games, dimension tables: players, teams, seasons, stats)
- Load the processed data into **PostgreSQL** with constraints to ensure referential integrity
- Enable exploratory analysis and machine learning applications (e.g., player performance prediction, clustering)

---

## Project Structure (planned)

```
nba/
│
├── data/
│   ├── raw/                # Raw CSVs or API dumps
│   ├── interim/            # Intermediate cleaned/unified data
│   └── processed/          # Final datasets for analysis & DB load
│
├── notebooks/
│   ├── 01_exploration.ipynb   # First exploration of raw/interim data
│   └── 02_modeling.ipynb      # Statistical modeling & ML experiments
│
├── src/
│   ├── __init__.py
│   │
│   ├── extract/
│   │   ├── __init__.py
│   │   └── extract.py         # Read CSVs or API requests
│   │
│   ├── transform/             # Cleaning, unifying, feature engineering
│   │   ├── __init__.py
│   │   ├── unify.py
│   │   └── transform.py
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   ├── schema.py          # Database schema (SQLAlchemy ORM)
│   │   └── load.py            # Save to CSV/SQL/Parquet/Postgres
│
├── analysis/
│   ├── eda.py                 # Exploratory Data Analysis
│   └── viz.py                 # Visualizations
│
├── stats/
│   ├── __init__.py
│   ├── player_stats.py        # Player-level metrics
│   ├── team_stats.py          # Team-level metrics
│   └── advanced_stats.py      # Advanced analytics (PER, TS%, ORtg, DRtg, etc.)
│
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── .gitignore
├── requirements.txt
├── README.md
└── main.py                    # Orchestrates the ETL pipeline


```
