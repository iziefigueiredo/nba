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
nba-database/
│── data/
│── docs/
│── src/
│ │── data/
│ │ │── extract.py
│ │ │── unify.py
│ │ │── preprocess.py
│ │ │── split.py
│ │── database/
│ │ │── schema.py
│ │ │── load.py
│── requirements.txt
│── main.py
│── README.md
```
