# Real‑cast Project

**Building an Efficient ETL Pipeline for Property Records in Real Estate**

**Specialization:** Data Engineering\
**Business Focus:** Real Estate (Zipco Real Estate Agency)\
**Tools:** Python · PostgreSQL · SQLAlchemy · GitHub\
**Project Level:** Beginner

---

## Table of Contents

1. [Business Context](#business-context)
2. [Problem Statement](#problem-statement)
3. [Project Objectives](#project-objectives)
4. [Assumptions](#assumptions)
5. [Architecture](#architecture)
6. [Data Extraction](#data-extraction)
7. [Data Transformation](#data-transformation)
8. [Data Schema](#data-schema)
9. [Data Loading](#data-loading)
10. [Automation & Scheduling](#automation--scheduling)
11. [Project Structure](#project-structure)
12. [Setup & Run Locally](#setup--run-locally)
13. [Environment Variables](#environment-variables)
14. [Contributing](#contributing)
15. [License & Contact](#license--contact)

---

## Business Context

Zipco Real Estate Agency operates in the fast-paced, competitive world of real estate. Their success hinges on:

- Deep knowledge of local market dynamics in the South Atlantic Division (DE, FL, GA, MD, NC, SC, VA, WV)
- Exceptional customer service and robust online presence
- Up-to-date property listings (no older than 5 days)

Timely access to accurate data is crucial for securing the hottest deals and maintaining a competitive edge.

## Problem Statement

Current challenges at Zipco:

1. **Inefficient Data Processing:** Manual workflows delay access to critical property information.
2. **Disparate & Inconsistent Datasets:** Multiple sources and formats complicate analysis and reporting.
3. **Compromised Data Quality:** Inaccuracies and outdated records lead to poor decision-making.
4. **High Operational Costs:** Manual data reconciliation diverts resources from growth activities.

## Project Objectives

- **Automate** the full ETL pipeline (Extract → Transform → Load) using Python and SQLAlchemy.
- **Standardize** and **cleanse** diverse property records.
- **Load** structured data into a PostgreSQL database with a 3NF schema.
- **Schedule** batch runs every 5 days to ensure the freshest listings.

## Assumptions

1. Zipco deals in **both rental** and **sales** listings.
2. Operations are focused on the **South Atlantic Division** (DE, FL, GA, MD, NC, SC, VA, WV).
3. Only properties listed within the **last 5 days** are ingested to maintain recency.

## Architecture

A high‑level overview can be found in the `schema_and_architecture/` folder (created in Canva).

```
┌────────────┐    ┌─────────────┐    ┌────────────┐
│  RentCast  │ →  │  Extraction │ →  │ Pre‑Clean  │
└────────────┘    └─────────────┘    └────────────┘
                                ↓
                           ┌──────────────┐
                           │ CleaningJob  │
                           └──────────────┘
                                ↓
                           ┌─────────────┐
                           │  Loading    │ → PostgreSQL
                           └─────────────┘
```

## Data Extraction

- **Source:** RentCast API (rental & sales endpoints)
- **Challenges:** 500‑row limit per call, one state per request
- **Solution:** Loop through 8 states & two endpoints, filter by `days_listed ≤ 5`, aggregate results.

## Data Transformation

1. **Pre‑cleaning:** Parse JSON into sub‑datasets:
   - Sales: `sales_info`, `property_history`, `agent_info`, `officer_info`
   - Rentals: `rental_info`, `property_history`
2. **CleaningJob:**
   - Load into pandas DataFrames
   - Drop duplicates & unnecessary columns
   - Fill selective missing values
   - Convert types (e.g. dates) & strip whitespace/symbols

## Data Schema

- **Normalization:** 3NF
- **Sales Tables (4):**
  - `sales_info` (central)
  - `property_history`
  - `agent_info`
  - `officer_info`
- **Rentals Tables (2):**
  - `rental_info` (central)
  - `property_history`

Foreign keys enforce 1‑to‑many relationships between central tables and their history/sub‑tables.

## Data Loading

- **Database:** PostgreSQL
- **ORM:** SQLAlchemy
- **Batch Logic:**
  - **First run:** `if_exists='replace'`
  - **Subsequent (every 5 days):** `if_exists='append'`

> *Note: Future PRs are welcome to propose upserts or merge logic to handle duplicates more gracefully.*

## Automation & Scheduling

Use a scheduler (e.g., cron, Airflow) to trigger the pipeline every **5 days**:

```bash
# Example cron: run at midnight every 5 days
0 0 */5 * * cd /path/to/Real-cast && python load.py
```

## Project Structure

```
Real-cast/
├── clean_data/               # Parquet files from first batch run
├── schema_and_architecture/  # Architecture diagrams (Canva exports)
├── data.py                   # Handles API extraction
├── precleaning.py            # JSON parsing & sub‑dataset extraction
├── cleaning_job.py           # DataFrame cleaning & transformation
├── load.py                   # ORM logic to load into PostgreSQL
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Setup & Run Locally

1. **Fork & Clone** this repo:
   ```bash
   git clone [https://github.com/Nel-zi/Real-cast-project cd Real-cast]
   ```



````

2. **Create & activate** a virtual environment:  
   ```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
.\.venv\Scripts\activate  # Windows
````

3. **Install** dependencies:
   ```bash
   pip install -r requirements.txt
   ``` 

````

4. **Obtain** your RentCast API key (free tier) from https://app.rentcast.io/  

5. **Create** a `.env` file in the project root:  
   ```dotenv
API_KEY="<your_api_key>"
DB_NAME="<your_db_name>"
DB_USER="<your_db_user>"
DB_PASSWORD="<your_db_password>"
DB_HOST="localhost"
DB_PORT="5432"
````

6. **Create** your PostgreSQL database and update `.env` accordingly.

7. **Run** the pipeline:

   ```bash
   python load.py
   ``` 

```

## Contributing

Contributions, issues, and feature requests are welcome!  
Feel free to open a Pull Request or an Issue with your suggested improvements, especially if you add visualizations or refine the loading logic.

## License & Contact

This project is open source under the MIT License.  

For questions or support, reach out via my GitHub profile: [@Nel-zi](https://github.com/Nel-zi)

```
