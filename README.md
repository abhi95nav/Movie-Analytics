# 🎬 Movie Analytics Project

> A production-grade **medallion-architecture** data pipeline on Databricks that transforms 5 raw movie CSV files into analyst-ready datamarts — then answers 7 strategic business questions a studio, distributor, or streaming platform would ask.

**10,000+ movies | 116K+ people | 150K+ cast credits | 13K+ reviews | 19 genres**

---

## 📑 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Pipeline Flow](#pipeline-flow)
- [Data Sources](#data-sources)
- [Silver Layer — Star Schema](#silver-layer--star-schema)
- [Gold Layer — Datamarts](#gold-layer--datamarts)
- [Business Cases & Key Insights](#business-cases--key-insights)
- [Unity Catalog & Storage Layout](#unity-catalog--storage-layout)
- [Tech Stack](#tech-stack)
- [How to Run](#how-to-run)
- [Author](#author)

---

## Architecture Overview

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│  ADLS (CSV)  │───▶│    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │───▶│ BUSINESS CASES / │
│  5 raw files │    │   5 tables   │    │   9 tables   │    │  5 datamarts │    │    DASHBOARD     │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────────┘
     Source            Raw + Meta         Star Schema         Denormalized         7 Analyses +
                       (strings)          (typed, clean)      (zero-join)          Visualizations
```

| Layer | Notebook | What It Does |
| --- | --- | --- |
| **Bronze** | `bronze` | Ingests raw CSVs, stamps metadata columns (`_bronze_ingested_at`, `_source_file`, `_batch_id`, `_is_valid`), writes to Delta |
| **Silver** | `silver` | Casts types, deduplicates, parses dates, builds a full star schema — 4 dimensions, 4 facts, 1 bridge |
| **Gold** | `Gold Layer Movie Datamarts` | Joins and aggregates silver into 5 wide, pre-computed datamarts for instant querying |
| **Analytics** | `Movie Industry Business Cases` | Answers 7 real-world strategy questions using the gold datamarts |
| **Dashboard** | `Movie Analytics — Industry & Talent Insights` | Interactive AI/BI dashboard with charts and filters |

---

## Pipeline Flow

```
                          ┌──────────────────────────────────────────────────┐
                          │               BRONZE LAYER                      │
  movies.csv ────────────▶│  movies   cast   crew   genres   reviews        │
  cast.csv ──────────────▶│  + _bronze_ingested_at                          │
  crew.csv ──────────────▶│  + _bronze_source_file                          │
  genres.csv ────────────▶│  + _bronze_batch_id                             │
  reviews.csv ───────────▶│  + _bronze_is_valid                             │
                          └──────────────────────┬─────────────────────────  ┘
                                                 │
                                                 ▼
                          ┌──────────────────────────────────────────────────┐
                          │               SILVER LAYER                      │
                          │                                                 │
                          │  Dimensions        Facts           Bridge       │
                          │  ───────────       ──────────      ──────       │
                          │  dim_movies        fact_metrics    bridge_      │
                          │  dim_genres        fact_cast       movie_       │
                          │  dim_people        fact_crew       genres       │
                          │  dim_date          fact_reviews                 │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 ▼
                          ┌──────────────────────────────────────────────────┐
                          │                GOLD LAYER                       │
                          │                                                 │
                          │  gold_movie_summary      gold_yearly_trends     │
                          │  gold_genre_analytics    gold_actor_analytics   │
                          │  gold_director_analytics                        │
                          └──────────────────────┬──────────────────────────┘
                                                 │
                                                 ▼
                          ┌──────────────────────────────────────────────────┐
                          │            ANALYTICS & DASHBOARD                │
                          │                                                 │
                          │  7 Business Cases + Interactive Dashboard       │
                          └─────────────────────────────────────────────────┘
```

---

## Data Sources

Five CSV files stored in **Azure Data Lake Storage Gen2**:

| File | Records | Description |
| --- | --- | --- |
| `movies.csv` | \~10,000 | Title, budget, revenue, runtime, release date, language, genres, status |
| `cast.csv` | \~150,000 | Actor–movie links with character name and billing order |
| `crew.csv` | \~64,000 | Director, writer, producer roles with department |
| `genres.csv` | 19 | Genre reference lookup |
| `reviews.csv` | \~15,000 | User reviews with author, rating, content, timestamps |

**Source path:**
```
abfss://employee@dataanlysisazuredatalake.dfs.core.windows.net/movies_data/
```

---

## Silver Layer — Star Schema

```
                              ┌────────────┐
                              │  dim_date  │
                              │  (6,486)   │
                              └─────┬──────┘
                                    │
┌─────────────┐   ┌────────────────────────────────┐   ┌──────────────┐
│ dim_genres  │   │       fact_movie_metrics        │   │  dim_movies  │
│    (19)     │◄──│           (9,770)               │──▶│   (9,770)    │
└─────────────┘   └────────────────────────────────┘   └──────────────┘
       ▲                                                       │
       │          ┌────────────────────────────────┐           │
       └──────────│      bridge_movie_genres       │───────────┘
                  │          (22,094)               │
                  └────────────────────────────────┘
                                                           ┌──────────────┐
                  ┌────────────────────────────────┐       │  dim_people  │
                  │  fact_movie_cast  (150,044)    │──────▶│  (116,478)   │
                  │  fact_movie_crew   (63,632)    │──────▶│              │
                  │  fact_movie_reviews (13,073)   │       └──────────────┘
                  └────────────────────────────────┘
```

### Dimension Tables

| Table | Rows | Key Columns |
| --- | --- | --- |
| `dim_movies` | 9,770 | `movie_id`, `title`, `release_date`, `runtime_minutes`, `original_language`, `genre_list`, `is_adult` |
| `dim_genres` | 19 | `genre_id`, `genre_name`, `movie_count` |
| `dim_people` | 116,478 | `person_id`, `person_name` — deduplicated union of cast + crew |
| `dim_date` | 6,486 | `date_key`, `full_date`, `year`, `quarter`, `month`, `day_of_week`, `day_name`, `month_name` |

### Fact Tables

| Table | Rows | Key Columns |
| --- | --- | --- |
| `fact_movie_metrics` | 9,770 | `movie_id`, `budget`, `revenue`, `profit`, `roi_pct`, `vote_average`, `vote_count`, `popularity` |
| `fact_movie_cast` | 150,044 | `cast_id`, `movie_id`, `person_id`, `character`, `cast_order` |
| `fact_movie_crew` | 63,632 | `crew_id`, `movie_id`, `person_id`, `job`, `department` |
| `fact_movie_reviews` | 13,073 | `review_id`, `movie_id`, `author`, `author_rating`, `content`, `created_at` |

### Bridge Table

| Table | Rows | Key Columns |
| --- | --- | --- |
| `bridge_movie_genres` | 22,094 | `movie_id`, `genre_id`, `genre_name` — exploded from comma-separated genre strings |

> **Date parsing note:** The source uses `dd/MM/yy` format. A century pivot at year 26 handles ambiguity — `yy > 26` maps to 1900s (e.g., 77 → 1977), `yy <= 26` maps to 2000s (e.g., 03 → 2003). This covers Metropolis (1927) through present-day releases.

---

## Gold Layer — Datamarts

| Datamart | Rows | Description |
| --- | --- | --- |
| `gold_movie_summary` | 9,770 | One wide row per movie — title, financials, director, cast size, review stats. Answers most questions with a single `SELECT`. |
| `gold_genre_analytics` | 19 | Per-genre aggregates — avg budget, revenue, profit, total revenue, rating |
| `gold_director_analytics` | 1,563 | Directors with 2+ films — track record stats + top-rated movie via `max_by()` |
| `gold_yearly_trends` | 100 | Year-over-year industry evolution — volume, budgets, revenue, ratings, runtime |
| `gold_actor_analytics` | 10,535 | Actors with 3+ films — movie count, top-billed frequency, avg rating, total revenue |

---

## Business Cases & Key Insights

Seven strategic analyses that a studio, distributor, or streaming platform would use:

### Case 1: Budget Tier ROI — Where Should You Place Your Bets?
> Micro-budget films (&lt;$1M) show extreme ROI but only a **28% hit rate**. Blockbusters ($150M+) are safer bets at **87% hit rate** with $409M average profit.

### Case 2: Genre Investment Strategy — Revenue Per Dollar
> **Animation** leads at **4x revenue multiplier**, followed by Horror at 3.5x. TV Movies and Documentaries barely break even.

### Case 3: Director Bankability — Reliable Box Office Performers
> **James Cameron** averages **$1.1B per film** across 9 movies. David Yates ($820M avg) and Jon Watts ($788M avg) round out the top 3.

### Case 4: Runtime Sweet Spot — Optimal Film Length
> Films over 180 minutes average a **7.58 rating** and **$325M revenue**, but survivorship bias plays a role — only strong scripts get approved at that length.

### Case 5: Language & Market Opportunity — Non-English Cinema Rising
> Non-English film share is growing year over year, and international films consistently rate higher — a selection effect where only the best cross borders.

### Case 6: Hidden Gems — Streaming Acquisition Targets
> **25 films** under $5M budget with 7.5+ rating and 500+ votes — proven crowd-pleasers like *12 Angry Men*, *Whiplash*, and *City of God*.

### Case 7: Pandemic Impact & Recovery
> 2020 revenue **dropped 85%** and volume **dropped 28%** vs. 2019. Recovery favors fewer, bigger-budget tentpoles over pre-pandemic volume.

---

## Unity Catalog & Storage Layout

**Unity Catalog:**
```
employeedatacatalog
├── bronze_movie/       ← 5 raw ingested tables
├── silver_movie/       ← 9 star-schema tables (dims, facts, bridge)
└── gold_movie/         ← 5 denormalized datamarts
```

**ADLS directory structure:**
```
abfss://employee@dataanlysisazuredatalake.dfs.core.windows.net/
├── movies_data/        ← source CSVs
├── bronze/             ← Delta tables (raw + metadata)
├── silver/movie/       ← Delta tables (typed, deduplicated)
└── gold/movie/         ← Delta tables (pre-aggregated)
```

---

## Tech Stack

| Component | Technology |
| --- | --- |
| **Cloud** | Microsoft Azure |
| **Platform** | Databricks (Serverless) |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |
| **Table Format** | Delta Lake |
| **Catalog** | Unity Catalog |
| **Languages** | PySpark, Spark SQL |
| **Visualization** | Databricks AI/BI Dashboard |
| **Architecture** | Medallion (Bronze → Silver → Gold) |

---

## How to Run

Run the notebooks **in order** — each layer depends on the previous one:

```
1. bronze                          → Ingest CSVs, write 5 raw Delta tables
2. silver                          → Transform into 9-table star schema
3. Gold Layer Movie Datamarts      → Build 5 denormalized datamarts
4. Movie Industry Business Cases   → Run 7 strategic analyses
5. Open the Dashboard              → Explore interactive visualizations
```

> **Idempotent by design** — every notebook uses `overwrite` mode with `overwriteSchema` enabled. Safe to re-run at any time.

---

## Author

**Abhinav thupili** Built on Azure Databricks | May 2026
