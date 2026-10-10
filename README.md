# Saskatchewan Public-Sector Job Market Pipeline

PROJ 613 capstone. Scrapes job postings from six Saskatchewan public-sector career sites every morning and keeps a running history of open and closed postings.

| Source | Platform |
|---|---|
| Government of Saskatchewan | Oracle HCM |
| Saskatchewan Health Authority | Oracle HCM |
| SaskPower | SAP SuccessFactors |
| SaskEnergy | Workday |
| SaskWater | WordPress + PDF postings |
| SaskTel | SAP SuccessFactors |

## How it runs
A GitHub Actions workflow (`.github/workflows/daily-scrape.yml`) runs `sk_jobs_scraper.py` daily at 5:47 AM Saskatchewan time, then commits the updated CSVs in `data/` back to the repo.

## Run locally
```bash
pip install -r requirements.txt
python sk_jobs_scraper.py --max-jobs 20      # quick test
python sk_jobs_scraper.py                    # full crawl
```

## Outputs (`data/`)
- `raw/` and `clean/` per-source CSVs for each run
- `saskatchewan_public_sector_jobs_master_<date>.csv` current open postings
- `history/open_postings_tracker.csv` open postings with first/last seen dates
- `past_postings_master.csv` every closed posting collected so far

## MongoDB
When the `MONGODB_URI` environment variable is set (a GitHub Actions secret in the scheduled
runs), every run also writes to MongoDB (`mongo_store.py`), and the database holds the history:

| Collection | Contents |
|---|---|
| `postings` | One document per posting (`_id` = `Record_ID`), with `first_seen`, `last_seen`, `status` (open/closed), `closed_date`, `closed_reason` |
| `raw_postings` | Latest raw scrape of each posting, before cleaning |
| `posting_changes` | Salary, closing date, title, etc. changes on open postings |
| `runs` | One summary per run: counts, failed sources, duration |

Test runs (`--max-jobs`) write to a separate `sk_jobs_test` database. Closures are skipped for
a source when a run lists under half of its currently open postings (likely a partial crawl).
