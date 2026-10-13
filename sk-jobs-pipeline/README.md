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
| SGI / SGI CANADA | Workday |
| Saskatchewan Liquor and Gaming Authority | ApplyToJob (JazzHR) |
| Lotteries and Gaming Saskatchewan | Careers page + PDF postings |

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

## Roadmap
- [ ] Write postings to MongoDB Atlas (upserts keyed on `Record_ID`)
