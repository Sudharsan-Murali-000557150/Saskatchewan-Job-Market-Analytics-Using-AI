# Scraper Logic: Saskatchewan Public-Sector Job Pipeline

These flowcharts show how `sk_jobs_scraper.py` collects job postings from nine public-sector career sites and turns them into one dataset. GitHub renders the Mermaid diagrams automatically.

1. [End-to-end pipeline](#1-end-to-end-pipeline): one daily run, from trigger to storage
2. [Per-source scraping](#2-per-source-scraping): how each platform is crawled
3. [Cleaning and standardization](#3-cleaning-and-standardization): how a raw posting becomes a master-schema row
4. [History and closure tracking](#4-history-and-closure-tracking): how closed postings are detected and kept

---

## 1. End-to-end pipeline

```mermaid
flowchart TD
    T(["GitHub Actions cron<br/>daily 5:47 AM SK time<br/>or manual run"]) --> S["Start run_pipeline()<br/>sources, max_jobs, workers, delay"]
    S --> M{"MONGODB_URI<br/>secret set?"}
    M -- Yes --> MT{"Test run?<br/>max_jobs set"}
    MT -- Yes --> DBT[("Connect to sk_jobs_test")]
    MT -- No --> DB[("Connect to sk_jobs")]
    M -- No --> CSVONLY["CSV output only"]
    DBT --> LOOP
    DB --> LOOP
    CSVONLY --> LOOP

    LOOP["For each selected source"] --> SCR["scraper.scrape()<br/>discover, list, fetch details"]
    SCR --> OK{"Crawl<br/>succeeded?"}
    OK -- No --> FAIL["Log error, add to failed sources<br/>other sources keep running"]
    OK -- Yes --> RAW["Save raw CSV<br/>raw/source_raw_date.csv"]
    RAW --> ENR["scraper.enrich()<br/>source-specific parsing"]
    ENR --> RAWDB{"MongoDB<br/>connected?"}
    RAWDB -- Yes --> RAWM[("Upsert raw_postings")]
    RAWDB -- No --> FIN
    RAWM --> FIN["finalize()<br/>map to 29-column master schema"]
    FIN --> CLEAN["Save clean CSV<br/>clean/source_clean_date.csv"]
    CLEAN --> SPLIT["Split rows: Open vs Closed by site"]
    SPLIT --> NEXT{"More<br/>sources?"}
    FAIL --> NEXT
    NEXT -- Yes --> LOOP
    NEXT -- No --> ANY{"At least one<br/>source succeeded?"}
    ANY -- No --> ERR(["Raise error: run fails,<br/>GitHub emails the owner"])
    ANY -- Yes --> MASTER["Concatenate open rows<br/>save master CSV + print data-quality summary"]

    MASTER --> HIST{"Where is history kept?"}
    HIST -- "MongoDB" --> SYNC[("store.sync()<br/>upsert postings, record closures<br/>store.log_run()")]
    HIST -- "No MongoDB, full run" --> FILES["update_history()<br/>tracker CSV + past_postings_master.csv"]
    HIST -- "No MongoDB, test run" --> SKIP["Skip history"]
    SYNC --> ART["Upload data folder as<br/>GitHub artifact, 14 days"]
    FILES --> COMMIT["Commit data folder back to repo<br/>CSV fallback only"]
    SKIP --> COMMIT
    COMMIT --> ART
    ART --> E(["End"])
```

---

## 2. Per-source scraping

Every scraper exposes the same interface: `scrape()` returns raw records and `enrich()` parses them, so the pipeline doesn't depend on any one platform. Only each employer's public careers page is configured. Every other URL is discovered at run time.

```mermaid
flowchart TD
    START(["scraper.scrape()"]) --> PLAT{"Platform"}

    PLAT -- "Oracle HCM<br/>Gov of SK, SHA" --> O1["Read site number from careers URL<br/>find API host in page HTML"]
    O1 --> O2["Page through REST API<br/>recruitingCEJobRequisitions<br/>25 per page until reported total"]
    O2 --> O3["Fetch each requisition's details<br/>in parallel"]

    PLAT -- "SAP SuccessFactors<br/>SaskPower, SaskTel" --> F1["Page through /search/ results<br/>sorted newest first"]
    F1 --> F2["Read each result row:<br/>title, link, date, department, location"]
    F2 --> F3["Fetch each job page in parallel<br/>description + schema.org metadata"]

    PLAT -- "Workday<br/>SaskEnergy, SGI" --> W1["Find myworkdayjobs.com link<br/>on careers page or its JSON"]
    W1 --> W2["POST to Workday jobs API<br/>20 per page until total"]
    W2 --> W3["Tag category and worker type<br/>one filtered search per facet value"]
    W3 --> W4["Fetch each posting's JSON in parallel"]

    PLAT -- "ApplyToJob<br/>SLGA" --> A1["Find applytojob.com board<br/>on careers page"]
    A1 --> A2["Read job list from /apply"]
    A2 --> A3["Fetch each job page in parallel<br/>JSON-LD: dates, pay, location + description"]

    PLAT -- "Careers page + PDFs<br/>SaskWater, LGS" --> P1["Collect posting PDF links<br/>from the careers page"]
    P1 --> P2{"SaskWater?"}
    P2 -- Yes --> P3["Also list past competition PDFs<br/>from WordPress media library<br/>newest copy per competition #"]
    P2 -- No --> P4
    P3 --> P4["Download each PDF in parallel<br/>extract text with pypdf"]

    O3 --> RET
    F3 --> RET
    W4 --> RET
    A3 --> RET
    P4 --> RET(["Return list of raw records"])

    subgraph HTTP["Shared HTTP layer, used by every scraper"]
        H1["requests.Session with browser User-Agent"]
        H2["Retry 429 / 5xx with exponential backoff"]
        H3["Polite delay after every request"]
        H4["Thread pool for detail requests,<br/>failed pages logged and skipped"]
    end
```

---

## 3. Cleaning and standardization

```mermaid
flowchart TD
    R(["Raw record"]) --> E["enrich(): source-specific parsing"]
    E --> E1["Split description into sections<br/>headings → Responsibilities, Qualifications, ..."]
    E1 --> E2["Pull labelled facts<br/>salary, closing date, employment type,<br/>union / scope, openings, location"]
    E2 --> E3["Drop repeated boilerplate<br/>benefits, diversity, how-to-apply"]
    E3 --> F["finalize(): shared by all sources"]

    F --> F1["Drop duplicate Job_IDs<br/>Record_ID = SOURCEKEY_JobID"]
    F1 --> F2["Location → City / Province / Country<br/>fill SK, Canada; blank non-city values"]
    F2 --> F3["Salary text → Salary_Min / Salary_Max"]
    F3 --> U{"Unit stated in salary text<br/>or hours-of-work code?"}
    U -- Yes --> U1["Hourly / Bi-weekly / Monthly / Annual"]
    U -- No --> U2{"Max value"}
    U2 -- "≤ 200" --> U3["Hourly (inferred)"]
    U2 -- "≥ 40,000" --> U4["Annual (inferred)"]
    U2 -- "in between" --> U5["Unspecified"]
    U1 --> F4
    U3 --> F4
    U4 --> F4
    U5 --> F4["Employment type → standard labels<br/>e.g. Full-time Permanent"]
    F4 --> F5["Dates → Saskatchewan time, YYYY-MM-DD"]
    F5 --> F6["Order columns: 29 master columns first,<br/>source-specific extras after"]
    F6 --> OUT(["Clean row"])
```

---

## 4. History and closure tracking

A job board only shows what's open today. To keep closed postings for analysis and model training, each run compares today's postings with what was open before.

```mermaid
flowchart TD
    subgraph A["A. Every posting in today's crawl"]
        S(["Posting found today"]) --> Q{"Posting<br/>status"}
        Q -- "Listed as closed by site<br/>e.g. SaskWater archive" --> CS["Mark closed<br/>reason: Listed as closed by site"]
        Q -- "Open on site" --> SEEN{"Seen before?"}
        SEEN -- No --> NEW["New posting<br/>first_seen = today"]
        SEEN -- "Yes, was open" --> CHG{"Content<br/>changed?"}
        CHG -- Yes --> LOGC["Updated: MongoDB logs salary / title /<br/>closing-date changes to posting_changes"]
        CHG -- No --> SAME["Unchanged"]
        SEEN -- "Yes, was closed" --> REOPEN["Re-opened: back to open"]
        NEW --> LS["last_seen = today, status = open"]
        LOGC --> LS
        SAME --> LS
        REOPEN --> LS
    end

    subgraph B["B. Every posting open before but missing today"]
        MISS(["Posting not in today's crawl"]) --> SRC{"Did that source<br/>crawl successfully?"}
        SRC -- No --> KEEP["Leave as open<br/>a failed site never closes postings"]
        SRC -- Yes --> STORE{"History store"}
        STORE -- "CSV files" --> CLOSE["Mark closed<br/>reason: No longer listed on site<br/>closed_date = today"]
        STORE -- "MongoDB" --> GUARD{"Listed at least half<br/>of its open postings?"}
        GUARD -- "No: looks like a partial crawl" --> KEEP
        GUARD -- Yes --> CLOSE
    end

    CS --> PAST[("Past postings<br/>MongoDB status = closed<br/>or past_postings_master.csv")]
    CLOSE --> PAST
    LS --> OPEN[("Open postings<br/>MongoDB status = open<br/>or open_postings_tracker.csv")]
    KEEP --> OPEN
```
