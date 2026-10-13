# MongoDB Logic: Saskatchewan Public-Sector Job Pipeline

These diagrams show how each daily run stores job postings in MongoDB Atlas (`mongo_store.py`). GitHub renders the Mermaid diagrams automatically.

1. [Data model](#1-data-model): the four collections and how they link
2. [One run, step by step](#2-one-run-step-by-step): what the pipeline sends to MongoDB and when
3. [Upsert and closure logic](#3-upsert-and-closure-logic): how `sync()` decides what happens to each posting
4. [Posting lifecycle](#4-posting-lifecycle): the states a posting moves through

MongoDB is switched on by the `MONGODB_URI` environment variable, which is a GitHub Actions secret in the scheduled runs. Full runs write to the **`sk_jobs`** database. Test runs (`--max-jobs`) write to **`sk_jobs_test`**, so they never touch the real data.

---

## 1. Data model

`Record_ID` (`<SOURCE>_<Job_ID>`, e.g. `SGI_R7076`) is the key that links everything. It's stable from run to run, so a posting is updated in place rather than duplicated.

```mermaid
erDiagram
    POSTINGS ||--o| RAW_POSTINGS : "same _id"
    POSTINGS ||--o{ POSTING_CHANGES : "record_id"

    POSTINGS {
        string _id PK "Record_ID e.g. SGI_R7076"
        string Source "employer name"
        string Job_Title
        string Organization
        string City
        number Salary_Min
        number Salary_Max
        string Salary_Unit
        string Employment_Type_Standardized
        string Closing_Date
        string other_fields "all 29 master-schema columns"
        string status "open or closed"
        string first_seen "set once, on insert"
        string last_seen "updated every run it is listed"
        string closed_date "set when it closes"
        string closed_reason "No longer listed / Listed as closed by site"
        string content_hash "SHA-1 of the posting fields"
        datetime created_at
        datetime updated_at
    }
    RAW_POSTINGS {
        string _id PK "Record_ID"
        string source
        string scraped_date
        object data "raw scrape, before cleaning"
    }
    POSTING_CHANGES {
        objectId _id PK
        string record_id FK "Record_ID"
        string Source
        string changed_on "run date"
        object changes "field: old and new value"
    }
    RUNS {
        objectId _id PK
        string run_date
        datetime started_at
        number duration_s
        bool test_run
        array sources_ok
        array sources_failed
        object per_source "open and closed counts"
        number new
        number updated
        number unchanged
        number reopened
        number closed_missing
        number closed_by_site
        array closure_skipped_sources
    }
```

| Collection | One document per | Purpose | Indexes |
|---|---|---|---|
| `postings` | posting | The clean, current state of every posting ever seen, open or closed | `status + Source`, `first_seen`, `last_seen` |
| `raw_postings` | posting | The latest raw scrape, so cleaning can be re-run without re-scraping | `_id` |
| `posting_changes` | change | Salary, title, closing-date, location, openings and employment-type edits on open postings | `record_id + changed_on` |
| `runs` | pipeline run | Run summary for monitoring: counts, failed sites, duration | `started_at` |

---

## 2. One run, step by step

```mermaid
sequenceDiagram
    autonumber
    participant GA as GitHub Actions
    participant P as run_pipeline()
    participant S as Career sites
    participant M as MongoStore
    participant DB as MongoDB Atlas

    GA->>P: Daily trigger, MONGODB_URI from secrets
    P->>M: MongoStore.from_env(test_run)
    M->>DB: Connect + ping
    alt test run (max_jobs set)
        M->>DB: use sk_jobs_test
    else full run
        M->>DB: use sk_jobs
    end
    M->>DB: Create indexes if missing

    loop For each source
        P->>S: scrape()
        alt site failed
            S-->>P: error
            P->>P: add to sources_failed, continue
        else site OK
            S-->>P: raw postings
            P->>P: enrich()
            P->>M: save_raw(source, records)
            M->>DB: bulk upsert raw_postings by _id
            P->>P: finalize() + split open / closed
        end
    end

    P->>M: sync(open, closed, sources_ok, close_missing = full run)
    M->>DB: find existing postings by _id
    M->>DB: bulk upsert postings
    M->>DB: insert posting_changes
    M->>DB: close missing postings per source
    M-->>P: stats
    P->>M: log_run(summary + stats)
    M->>DB: insert into runs
    P->>M: close()
```

---

## 3. Upsert and closure logic

```mermaid
flowchart TD
    START(["sync(): today's open and site-closed postings"]) --> FETCH["Load existing docs for today's Record_IDs<br/>status, content_hash, tracked fields"]
    FETCH --> EACH["For each open posting"]
    EACH --> HASH["Drop Collection_Date<br/>content_hash = SHA-1 of the fields"]
    HASH --> EX{"Exists in<br/>postings?"}
    EX -- No --> NEW["new += 1"]
    EX -- "Yes, status closed" --> RE["reopened += 1"]
    EX -- "Yes, status open" --> H{"Hash<br/>changed?"}
    H -- No --> UN["unchanged += 1"]
    H -- Yes --> UP["updated += 1"]
    UP --> TF{"Any tracked field<br/>changed?"}
    TF -- Yes --> CH["Queue posting_changes doc<br/>field: old → new"]
    TF -- No --> OP
    CH --> OP
    NEW --> OP
    RE --> OP
    UN --> OP
    OP["UpdateOne upsert by _id<br/>$set fields, status = open, last_seen = today, content_hash<br/>$setOnInsert first_seen = today<br/>$unset closed_date, closed_reason"]

    OP --> SC["For each posting the site lists as closed"]
    SC --> AC{"Already closed<br/>in the database?"}
    AC -- Yes --> KEEPC["Skip, keeps its original closed_date"]
    AC -- No --> CL1["UpdateOne upsert by _id<br/>status = closed, closed_date = today<br/>reason: Listed as closed by site"]

    CL1 --> BW[("bulk_write all upserts<br/>insert_many posting_changes")]
    KEEPC --> BW
    BW --> FR{"Full run?<br/>close_missing"}
    FR -- "No, test run" --> STATS
    FR -- Yes --> PS["For each source that crawled successfully"]
    PS --> G{"At least 10 open in DB<br/>and listed under half today?"}
    G -- "Yes: partial crawl" --> SKIP["Record in closure_skipped_sources<br/>close nothing for this source"]
    G -- No --> CM["update_many: open postings of this source<br/>not in today's list → status = closed<br/>closed_date = today, reason: No longer listed on site"]
    SKIP --> STATS
    CM --> STATS(["Return stats → logged to runs"])
```

**Why the run is safe to repeat (idempotent):** every write is an upsert keyed on `Record_ID`. Running twice on the same day gives the same database: `first_seen` is only set on insert, an unchanged posting keeps its hash, so no duplicate change records are logged, and closures already recorded keep their date.

---

## 4. Posting lifecycle

```mermaid
flowchart TD
    NEW(("New<br/>posting")) -- "T1 first seen on a site" --> OPEN(["OPEN"])
    NEW -- "T2 first seen in an archive" --> CLOSED(["CLOSED"])
    OPEN -- "T4 missing from a healthy crawl<br/>T5 site marks it closed" --> CLOSED
    CLOSED -- "T6 re-posted" --> OPEN
    OPEN -.- N1["T3 stays OPEN:<br/>listed again, or its site<br/>failed / looked partial"]
    CLOSED -.- N2["T7 stays CLOSED:<br/>still in the archive,<br/>closed_date kept"]
```

| | Transition | What triggers it | What gets written |
|---|---|---|---|
| T1 | New → Open | A posting appears on a site for the first time | `first_seen = last_seen = today`, `status = open` |
| T2 | New → Closed | First seen in a site's archive (SaskWater's past PDFs) | `status = closed`, reason "Listed as closed by site" |
| T3 | Open → Open | Listed again; or its site failed or looked like a partial crawl | `last_seen = today` when listed; field edits logged to `posting_changes` |
| T4 | Open → Closed | Missing from a full, healthy crawl of its site | `closed_date = today`, reason "No longer listed on site" |
| T5 | Open → Closed | The site itself marks it closed | `closed_date = today`, reason "Listed as closed by site" |
| T6 | Closed → Open | The same posting is listed again (re-posted) | `status = open`, `closed_date` and `closed_reason` removed, counted as `reopened` |
| T7 | Closed → Closed | Still in the site's archive | Nothing, so the original `closed_date` is kept |

**For analysis:** `last_seen − first_seen` is how long a posting stayed up. Closed postings (`status = closed`) are the training set for the AI/ML models, and `posting_changes` shows how salaries and deadlines move while a posting is open.
