# NLP Logic: Text Cleaning and Skill Extraction

These diagrams show how the NLP notebook (`job_postings_nlp_1_skills.ipynb`) turns the free text of each posting into structured features for analysis and modelling. GitHub renders the Mermaid diagrams automatically.

1. [Notebook pipeline](#1-notebook-pipeline): from the scraper's files on Google Drive to the feature CSVs
2. [Text cleaning](#2-text-cleaning): sections, boilerplate and the Government skills framework
3. [Feature extraction](#3-feature-extraction): skills, certifications, education and experience
4. [Where this leads](#4-where-this-leads): the NLP stages planned next

Extraction is **rule-based**: a skill taxonomy plus regex patterns. Every result can be traced back to the text that produced it, and no labelled training data is needed.

---

## 1. Notebook pipeline

```mermaid
flowchart TD
    D[("Google Drive<br/>scraper OUT_DIR")] --> F["find_master_files()<br/>newest saskatchewan_public_sector_jobs_master_date.csv<br/>+ past_postings_master.csv if present"]
    F --> L["load_postings()<br/>Posting_Status = Open or Closed by file"]
    L --> DUP{"Record_ID in<br/>both files?"}
    DUP -- Yes --> KEEP["Keep the current, Open row"]
    DUP -- No --> C
    KEEP --> C["clean_postings()<br/>see diagram 2"]
    C --> X["extract_all()<br/>see diagram 3"]
    X --> LONG["skills_long()<br/>one row per posting × skill"]
    X --> COV["Coverage table per employer<br/>% with education, experience, certification"]
    LONG --> VIZ["Charts<br/>top skills overall and excluding SHA,<br/>top skills per employer, certifications,<br/>education by employer, experience level"]
    COV --> VIZ
    VIZ --> SPOT["Spot-check a random posting<br/>text next to extracted fields"]
    SPOT --> SAVE[("Save to Drive: DATA_DIR/nlp/<br/>job_postings_nlp_features_date.csv<br/>job_skills_long_date.csv")]
    SAVE --> NEXT(["Input for topic modelling,<br/>salary prediction and job matching"])
```

---

## 2. Text cleaning

```mermaid
flowchart TD
    P(["One posting"]) --> N["normalize_text()<br/>unicode NFKC, straight quotes, bullets → line breaks,<br/>remove URLs, emails, phone numbers, #LI- tags"]
    N --> SEC{"Has separate Responsibilities /<br/>Qualifications columns?"}
    SEC -- "No, SHA style" --> SHA["Split the description on SHA's inline labels<br/>Job Description, Experience, Education, Licenses,<br/>Competencies, Knowledge and Abilities, Other Information"]
    SHA --> SHAOUT["Resp = Job Description<br/>Qual = Experience + Education + Licenses + Competencies + Knowledge<br/>Other = Other Information"]
    SEC -- Yes --> DEDUP["Remove the parts of the description that<br/>repeat the Responsibilities / Qualifications text"]
    DEDUP --> OVL{"Qualifications start<br/>with the duties text?"}
    OVL -- Yes --> CUT["Remove the duplicated duties"]
    OVL -- No --> GOV
    CUT --> GOV
    SHAOUT --> GOV{"Government 'Job Skills:'<br/>framework block?"}
    GOV -- Yes --> LEVELS["Move it to Gov_Skill_Levels<br/>e.g. Communication: C; Data Literacy: B<br/>and remove it from the text"]
    GOV -- No --> BP
    LEVELS --> BP["Boilerplate check, per employer"]

    subgraph BOIL["Boilerplate rule, computed across all postings"]
        B1["Split every posting into sentences ≥ 25 characters"]
        B2["Count how many of that employer's postings contain each sentence"]
        B3{"In at least max(3, 30% of<br/>the employer's postings)?"}
        B1 --> B2 --> B3
        B3 -- Yes --> B4["Boilerplate: benefits blurbs, diversity statements,<br/>standard Job Capabilities list"]
        B3 -- No --> B5["Keep: posting-specific text"]
    end

    BP --> BOIL
    BOIL --> OUT["Clean_Text = Job_Title + Resp + Qual + Other<br/>+ Word_Count"]
    OUT --> DONE(["Cleaned posting"])
```

---

## 3. Feature extraction

```mermaid
flowchart TD
    IN(["Cleaned posting"]) --> SPLIT["Run four extractors"]

    SPLIT --> SK["Skills<br/>~120 regex patterns in 6 categories:<br/>Software & Data, Business & Management, Healthcare,<br/>Trades & Technical, Soft skills, Languages"]
    SK --> SKO["Skills + Skills_category lists + Skill_Count<br/>match anywhere in Clean_Text"]

    SPLIT --> CR["Certifications and requirements<br/>separate pattern lists"]
    CR --> CRO["Certifications: RN, LPN, P.Eng., Red Seal, CPR, driver's licence, ...<br/>Requirements: criminal record check, drug test, travel, shift work, ..."]

    SPLIT --> ED["Education: qualifications text only"]
    ED --> E1["Remove non-education certificates<br/>First Aid, CPR, driver's, Food Safe, WHMIS, ..."]
    E1 --> E2["For each sentence, join 'OR …' sentences to the one before,<br/>then split into clauses"]
    E2 --> E3{"Clause says asset /<br/>preferred / ideally?"}
    E3 -- Yes --> E4["Level goes to Education_Preferred"]
    E3 -- No --> E5{"Levels joined<br/>by 'or'?"}
    E5 -- "Yes: alternatives" --> E6["Lowest level in the sentence<br/>diploma or degree → Diploma"]
    E5 -- "No" --> E7["Highest level in the sentence<br/>Grade 12 and CCA certificate → Certificate"]
    E6 --> E8["Education_Required = highest across sentences<br/>+ Education_Field, Equivalency_Accepted"]
    E7 --> E8

    SPLIT --> XP["Experience"]
    XP --> X1["Turn 'Twenty-four (24)' into '24'<br/>split sentences into clauses"]
    X1 --> X2["Find N years / N months … experience<br/>months ÷ 12"]
    X2 --> X3{"Clause says asset /<br/>preferred?"}
    X3 -- Yes --> X4["Experience_Years_Preferred"]
    X3 -- No --> X5["Experience_Years_Min = largest minimum found"]
    X5 --> X6{"Nothing found but<br/>'no previous experience' /<br/>'new graduates'?"}
    X6 -- Yes --> X7["Experience_Years_Min = 0"]
    X6 -- No --> LV
    X7 --> LV
    X4 --> LV{"Title keyword?"}
    LV -- "Director, VP, Chief" --> L1["Executive / Director"]
    LV -- "Manager, Supervisor" --> L2["Manager / Supervisor"]
    LV -- "Student, Co-op, Trainee" --> L3["Student / Trainee"]
    LV -- "Senior, Lead" --> L4["Senior (by title)"]
    LV -- "None: use years" --> L5["Entry 0–1 · Intermediate 2–4 ·<br/>Senior 5+ · Not stated"]

    SKO --> OUT
    CRO --> OUT
    E8 --> OUT
    E4 --> OUT
    L1 --> OUT
    L2 --> OUT
    L3 --> OUT
    L4 --> OUT
    L5 --> OUT(["Feature row: 20+ new columns<br/>per posting"])
```

---

## 4. Where this leads

The features from this notebook are the input for the next NLP stages. Dashed boxes are planned, not built yet.

```mermaid
flowchart LR
    S[("Scraper<br/>MongoDB / Drive")] --> N1["NLP 1: cleaning +<br/>skill extraction<br/>(built)"]
    N1 --> F[("Feature CSVs<br/>Clean_Text, Skills,<br/>Education, Experience")]
    F -.-> N2["NLP 2: topic modelling /<br/>clustering into job families"]
    F -.-> N3["NLP 3: salary prediction<br/>from text + features"]
    F -.-> N4["NLP 4: semantic search /<br/>resume-to-job matching"]
    N2 -.-> R["Analytics dashboard<br/>and capstone report"]
    N3 -.-> R
    N4 -.-> R

    classDef planned stroke-dasharray: 5 5
    class N2,N3,N4,R planned
```
