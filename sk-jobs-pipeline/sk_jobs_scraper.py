#!/usr/bin/env python3
"""Saskatchewan public-sector job scraper (script version of sk_jobs_scraper.ipynb).

Run locally:      python sk_jobs_scraper.py --out-dir data
Quick test:       python sk_jobs_scraper.py --max-jobs 20
Scheduled runs:   .github/workflows/daily-scrape.yml
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("sk_jobs")

GOS_CAREER_PAGE = "https://careers.saskatchewan.ca/en/sites/CX_1"
SHA_CAREER_PAGE = "https://emqk.fa.ca3.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001"
SASKPOWER_CAREER_SITE = "https://jobs.saskpower.com"
SASKENERGY_CAREER_PAGE = "https://www.saskenergy.com/careers"
SASKTEL_CAREER_SITE = "https://jobs.sasktel.com"
SASKWATER_CAREER_PAGE = "https://www.saskwater.com/why-work-at-saskwater/"

LOCAL_TZ = "America/Regina"
REQUEST_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)

# Columns every source is mapped onto in the master dataset.
MASTER_COLUMNS = [
    "Record_ID", "Source", "Job_ID", "Job_Title", "Organization", "Category",
    "Job_Function", "City", "Province", "Country", "Location_Raw",
    "Employment_Type", "Employment_Type_Standardized", "FTE", "Union",
    "Hours_of_Work", "Salary_Range", "Salary_Min", "Salary_Max", "Salary_Unit",
    "Salary_Status", "Number_of_Openings", "Posting_Date", "Closing_Date",
    "Description", "Qualifications", "Responsibilities", "Job_URL",
    "Collection_Date",
]

class Http:
    """requests.Session with retries/backoff and a polite delay after each call."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self.session = requests.Session()
        retry = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=16)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers["User-Agent"] = USER_AGENT

    def get(self, url: str, params: dict | None = None,
            accept: str = "text/html,application/xhtml+xml") -> requests.Response:
        resp = self.session.get(url, params=params, headers={"Accept": accept},
                                timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        if self.delay:
            time.sleep(self.delay)
        return resp

    def json(self, url: str, params: dict | None = None) -> dict:
        resp = self.get(url, params=params, accept="application/json")
        ctype = resp.headers.get("Content-Type", "")
        if "json" not in ctype:
            raise ValueError(f"Expected JSON but got '{ctype}' from {resp.url}")
        return resp.json()

    def post_json(self, url: str, payload: dict) -> dict:
        resp = self.session.post(url, json=payload, headers={"Accept": "application/json"},
                                 timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        if self.delay:
            time.sleep(self.delay)
        return resp.json()

    def soup(self, url: str, params: dict | None = None) -> BeautifulSoup:
        return BeautifulSoup(self.get(url, params=params).text, "html.parser")


def fetch_parallel(items: list, fn: Callable, workers: int, label: str) -> list:
    """Run fn over items with a small thread pool; keeps input order, drops failures."""
    results: list = [None] * len(items)
    failed = 0
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
        for n, fut in enumerate(as_completed(futures), start=1):
            i = futures[fut]
            try:
                results[i] = fut.result()
            except Exception as exc:  # noqa: BLE001 - log and keep crawling
                log.warning("%s: detail %d failed: %s", label, i, exc)
            if results[i] is None:
                failed += 1
            if n % 100 == 0 or n == len(items):
                log.info("%s: %d/%d details fetched (%d failed)", label, n, len(items), failed)
    return [r for r in results if r is not None]

def is_blank(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and not value.strip()


def squash(text: str | None) -> str | None:
    """Collapse all whitespace (incl. non-breaking/zero-width) to single spaces."""
    if is_blank(text):
        return None
    text = str(text).replace("\xa0", " ").replace("\u200b", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def clean_html(html: str | None) -> str | None:
    if is_blank(html):
        return None
    return squash(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))


def to_number(value) -> float | None:
    if is_blank(value):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def to_local_date(value) -> str | None:
    """Normalise any date/timestamp to an ISO date in Saskatchewan time.

    Plain dates are kept as-is; timestamps (e.g. Oracle's 05:59 UTC closing
    times) are converted to America/Regina first, so a posting that closes
    '2026-10-17T05:59Z' correctly becomes 2026-10-16 (11:59 pm local).
    """
    if is_blank(value):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert(LOCAL_TZ)
    return ts.date().isoformat()


# ---- salary -----------------------------------------------------------------
def parse_salary_range(text: str | None) -> tuple[float | None, float | None]:
    """'Pay Band 16 $36.160 to $38.720 (3 step range)' -> (36.16, 38.72).

    If the text contains '$', only dollar amounts are used, so pay-band
    numbers and '(3 step range)' are ignored. Otherwise all numbers are used
    (Government of Saskatchewan writes '22.295 - 27.939').
    """
    if is_blank(text):
        return None, None
    text = str(text)
    pattern = r"\$\s*(\d[\d,]*(?:\.\d+)?)" if "$" in text else r"(\d[\d,]*(?:\.\d+)?)"
    nums = [n for n in (to_number(x) for x in re.findall(pattern, text)) if n]
    if not nums:
        return None, None
    low, high = nums[0], nums[1] if len(nums) > 1 else nums[0]
    return min(low, high), max(low, high)


_UNIT_PATTERNS = [
    (r"\b(per\s+hour|/\s*h(ou)?r|hourly)\b", "Hourly"),
    (r"\bbi-?weekly\b", "Bi-weekly"),
    (r"\b(per\s+month|monthly)\b", "Monthly"),
    (r"\b(per\s+year|per\s+annum|annual(ly)?|/\s*year)\b", "Annual"),
]


def salary_unit(text: str | None, high: float | None, hint: str | None = None) -> str | None:
    """Unit stated in the salary text; else a hint (e.g. hours-of-work code) if the
    magnitude agrees; else a clearly-labelled inference from magnitude."""
    if is_blank(high):
        return None
    for source in (text, hint):
        if is_blank(source):
            continue
        for pattern, unit in _UNIT_PATTERNS:
            if re.search(pattern, str(source), re.I):
                # A hint like 'M - Monthly Out of Scope' is ignored for hourly-sized values.
                if source is hint and unit != "Hourly" and high <= 200:
                    break
                return unit
    if high <= 200:
        return "Hourly (inferred)"
    if high >= 40000:
        return "Annual (inferred)"
    return "Unspecified"


def salary_status(text: str | None, low: float | None) -> str:
    if low is not None:
        return "Numeric"
    if is_blank(text):
        return "Not Provided"
    t = str(text).lower()
    if "terms and conditions" in t:
        return "Per Terms and Conditions"
    if any(k in t for k in ("commensurate", "competitive", "negotiable")):
        return "Non-numeric Statement"
    return "Other Non-numeric"


# ---- employment type --------------------------------------------------------
def standardize_employment_type(value: str | None) -> str | None:
    """Map each source's wording onto one shared vocabulary."""
    if is_blank(value):
        return None
    t = str(value).lower()
    full = bool(re.search(r"full[\s-]?time", t))
    part = bool(re.search(r"part[\s-]?time", t))
    if "student" in t:
        return "Student"
    if "casual" in t:
        return "Casual"
    if "relief" in t:
        return "Relief"
    if "seasonal" in t or "labour service" in t:
        return "Seasonal"
    if "contract" in t:
        return "Contract"
    if any(k in t for k in ("temporary", "term", "non-perm", "expression of interest")):
        return "Full-time Temporary" if full else "Part-time Temporary" if part else "Temporary"
    if "permanent" in t or "regular" in t:
        return "Full-time Permanent" if full else "Part-time Permanent" if part else "Permanent"
    if full:
        return "Full-time"
    if part:
        return "Part-time"
    return "Other"


def fte_numeric(value) -> float | None:
    """Leading number in [0, 1]: '1 Shift info: Days' -> 1.0; 'Full Time' -> None."""
    if is_blank(value):
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)?)", str(value))
    if m and 0 <= float(m.group(1)) <= 1:
        return float(m.group(1))
    return None


# ---- location ---------------------------------------------------------------
_PROVINCES = {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}


def normalize_city(city: str | None) -> str | None:
    if is_blank(city):
        return None
    city = str(city).replace("\u2019", "'").strip(" ,.")
    if city.upper() == "MULTIPLE":
        return "Multiple Locations"
    if city.isupper():
        city = city.title()
    return city or None


def split_location(location: str | None) -> tuple[str | None, str | None, str | None]:
    """'Regina, SK, Canada' -> ('Regina', 'SK', 'Canada'); 'SK, Canada' -> (None, 'SK', 'Canada')."""
    if is_blank(location):
        return None, None, None
    parts = [p.strip() for p in str(location).split(",") if p.strip()]
    city = prov = country = None
    if len(parts) >= 3:
        city, prov, country = ", ".join(parts[:-2]), parts[-2], parts[-1]
    elif len(parts) == 2:
        if parts[0].upper() in _PROVINCES:
            prov, country = parts
        else:
            city, prov = parts
    elif parts:
        if parts[0].upper() in _PROVINCES:
            prov = parts[0]
        else:
            city = parts[0]
    if country and country.upper() in {"CA", "CAN"}:
        country = "Canada"
    return normalize_city(city), prov, country


def clean_city_text(text: str | None) -> str | None:
    """Tidy free-text city fields ('Fort Qu'Appelle, SK.', 'Meadow Lake Saskatchewan')."""
    if is_blank(text):
        return None
    text = str(text).replace("\u2019", "'")
    text = re.sub(r",?\s*(SK\.?|Saskatchewan)\s*$", "", text, flags=re.I).strip(" ,.")
    if re.search(r"\b(tbd|to be determined|provincial|anywhere)\b", text, re.I):
        return None
    return normalize_city(text)


def split_org_code(value: str | None) -> tuple[str | None, str | None]:
    """'036 Social Services' -> ('036', 'Social Services')."""
    if is_blank(value):
        return None, None
    m = re.match(r"^\s*(\d+)\s+(.*)$", str(value))
    return (m.group(1), m.group(2).strip()) if m else (None, str(value).strip())

@dataclass
class OracleSite:
    key: str                     # Record_ID prefix
    name: str                    # Source label
    career_page: str             # public career-site URL (contains /sites/CX_...)
    enrich: Callable[[dict], dict]
    fallback_api_host: str | None = None


class OracleHCMScraper:
    LIST_PATH = "/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    DETAIL_PATH = "/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"

    def __init__(self, site: OracleSite, http: Http, workers: int = 4, page_size: int = 25):
        self.site = site
        self.http = http
        self.workers = workers
        self.page_size = page_size
        m = re.search(r"/sites/([A-Za-z0-9_]+)", site.career_page)
        if not m:
            raise ValueError(f"No /sites/<id> in career page URL: {site.career_page}")
        self.site_number = m.group(1)
        self.api_host: str | None = None

    @property
    def key(self) -> str:
        return self.site.key

    @property
    def name(self) -> str:
        return self.site.name

    def enrich(self, record: dict) -> dict:
        return self.site.enrich(record)

    # -- discovery ----------------------------------------------------------
    def discover_api_host(self) -> str:
        """The REST API lives on the tenant's *.oraclecloud.com host."""
        parsed = urlparse(self.site.career_page)
        if parsed.netloc.endswith("oraclecloud.com"):
            return f"{parsed.scheme}://{parsed.netloc}"
        try:
            html = self.http.get(self.site.career_page).text
            hosts = re.findall(r"https://[A-Za-z0-9.-]+\.oraclecloud\.com", html)
            if hosts:
                return Counter(hosts).most_common(1)[0][0]
        except requests.RequestException as exc:
            log.warning("%s: could not load career page (%s)", self.name, exc)
        if self.site.fallback_api_host:
            log.warning("%s: API host not found in page, using fallback", self.name)
            return self.site.fallback_api_host
        raise RuntimeError(f"{self.name}: could not discover the Oracle API host")

    # -- listing ------------------------------------------------------------
    def list_requisitions(self, max_jobs: int | None = None) -> list[dict]:
        url = self.api_host + self.LIST_PATH
        listings: dict[str, dict] = {}
        offset, total = 0, None
        while True:
            finder = (f"findReqs;siteNumber={self.site_number},facetsList=NONE,"
                      f"limit={self.page_size},offset={offset},sortBy=POSTING_DATES_DESC")
            data = self.http.json(url, params={
                "onlyData": "true",
                "expand": "requisitionList.secondaryLocations",  # required to get the list
                "finder": finder,
            })
            search = (data.get("items") or [{}])[0]
            total = search.get("TotalJobsCount", total)
            batch = search.get("requisitionList") or []
            for req in batch:
                listings.setdefault(str(req["Id"]), req)
            log.info("%s: listed %d / %s", self.name, len(listings), total)
            offset += len(batch)
            if (not batch or (total is not None and offset >= total)
                    or (max_jobs and len(listings) >= max_jobs)):
                break
        if total and not max_jobs and len(listings) < total:
            log.warning("%s: %d of %d reported postings listed (postings can change mid-crawl)",
                        self.name, len(listings), total)
        items = list(listings.values())
        return items[:max_jobs] if max_jobs else items

    # -- detail -------------------------------------------------------------
    def get_detail(self, listing: dict) -> dict | None:
        job_id = str(listing["Id"])
        data = self.http.json(self.api_host + self.DETAIL_PATH, params={
            "expand": "all",
            "onlyData": "true",
            "finder": f'ById;Id="{job_id}",siteNumber={self.site_number}',
        })
        items = data.get("items") or []
        if not items:
            return None
        d = items[0]
        flex = {f["Prompt"]: f.get("Value")
                for f in d.get("requisitionFlexFields") or [] if f.get("Prompt")}
        work = (d.get("workLocation") or [{}])[0] or {}
        secondary = [s.get("Name") for s in listing.get("secondaryLocations") or [] if s.get("Name")]
        return {
            "Job_ID": job_id,
            "Job_Title": d.get("Title") or listing.get("Title"),
            "Category": d.get("Category"),
            "Job_Function": d.get("JobFunction"),
            "Job_Schedule": d.get("JobSchedule"),
            "Location_Raw": d.get("PrimaryLocation") or listing.get("PrimaryLocation"),
            "Secondary_Locations": "; ".join(secondary) or None,
            "Work_Location_Name": work.get("LocationName"),
            "Posting_Date": listing.get("PostedDate") or d.get("ExternalPostedStartDate"),
            "Closing_Date": d.get("ExternalPostedEndDate") or listing.get("PostingEndDate"),
            "Description": clean_html(d.get("ExternalDescriptionStr")),
            "Qualifications": clean_html(d.get("ExternalQualificationsStr")),
            "Responsibilities": clean_html(d.get("ExternalResponsibilitiesStr")),
            "Short_Description": clean_html(d.get("ShortDescriptionStr")),
            "Flex_Fields": flex,
            "Job_URL": f"{self.site.career_page.rstrip('/')}/job/{job_id}",
        }

    def scrape(self, max_jobs: int | None = None) -> list[dict]:
        self.api_host = self.discover_api_host()
        log.info("%s: API host %s, site %s", self.name, self.api_host, self.site_number)
        listings = self.list_requisitions(max_jobs)
        return fetch_parallel(listings, self.get_detail, self.workers, self.name)


# ---- Government of Saskatchewan: structured "flex fields" -------------------
def enrich_gos(record: dict) -> dict:
    r = dict(record)
    flex = r.pop("Flex_Fields", None) or {}
    if isinstance(flex, str):
        flex = json.loads(flex)
    code, org = split_org_code(flex.get("Organization"))
    r.update({
        "Organization_Code": code,
        "Organization": org,
        "Employment_Type": flex.get("Employment Type"),
        "Salary_Range": flex.get("Salary Range"),
        "Hours_of_Work": flex.get("Hours of Work"),
        "Occupation_Code": flex.get("Occupation Code"),
        "Grade": flex.get("Grade"),
        "Number_of_Openings": to_number(flex.get("Number of Openings")),
        "Workplace_Diversity_Groups": flex.get("Diversity Groups"),
    })
    return r


# ---- SHA: facts are embedded in the description as "Label: value" -----------
# label -> output column (None = recognised as a boundary but not kept)
SHA_LABELS: dict[str, str | None] = {
    "Position #": "Position_Number",
    "Position Name": "Position_Name",
    "Closure Date": "Closure_Note",
    "Expected Start Date": "Expected_Start_Date",
    "Expected Up to Date": "Expected_End_Date",
    "Expected end date": "Expected_End_Date",
    "Union": "Union",
    "Facility": "Facility",
    "City/Town": "City_Town",
    "Work Location": "Work_Location",
    "Department": "Department",
    "Type": "Employment_Type",
    "FTE": "FTE",
    "Shift Information": "Shift_Information",
    "Shift info": "Shift_Information",
    "Number of Hours per Rotation": "Hours_per_Rotation",
    "Hours of Work": "Hours_of_Work",
    "Relief": "Relief",
    "Float": "Float",
    "Field Hours": "Field_Hours",
    "Salary or Pay Band": "Salary_Range",
    "Travel Required": "Travel_Required",
    "Profile Description": None,
    "Job Description": None,
    "Job Summary": None,
    "Human Resources Exemption": None,
}
_SHA_LABEL_LOOKUP = {k.lower(): v for k, v in SHA_LABELS.items()}
_SHA_LABEL_RE = re.compile(
    r"(?<![\w/#])("
    + "|".join(re.escape(k) for k in sorted(SHA_LABELS, key=len, reverse=True))
    + r")\s*:",
    re.I,
)


def parse_labelled_fields(text: str | None) -> dict:
    """Slice 'Label: value Label: value ...' text; each value runs to the next label."""
    if is_blank(text):
        return {}
    text = str(text)
    out: dict = {}
    matches = list(_SHA_LABEL_RE.finditer(text))
    for i, m in enumerate(matches):
        column = _SHA_LABEL_LOOKUP.get(m.group(1).lower())
        if not column:
            continue
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        value = squash(text[m.end():end].strip(" ,;"))
        if column not in out or out[column] is None:
            out[column] = value
    return out


def enrich_sha(record: dict) -> dict:
    r = dict(record)
    r.pop("Flex_Fields", None)
    r.update(parse_labelled_fields(r.get("Description")))
    r["Organization"] = r.get("Facility") or r.get("Work_Location_Name")
    city_town = clean_city_text(r.get("City_Town"))
    if city_town and "/" in city_town:          # multi-city postings, e.g. Regina/Saskatoon
        r["City"] = city_town
    else:
        r["City"] = split_location(r.get("Location_Raw"))[0] or city_town
    return r

_SECTION_HEADING = re.compile(r"^([A-Z][A-Z0-9 /&'\-]{2,60}):\s*$")
_BOILERPLATE = re.compile(
    r"Candidates under consideration|At SaskPower, we believe|Please apply by", re.I)
_EMPLOYMENT_PATTERNS = [
    r"permanent\s*[-\u2013]?\s*full[\s-]?time", r"permanent\s*[-\u2013]?\s*part[\s-]?time",
    r"temporary\s*[-\u2013]?\s*full[\s-]?time", r"temporary\s*[-\u2013]?\s*part[\s-]?time",
    r"full[\s-]?time", r"part[\s-]?time", r"casual", r"temporary", r"permanent",
]
SASKPOWER_SECTIONS = {
    "JOB SUMMARY": "Job_Summary",
    "KEY ACCOUNTABILITIES": "Responsibilities",
    "KNOWLEDGE/SKILLS/ABILITIES": "Qualifications",
    "NOTES": "Notes",
    "AT A GLANCE": "At_A_Glance",
    "JOIN OUR TALENT POOL": "Talent_Pool_Info",
}


def split_sections(text: str | None) -> dict[str, str]:
    """Split multi-line posting text on stand-alone 'HEADING:' lines."""
    sections: dict[str, str] = {}
    current, buf = None, []

    def flush():
        if current:
            value = _BOILERPLATE.split("\n".join(buf))[0]
            sections[current] = squash(value)

    for line in (text or "").splitlines():
        m = _SECTION_HEADING.match(line.strip())
        if m:
            flush()
            current, buf = m.group(1).strip(), []
        elif current:
            buf.append(line)
    flush()
    return sections


def find_employment_type(text: str) -> str | None:
    m = re.search(r"Employment type\s*:\s*([^\n]+)", text, re.I)
    if m:
        return squash(m.group(1))
    for pattern in _EMPLOYMENT_PATTERNS:
        m = re.search(rf"\b{pattern}\b", text, re.I)
        if m:
            return squash(m.group(0))
    return None


class SaskPowerScraper:
    key = "SASKPOWER"
    name = "SaskPower"

    def __init__(self, base_url: str, http: Http, workers: int = 4):
        self.base_url = base_url.rstrip("/")
        self.http = http
        self.workers = workers

    def _parse_result(self, cell) -> dict | None:
        link = cell.select_one("a.jobTitle-link") or cell.select_one('a[href*="/job/"]')
        if not link or not link.get("href"):
            return None
        url = urljoin(self.base_url + "/", link["href"])
        m = re.search(r"/(\d+)/?(?:\?.*)?$", url)
        if not m:
            return None

        row = cell.find_parent("tr") or cell  # date/department/location may sit in sibling cells

        def text(selector: str) -> str | None:
            el = row.select_one(selector)
            return squash(el.get_text(" ", strip=True)) if el else None

        return {
            "Job_ID": m.group(1),
            "Job_Title": squash(link.get_text(" ", strip=True)),
            "Location_Raw": text(".jobLocation"),
            "Category": text(".jobDepartment"),
            "Posting_Date": text(".jobDate"),
            "Job_URL": url,
        }

    def list_jobs(self, max_jobs: int | None = None) -> list[dict]:
        jobs: dict[str, dict] = {}
        startrow, total = 0, None
        while True:
            soup = self.http.soup(f"{self.base_url}/search/", params={
                "q": "", "sortColumn": "referencedate",
                "sortDirection": "desc", "startrow": startrow,
            })
            # Each result row has a td.colTitle; fall back to raw links if the template changes.
            cells = soup.select("td.colTitle") or soup.select('a[href*="/job/"]')
            new = 0
            for cell in cells:
                job = self._parse_result(cell if cell.name != "a" else cell.parent)
                if job and job["Job_ID"] not in jobs:
                    jobs[job["Job_ID"]] = job
                    new += 1
            label = soup.select_one(".paginationLabel")
            if label:
                m = re.search(r"of\s+([\d,]+)", label.get_text(" ", strip=True))
                if m:
                    total = int(m.group(1).replace(",", ""))
            log.info("%s: listed %d / %s", self.name, len(jobs), total)
            startrow += len(cells)
            if (not cells or new == 0 or (total and len(jobs) >= total)
                    or (max_jobs and len(jobs) >= max_jobs)):
                break
        items = list(jobs.values())
        return items[:max_jobs] if max_jobs else items

    def get_detail(self, listing: dict) -> dict:
        soup = self.http.soup(listing["Job_URL"])
        header = soup.select_one(".jobTitle")
        header_text = header.get_text(" ", strip=True) if header else ""
        title_el = header.find(["h1", "h2", "h3"]) if header else None
        posted = re.search(r"Date Posted:\s*([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})", header_text)
        location = re.search(r"Location:\s*(.+?)(?:\s+Apply now|$)", header_text)
        description = soup.select_one(".jobdescription")
        return {
            **listing,
            "Job_Title": squash(title_el.get_text(" ", strip=True)) if title_el else listing["Job_Title"],
            "Posting_Date": posted.group(1) if posted else listing.get("Posting_Date"),
            "Location_Raw": location.group(1).strip() if location else listing.get("Location_Raw"),
            "Description_Raw": description.get_text("\n", strip=True) if description else None,
        }

    def scrape(self, max_jobs: int | None = None) -> list[dict]:
        listings = self.list_jobs(max_jobs)
        return fetch_parallel(listings, self.get_detail, self.workers, self.name)

    @staticmethod
    def enrich(record: dict) -> dict:
        r = dict(record)
        text = r.pop("Description_Raw", None) or ""
        for heading, column in SASKPOWER_SECTIONS.items():
            r[column] = split_sections(text).get(heading)

        head = text.split("JOB SUMMARY:")[0][:800]
        employment = find_employment_type(head) or find_employment_type(text)
        if "student" in (r.get("Job_Title") or "").lower():
            employment = f"Student ({employment})" if employment else "Student"
        r["Employment_Type"] = employment

        lines = text.splitlines()
        salary_lines = [squash(f"{lines[i - 1] if i else ''} {line}")
                        for i, line in enumerate(lines) if "$" in line]
        r["Salary_Range"] = " | ".join(salary_lines) or None

        closing = re.search(r"(?:apply\s+by|no\s+later\s+than)\s*(\d{1,2}/\d{1,2}/\d{4})", text, re.I)
        r["Closing_Date"] = closing.group(1) if closing else None

        union = (re.search(r"Union\s*:\s*([^\n]+)", text)
                 or re.search(r"\(([A-Z]{3,6})\),?\s*(Local\s*\d+)", text))
        r["Union"] = squash(" ".join(g for g in union.groups() if g)) if union else None

        r["Organization"] = "SaskPower"
        r["Description"] = squash(text)
        return r

_WORKDAY_URL = re.compile(r"https://[\w.-]+\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?[\w-]+")
_UNION_PATTERN = re.compile(r"\b(?:COPE|CUPE|SEIU|SGEU|IBEW|Unifor|UFCW|OPEIU)\b(?:[\s,]*Local\s*\d+)?", re.I)

# keyword found in a section heading (lower-case) -> output column; first match wins
WORKDAY_SECTIONS = [
    ("accountabilities", "Responsibilities"),
    ("responsibilities", "Responsibilities"),
    ("duties", "Responsibilities"),
    ("knowledge/skills", "Qualifications"),
    ("qualifications", "Qualifications"),
    ("requirements", "Qualifications"),
    ("position details", "Position_Details"),
    ("additional information", "Additional_Information"),
    ("salary", "Salary_Range"),
    ("rate range", "Salary_Range"),
    ("compensation", "Salary_Range"),
    ("closing date", "Closing_Date_Text"),
]


def html_sections(html: str | None) -> tuple[str, dict[str, str]]:
    """Split a posting's HTML on its headings (<h1>-<h5>, or a <p> that is only bold text).

    Returns (intro text before the first heading, {heading_lower: section text}).
    """
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "p"]):
        text = tag.get_text(" ", strip=True)
        if not text:
            continue
        is_heading = tag.name != "p" or (
            tag.find(["b", "strong"]) and tag.find(["b", "strong"]).get_text(" ", strip=True) == text)
        if is_heading and len(text) <= 60:
            tag.replace_with(f"\n@@SECTION@@{text}\n")
    for li in soup.find_all("li"):
        li.insert_before("\n• ")
    parts = re.split(r"@@SECTION@@(.+)", soup.get_text("\n"))
    intro = squash(parts[0])
    sections = {}
    for heading, body in zip(parts[1::2], parts[2::2]):
        sections.setdefault(heading.strip().lower(), squash(body))
    return intro, sections


@dataclass
class WorkdaySite:
    key: str                          # Record_ID prefix
    name: str                         # Source label
    career_page: str                  # employer's own careers page that links to Workday
    fallback_workday_url: str | None = None
    tag_facets: tuple = (("jobFamilyGroup", "Category"), ("workerSubType", "Worker_Type"))


class WorkdayScraper:
    PAGE_SIZE = 20  # Workday's CXS API rejects anything larger

    def __init__(self, site: WorkdaySite, http: Http, workers: int = 4):
        self.site = site
        self.http = http
        self.workers = workers
        self.api: str | None = None

    @property
    def key(self) -> str:
        return self.site.key

    @property
    def name(self) -> str:
        return self.site.name

    # -- discovery ----------------------------------------------------------
    def discover_workday_url(self) -> str:
        try:
            html = self.http.get(self.site.career_page).text
            urls = _WORKDAY_URL.findall(html)
            if urls:
                return Counter(urls).most_common(1)[0][0]
        except requests.RequestException as exc:
            log.warning("%s: could not load career page (%s)", self.name, exc)
        if self.site.fallback_workday_url:
            log.warning("%s: Workday link not found in page, using fallback", self.name)
            return self.site.fallback_workday_url
        raise RuntimeError(f"{self.name}: could not discover the Workday site")

    @staticmethod
    def api_base(workday_url: str) -> str:
        """https://acme.wd10.myworkdayjobs.com/en-US/Careers -> .../wday/cxs/acme/Careers"""
        parsed = urlparse(workday_url)
        tenant = parsed.netloc.split(".")[0]
        path = [p for p in parsed.path.split("/") if p and not re.fullmatch(r"[a-z]{2}-[A-Z]{2}", p)]
        return f"{parsed.scheme}://{parsed.netloc}/wday/cxs/{tenant}/{path[0]}"

    # -- listing ------------------------------------------------------------
    def _search(self, offset: int = 0, facets: dict | None = None) -> dict:
        return self.http.post_json(self.api + "/jobs", {
            "appliedFacets": facets or {}, "limit": self.PAGE_SIZE,
            "offset": offset, "searchText": "",
        })

    def _all_paths(self, facets: dict) -> list[str]:
        paths, offset = [], 0
        while True:
            batch = self._search(offset, facets).get("jobPostings") or []
            paths += [j["externalPath"] for j in batch if j.get("externalPath")]
            offset += len(batch)
            if len(batch) < self.PAGE_SIZE:
                return paths

    def list_jobs(self, max_jobs: int | None = None) -> list[dict]:
        jobs: dict[str, dict] = {}
        offset, total, facets = 0, None, []
        while True:
            data = self._search(offset)
            if offset == 0:
                total, facets = data.get("total"), data.get("facets") or []
            batch = data.get("jobPostings") or []
            for job in batch:
                if job.get("externalPath"):
                    jobs.setdefault(job["externalPath"], job)
            log.info("%s: listed %d / %s", self.name, len(jobs), total)
            offset += len(batch)
            if (not batch or (total and offset >= total)
                    or (max_jobs and len(jobs) >= max_jobs)):
                break
        items = list(jobs.values())[:max_jobs] if max_jobs else list(jobs.values())

        # The detail API doesn't return category / worker type, so tag postings by
        # running one filtered search per facet value.
        wanted = {f["facetParameter"]: f for f in facets}
        for param, column in self.site.tag_facets:
            for value in (wanted.get(param) or {}).get("values") or []:
                if not value.get("id"):
                    continue
                for path in self._all_paths({param: [value["id"]]}):
                    if path in jobs:
                        jobs[path][column] = value.get("descriptor")
        return items

    # -- detail -------------------------------------------------------------
    def get_detail(self, listing: dict) -> dict:
        data = self.http.json(self.api + listing["externalPath"])
        info = data.get("jobPostingInfo") or {}
        return {
            "Job_ID": info.get("jobReqId") or (listing.get("bulletFields") or [None])[0],
            "Job_Title": info.get("title") or listing.get("title"),
            "Category": listing.get("Category"),
            "Worker_Type": listing.get("Worker_Type"),
            "Time_Type": info.get("timeType"),
            "Location_Raw": info.get("location") or listing.get("locationsText"),
            "Secondary_Locations": "; ".join(info.get("additionalLocations") or []) or None,
            "Work_Location_Name": (info.get("jobRequisitionLocation") or {}).get("descriptor"),
            "Posting_Date": info.get("startDate"),
            "Closing_Date_Posted": info.get("endDate"),
            "Organization": (data.get("hiringOrganization") or {}).get("name") or self.name,
            "Description_HTML": info.get("jobDescription"),
            "Job_URL": info.get("externalUrl"),
        }

    def scrape(self, max_jobs: int | None = None) -> list[dict]:
        workday_url = self.discover_workday_url()
        self.api = self.api_base(workday_url)
        log.info("%s: Workday API %s", self.name, self.api)
        listings = self.list_jobs(max_jobs)
        return fetch_parallel(listings, self.get_detail, self.workers, self.name)

    # -- enrichment ---------------------------------------------------------
    @staticmethod
    def enrich(record: dict) -> dict:
        r = dict(record)
        intro, sections = html_sections(r.pop("Description_HTML", None))
        salary_heading = ""
        for heading, text in sections.items():
            for keyword, column in WORKDAY_SECTIONS:
                if keyword in heading:
                    if not r.get(column):
                        r[column] = text
                        salary_heading = heading if column == "Salary_Range" else salary_heading
                    break

        details = r.get("Position_Details") or ""
        employment = " ".join(p for p in (r.get("Worker_Type"), r.get("Time_Type")) if p)
        r["Employment_Type"] = (employment or find_employment_type(details)
                                or find_employment_type(intro or ""))

        named = _UNION_PATTERN.search(details) or _UNION_PATTERN.search(intro or "")
        scope = re.match(r"\W*(non[\s-]?union|union)\b", details, re.I)
        if named:
            r["Union"] = squash(named.group(0))
        elif scope:
            r["Union"] = "Non-union" if scope.group(1).lower().startswith("non") else "Union"
        else:
            r["Union"] = None

        if r.get("Salary_Range"):
            # '$39.91 ($6,474.20) - $47.84 ($7,760.60)  #LI-POST' -> '$39.91 - $47.84'
            pay = re.sub(r"#\w+(?:-\w+)*|\(\s*\$[^)]*\)", "", r["Salary_Range"])
            if "hourly" in salary_heading and "hour" not in pay.lower():
                pay = f"Hourly: {pay}"
            r["Salary_Range"] = squash(pay) or None
        r["Closing_Date"] = r.pop("Closing_Date_Text", None) or r.get("Closing_Date_Posted")
        r["Description"] = squash(" ".join(filter(None, [intro, *sections.values()])))
        return r

_COMPETITION = re.compile(r"\bSW[-\s]?(\d{1,3})[-\s](\d{4})\b", re.I)
_PDF_FIELD = re.compile(
    r"^\s*(Job Title|Job Competition|Competition\s*#?|Location|Salary(?: Range)?|"
    r"Organizational Unit|Division)\s*:\s*(.+)$", re.I | re.M)
_PDF_SECTIONS = [  # (regex at the start of a line, column) - text runs to the next marker
    (r"Short Description\b", "Job_Summary"),
    (r"(?:Key Duties and Responsibilities|Job Duties|Duties and Responsibilities)\b:?", "Responsibilities"),
    (r"(?:Job Specifications|Qualifications|Educational Requirements and Technical Qualifications)\b:?", "Qualifications"),
    (r"(?=To qualify\b)", "Qualifications"),
    (r"Special Requirements\b:?", "Special_Requirements"),
    (r"Application Deadline\b", "Deadline_Text"),
    (r"To Apply\b", None),
]


def pdf_text(data: bytes) -> tuple[str, dict]:
    """Text of every page (page header/footer lines removed) plus the PDF metadata."""
    reader = PdfReader(io.BytesIO(data))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    text = "\n".join(line for line in text.splitlines()
                     if not re.search(r"\bPage \d+ of \d+\b", line))
    return text, dict(reader.metadata or {})


def pdf_sections(text: str) -> dict[str, str]:
    markers = []
    for pattern, column in _PDF_SECTIONS:
        m = re.search(rf"^\s*{pattern}", text, re.M)
        if m:
            markers.append((m.start(), m.end(), column))
    markers.sort()
    out = {}
    for i, (start, end, column) in enumerate(markers):
        stop = markers[i + 1][0] if i + 1 < len(markers) else len(text)
        if column and not out.get(column):
            out[column] = squash(text[end:stop])
    if not out.get("Responsibilities"):
        # No duties heading: the role paragraph starts 'As the <title>, you will...'.
        m = re.search(r"^\s*(?:As the|As a|This position|The successful candidate)\b", text, re.M)
        if m:
            stop = min([s for s, _, _ in markers if s > m.start()] or [len(text)])
            out["Responsibilities"] = squash(text[m.start():stop])
    return {k: v for k, v in out.items() if v}


class SaskWaterScraper:
    key = "SASKWATER"
    name = "SaskWater"
    MEDIA_API = "/wp-json/wp/v2/media"

    def __init__(self, career_page: str, http: Http, workers: int = 4, include_past: bool = True):
        self.career_page = career_page
        self.http = http
        self.workers = workers
        self.include_past = include_past
        parsed = urlparse(career_page)
        self.site = f"{parsed.scheme}://{parsed.netloc}"

    # -- listing ------------------------------------------------------------
    def current_pdfs(self) -> list[str]:
        """PDF links under the 'Current Opportunities' heading of the careers page."""
        soup = self.http.soup(self.career_page)
        headings = [h for h in soup.find_all(["h2", "h3", "h4"])
                    if "current opportunities" in h.get_text(" ", strip=True).lower()]
        links = []
        if headings:
            for sib in headings[-1].find_next_siblings():
                if sib.name in ("h1", "h2", "h3") or sib.find(["h1", "h2", "h3"]):
                    break
                links += [a["href"] for a in sib.find_all("a", href=True)]
                if sib.name == "a" and sib.get("href"):
                    links.append(sib["href"])
        if not links:  # template changed: any posting PDF on the page
            links = [a["href"] for a in soup.find_all("a", href=True)
                     if _COMPETITION.search(a["href"])]
        urls = [urljoin(self.career_page, u) for u in links if u.lower().split("?")[0].endswith(".pdf")]
        return list(dict.fromkeys(urls))

    def media_library(self) -> list[dict]:
        """Every PDF in the WordPress media library (gives upload dates and past postings)."""
        items, page, pages = [], 1, 1
        while page <= pages:
            resp = self.http.get(self.site + self.MEDIA_API, accept="application/json", params={
                "mime_type": "application/pdf", "per_page": 100, "page": page,
                "orderby": "date", "order": "desc", "_fields": "date,source_url,title"})
            items += resp.json()
            pages = int(resp.headers.get("X-WP-TotalPages", 1))
            page += 1
        return items

    def list_jobs(self, max_jobs: int | None = None) -> list[dict]:
        media = self.media_library()
        uploaded = {m["source_url"]: m["date"] for m in media}
        current = self.current_pdfs()
        log.info("%s: %d open posting(s) on the careers page", self.name, len(current))
        jobs = {url: {"PDF_URL": url, "Uploaded": uploaded.get(url), "Posting_Status": "Open"}
                for url in current}
        if self.include_past:
            # Newest upload per competition number (reposts replace older copies).
            latest: dict[str, dict] = {}
            for m in media:  # newest first
                title = BeautifulSoup(m["title"]["rendered"], "html.parser").get_text()
                comp = _COMPETITION.search(title) or _COMPETITION.search(m["source_url"])
                if comp:
                    latest.setdefault(f"{comp.group(1)}-{comp.group(2)}", m)
            open_comps = {f"{c.group(1)}-{c.group(2)}" for c in map(_COMPETITION.search, current) if c}
            for comp, m in latest.items():
                if comp not in open_comps:
                    jobs.setdefault(m["source_url"], {"PDF_URL": m["source_url"], "Uploaded": m["date"],
                                                      "Posting_Status": "Closed"})
            log.info("%s: %d posting(s) including past competitions", self.name, len(jobs))
        items = sorted(jobs.values(), key=lambda j: j["Uploaded"] or "", reverse=True)
        return items[:max_jobs] if max_jobs else items

    # -- detail -------------------------------------------------------------
    def get_detail(self, listing: dict) -> dict:
        text, meta = pdf_text(self.http.get(listing["PDF_URL"], accept="application/pdf").content)
        created = re.match(r"D:(\d{4})(\d{2})(\d{2})", str(meta.get("/CreationDate", "")))
        return {
            **listing,
            "Uploaded": listing["Uploaded"] or ("-".join(created.groups()) if created else None),
            "PDF_Text": text,
        }

    def scrape(self, max_jobs: int | None = None) -> list[dict]:
        listings = self.list_jobs(max_jobs)
        return fetch_parallel(listings, self.get_detail, self.workers, self.name)

    # -- enrichment ---------------------------------------------------------
    @staticmethod
    def enrich(record: dict) -> dict:
        r = dict(record)
        text = r.pop("PDF_Text", None) or ""
        fields = {}
        for label, value in _PDF_FIELD.findall(text):
            label = "Competition" if "competition" in label.lower() else label.title()
            label = "Salary" if label.startswith("Salary") else label
            fields.setdefault(label, squash(value))

        competition = fields.get("Competition") or ""
        comp = _COMPETITION.search(competition) or _COMPETITION.search(r["PDF_URL"])
        r["Job_ID"] = f"SW-{int(comp.group(1)):02d}-{comp.group(2)}" if comp else r["PDF_URL"].rsplit("/", 1)[-1]
        r["Is_Repost"] = bool(re.search(r"repost", competition + r["PDF_URL"], re.I))

        title = fields.get("Job Title") or ""
        openings = re.search(r"[-–]\s*(\d+)\s+positions?\b", title, re.I)
        r["Number_of_Openings"] = int(openings.group(1)) if openings else 1
        r["Job_Title"] = squash(re.sub(r"\s*[-–]\s*\d+\s+positions?\b|\(\s*repost\s*\)", "", title, flags=re.I))
        term = re.search(r"\(([^)]*(?:term|time|seasonal|casual)[^)]*)\)", title, re.I)
        if term:
            r["Job_Title"] = squash(r["Job_Title"].replace(term.group(0), ""))
        r["Employment_Type"] = (squash(term.group(1)) if term
                                else find_employment_type(text.split("Application Deadline")[0][:1500]))

        r["Organization"] = "SaskWater"
        r["Category"] = fields.get("Division")
        r["Job_Function"] = fields.get("Organizational Unit")

        # Location: keep the full text, use the first city listed for City.
        location = fields.get("Location")
        r["Location_Raw"] = location
        if location:
            first = re.split(r",|\bor\b|\band\b", re.sub(r"\(.*?\)|to be determined\s*[-–]?|preferred",
                                                        "", location, flags=re.I))[0]
            r["City"] = clean_city_text(first)
            r["Province"], r["Country"] = "SK", "Canada"

        salary = fields.get("Salary")
        r["Salary_Range"] = salary
        scale = re.search(r"(Pay Grade\s*\d+|Management Level\s*\d+|\bML\s*\d+)", salary or "", re.I)
        r["Pay_Scale"] = (re.sub(r"^ML\s*", "Management Level ", scale.group(1), flags=re.I)
                          if scale else None)
        r["Scope"] = (None if not scale else "In-scope" if scale.group(1).lower().startswith("pay")
                      else "Out-of-scope")

        sections = pdf_sections(text)
        for column in ("Job_Summary", "Responsibilities", "Qualifications", "Special_Requirements"):
            r[column] = sections.get(column)

        # Deadline, e.g. 'September 8, 2026 at 5:00 p.m.' or 'April 22 at 5:00 p.m.' (no year).
        posted = pd.to_datetime(r.get("Uploaded"), errors="coerce")
        r["Posting_Date"] = posted.date().isoformat() if not pd.isna(posted) else None
        deadline = re.search(r"([A-Z][a-z]+\.?\s+\d{1,2})(?:\s*,\s*|\s+)?(\d{4})?",
                             sections.get("Deadline_Text") or "")
        r["Closing_Date"] = None
        if deadline:
            year = deadline.group(2) or (posted.year if not pd.isna(posted) else None)
            closing = pd.to_datetime(f"{deadline.group(1)} {year}", errors="coerce")
            if not pd.isna(closing) and not pd.isna(posted) and closing < posted.normalize() and not deadline.group(2):
                closing += pd.DateOffset(years=1)
            r["Closing_Date"] = None if pd.isna(closing) else closing.date().isoformat()

        r["Description"] = squash(text.split("To Apply")[0])
        r["Job_URL"] = r["PDF_URL"]
        return r

# 'Label:' lines at the top of each posting -> column. The value may sit on the same
# line or the next one, and SaskTel sometimes writes 'Salary' / ': $27.42' on two lines.
SASKTEL_LABELS = {
    "location": "Location_Text",
    "number of positions": "Number_of_Openings",
    "salary": "Salary_Text",
    "type of position": "Employment_Type",
    "position type": "Employment_Type",
    "closing date": "Closing_Date_Text",
    "hours of work": "Hours_of_Work",
    "competition comments": "Comments",
    "comments": "Comments",
}
# Upper-case section heading -> column (first one found wins)
SASKTEL_SECTIONS = {
    "JOB SUMMARY": "Job_Summary",
    "GENERAL ACCOUNTABILITY": "Job_Summary",
    "POSITION OVERVIEW": "Job_Summary",
    "TYPICAL DUTIES & RESPONSIBILITIES": "Responsibilities",
    "TYPICAL DUTIES & RESPONSIBILITES": "Responsibilities",  # (sic) appears on the site
    "SPECIFIC ACCOUNTABILITIES": "Responsibilities",
    "QUALIFICATIONS": "Qualifications",
    "SPECIFICATIONS": "Qualifications",
    "WORKING CONDITIONS": "Working_Conditions",
    "PHYSICAL DEMANDS": "Working_Conditions",
    "ENVIRONMENTAL CONDITIONS": "Working_Conditions",
}
_SASKTEL_HEADING = re.compile(r"^[A-Z][A-Z0-9 &/,'()-]{3,60}$")
_SASKTEL_BOILERPLATE = re.compile(
    r"^(#LI-|What makes SaskTel special|CULTURE AND VALUES|About SaskTel|At SaskTel, we believe)", re.I)


def labelled_lines(lines: list[str], labels: dict[str, str]) -> dict[str, str]:
    """{'Location:' + next line 'Regina, SK'} -> {'Location_Text': 'Regina, SK'}."""
    pattern = re.compile(rf"^({'|'.join(map(re.escape, labels))})\s*(:)?\s*(.*)$", re.I)
    out = {}
    for i, line in enumerate(lines):
        m = pattern.match(line)
        if not m or not (m.group(2) or not m.group(3)):
            continue  # needs 'Label:' or a bare 'Label' line
        value = m.group(3) or (lines[i + 1] if i + 1 < len(lines) else "")
        value = squash(value.lstrip(": "))
        if (value and re.search(r"\w", value) and not pattern.match(value)
                and not _SASKTEL_HEADING.match(value)):  # empty label followed by a heading
            out.setdefault(labels[m.group(1).lower()], value)
    return out


def caps_sections(lines: list[str], headings: dict[str, str]) -> dict[str, str]:
    """Split on stand-alone UPPER-CASE headings; stop at the closing boilerplate."""
    out, current, buf = {}, None, []

    def flush():
        if current and buf and not out.get(current):
            out[current] = squash("\n".join(buf))

    for line in lines:
        if _SASKTEL_BOILERPLATE.match(line):
            break
        if _SASKTEL_HEADING.match(line):
            flush()
            current, buf = headings.get(line.strip()), []
        elif current:
            buf.append(line)
    flush()
    return out


class SaskTelScraper(SaskPowerScraper):
    key = "SASKTEL"
    name = "SaskTel"

    def get_detail(self, listing: dict) -> dict:
        soup = self.http.soup(listing["Job_URL"])
        meta = {m["itemprop"]: m.get("content") for m in soup.select("meta[itemprop]")}
        description = soup.select_one(".jobdescription")
        return {
            **listing,
            "Meta_Location": meta.get("addressLocality"),
            "Meta_Date_Posted": meta.get("datePosted"),
            "Meta_Valid_Through": meta.get("validThrough"),
            "Description_Raw": description.get_text("\n", strip=True) if description else None,
        }

    @staticmethod
    def enrich(record: dict) -> dict:
        r = dict(record)
        text = (r.pop("Description_Raw", None) or "").replace("\xa0", " ")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        r.update(labelled_lines(lines, SASKTEL_LABELS))
        r.update(caps_sections(lines, SASKTEL_SECTIONS))

        r["Is_Talent_Pool"] = bool(re.match(r"future opportunities", r.get("Job_Title") or "", re.I))
        r["Organization"] = "SaskTel"

        # Location: posting text > page metadata > search results. City = first one listed.
        location = r.pop("Location_Text", None) or r.get("Meta_Location") or r.get("Location_Raw")
        r["Location_Raw"] = location
        city, province, _ = split_location(location)
        r["City"] = clean_city_text(re.split(r"\bor\b|\band\b", city or "")[0])
        r["Province"] = province or ("SK" if r["City"] else None)
        r["Country"] = "Canada"

        # Salary: keep commission / 'based on experience' notes out of the numbers.
        salary = r.pop("Salary_Text", None)
        if salary:
            r["Salary_Notes"] = squash(" ".join(re.findall(r"\(([^)]*)\)", salary))) or None
            salary = squash(re.sub(r"\([^)]*\)", "", salary))
        r["Salary_Range"] = salary

        if r.get("Employment_Type"):
            r["Employment_Type"] = re.sub(r"\bPerm\b", "Permanent", r["Employment_Type"], flags=re.I)

        # Closing date: the posting's own date; else the page's validThrough (end-of-day
        # timestamp, so step back a second). Talent pools stay open, so no closing date.
        closing = r.pop("Closing_Date_Text", None)
        if not closing and r.get("Meta_Valid_Through") and not r["Is_Talent_Pool"]:
            ts = pd.to_datetime(r["Meta_Valid_Through"].replace(" UTC", ""), errors="coerce")
            if not pd.isna(ts):
                closing = (ts.tz_localize("UTC") - pd.Timedelta(seconds=1)).isoformat()
        r["Closing_Date"] = closing
        if not r.get("Posting_Date") and r.get("Meta_Date_Posted"):
            r["Posting_Date"] = r["Meta_Date_Posted"].replace(" UTC", "")

        r["Description"] = squash("\n".join(
            line for line in lines[: next((i for i, l in enumerate(lines) if _SASKTEL_BOILERPLATE.match(l)),
                                          len(lines))]))
        return r

def finalize(records: Iterable[dict], key: str, source: str, collection_date: str) -> pd.DataFrame:
    df = pd.DataFrame(list(records))
    if df.empty:
        return pd.DataFrame(columns=MASTER_COLUMNS)
    for col in MASTER_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["Job_ID"] = df["Job_ID"].astype(str)
    df = df.drop_duplicates("Job_ID").reset_index(drop=True)
    df["Record_ID"] = key + "_" + df["Job_ID"]
    df["Source"] = source

    # Location
    parsed = pd.DataFrame(df["Location_Raw"].apply(split_location).tolist(),
                          index=df.index, columns=["City", "Province", "Country"])
    for col in ("City", "Province", "Country"):
        df[col] = df[col].where(df[col].notna(), parsed[col])
    df["City"] = df["City"].apply(normalize_city)
    not_a_city = df["City"].str.contains(
        r"^(?:saskatchewan|sk|canada)$|\b(?:anywhere|tbd|to be determined|province[- ]?wide)\b",
        case=False, na=False)
    df.loc[not_a_city, "City"] = None
    df["Province"] = df["Province"].fillna("SK")
    df["Country"] = df["Country"].fillna("Canada")

    # Salary
    bounds = df["Salary_Range"].apply(parse_salary_range)
    df["Salary_Min"] = bounds.str[0]
    df["Salary_Max"] = bounds.str[1]
    df["Salary_Unit"] = [salary_unit(t, hi, hint)
                         for t, hi, hint in zip(df["Salary_Range"], df["Salary_Max"], df["Hours_of_Work"])]
    df["Salary_Status"] = [salary_status(t, lo) for t, lo in zip(df["Salary_Range"], df["Salary_Min"])]

    # Employment / FTE / counts
    df["Employment_Type_Standardized"] = df["Employment_Type"].apply(standardize_employment_type)
    df["FTE_Original"] = df["FTE"]
    df["FTE"] = df["FTE"].apply(fte_numeric)
    df["Number_of_Openings"] = pd.to_numeric(df["Number_of_Openings"], errors="coerce")

    # Dates
    for col in ("Posting_Date", "Closing_Date", "Expected_Start_Date", "Expected_End_Date"):
        if col in df.columns:
            df[col] = df[col].apply(to_local_date)
    df["Collection_Date"] = collection_date

    extras = [c for c in df.columns if c not in MASTER_COLUMNS]
    return df[MASTER_COLUMNS + extras]


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.apply(lambda col: col.map(
        lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v))
    out.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Saved %s (%d rows, %d cols)", path, len(df), len(df.columns))


# ---- history: open-posting tracker + cumulative past-postings master ---------
HISTORY_COLUMNS = ["First_Seen_Date", "Last_Seen_Date", "Closed_Detected_Date", "Closed_Reason"]


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False).replace("", None) if path.exists() else pd.DataFrame()


def split_open_closed(clean: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows a scraper already knows are closed (e.g. SaskWater's PDF archive) vs. open ones."""
    closed = clean["Posting_Status"].eq("Closed") if "Posting_Status" in clean else pd.Series(False, index=clean.index)
    return clean[~closed], clean[closed]


def update_history(current: pd.DataFrame, closed: pd.DataFrame, sources_ok: set[str],
                   out_dir: Path, today: str) -> pd.DataFrame:
    """Keep postings after they close.

    - history/open_postings_tracker.csv: every open posting with First/Last_Seen_Date.
    - past_postings_master.csv: cumulative closed postings. A posting lands here when it
      disappears from its site (only checked for sources that crawled successfully this run)
      or when the site itself lists it as closed.
    """
    tracker_path = out_dir / "history" / "open_postings_tracker.csv"
    past_path = out_dir / "past_postings_master.csv"
    tracker, past = load_csv(tracker_path), load_csv(past_path)

    first_seen = dict(zip(tracker["Record_ID"], tracker["First_Seen_Date"])) if not tracker.empty else {}
    current = current.copy()
    current["First_Seen_Date"] = current["Record_ID"].map(first_seen).fillna(today)
    current["Last_Seen_Date"] = today

    gone = pd.DataFrame()
    untouched = pd.DataFrame()
    if not tracker.empty:
        scraped = tracker["Source"].isin(sources_ok)
        gone = tracker[scraped & ~tracker["Record_ID"].isin(current["Record_ID"])].copy()
        gone["Closed_Detected_Date"] = today
        gone["Closed_Reason"] = "No longer listed on site"
        untouched = tracker[~scraped]  # sources not crawled (or failed) this run keep their state

    archived = closed.copy()
    archived["First_Seen_Date"] = today
    archived["Last_Seen_Date"] = today
    archived["Closed_Detected_Date"] = today
    archived["Closed_Reason"] = "Listed as closed by site"

    keep = MASTER_COLUMNS + HISTORY_COLUMNS
    parts = [f.reindex(columns=keep) for f in (past, gone, archived) if not f.empty]
    new_past = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=keep)
    new_past = new_past.drop_duplicates("Record_ID", keep="first")          # first closure wins
    new_past = new_past[~new_past["Record_ID"].isin(current["Record_ID"])]  # re-opened postings
    new_tracker = pd.concat([untouched, current.reindex(columns=MASTER_COLUMNS + HISTORY_COLUMNS[:2])],
                            ignore_index=True)

    save_csv(new_tracker, tracker_path)
    save_csv(new_past.reset_index(drop=True), past_path)
    already = set(past["Record_ID"]) if not past.empty else set()
    log.info("History: %d newly added to past postings (%d no longer listed); %d past postings in total",
             (~new_past["Record_ID"].isin(already)).sum(), len(gone), len(new_past))
    return new_past


def summarize(master: pd.DataFrame) -> pd.DataFrame:
    key_fields = ["Job_Title", "Description", "City", "Posting_Date",
                  "Salary_Min", "Employment_Type_Standardized"]
    summary = master.groupby("Source").agg(Records=("Record_ID", "count"))
    for col in key_fields:
        summary[f"{col}_missing_%"] = (master[col].isna().groupby(master["Source"]).mean() * 100).round(1)
    print("\n" + "=" * 70)
    print("MASTER DATASET SUMMARY")
    print("=" * 70)
    print(f"Total records:        {len(master)}")
    print(f"Duplicate Record_IDs: {master['Record_ID'].duplicated().sum()}")
    print(summary.to_string())
    return summary

SOURCE_KEYS = ("gos", "sha", "saskpower", "saskenergy", "saskwater", "sasktel")


def build_scrapers(http: Http, workers: int) -> dict:
    return {
        "gos": OracleHCMScraper(OracleSite(
            key="GOV_SK", name="Government of Saskatchewan",
            career_page=GOS_CAREER_PAGE, enrich=enrich_gos,
            fallback_api_host="https://fa-evgd-saasfaprod1.fa.ocs.oraclecloud.com",
        ), http, workers),
        "sha": OracleHCMScraper(OracleSite(
            key="SHA", name="Saskatchewan Health Authority",
            career_page=SHA_CAREER_PAGE, enrich=enrich_sha,
        ), http, workers),
        "saskpower": SaskPowerScraper(SASKPOWER_CAREER_SITE, http, workers),
        "saskenergy": WorkdayScraper(WorkdaySite(
            key="SASKENERGY", name="SaskEnergy",
            career_page=SASKENERGY_CAREER_PAGE,
            fallback_workday_url="https://saskenergy.wd10.myworkdayjobs.com/SaskEnergy",
        ), http, workers),
        "saskwater": SaskWaterScraper(SASKWATER_CAREER_PAGE, http, workers),
        "sasktel": SaskTelScraper(SASKTEL_CAREER_SITE, http, workers),
    }


def run_pipeline(sources: Iterable[str] = SOURCE_KEYS, out_dir: str | Path = "data",
                 max_jobs: int | None = None, workers: int = 4,
                 delay: float = 0.5) -> pd.DataFrame:
    """Crawl the selected sources, save raw/clean/master CSVs, return the master DataFrame.

    If MONGODB_URI is set, every posting is also written to MongoDB (see mongo_store.py),
    and MongoDB becomes the record of first/last-seen dates and closures.
    """
    import time as _time
    from datetime import datetime, timezone

    started = datetime.now(timezone.utc)
    t0 = _time.monotonic()
    out_dir = Path(out_dir)
    today = date.today().isoformat()
    http = Http(delay=delay)
    scrapers = build_scrapers(http, workers)

    store = None
    try:
        from mongo_store import MongoStore
        store = MongoStore.from_env(test_run=bool(max_jobs))
    except ImportError:
        if os.environ.get("MONGODB_URI"):
            raise
    if store is None:
        log.info("MONGODB_URI not set: CSV output only")

    frames, closed_frames, sources_ok, failed, per_source = [], [], set(), [], {}
    for key in sources:
        scraper = scrapers[key]
        log.info("=== %s ===", scraper.name)
        try:
            raw = scraper.scrape(max_jobs)
        except Exception:  # noqa: BLE001 - one site failing shouldn't stop the others
            log.exception("%s: crawl failed, skipping", scraper.name)
            failed.append(scraper.name)
            continue
        save_csv(pd.DataFrame(raw), out_dir / "raw" / f"{key}_raw_{today}.csv")
        enriched = [scraper.enrich(r) for r in raw]
        if store:
            store.save_raw(scraper.name,
                           [(f"{scraper.key}_{e.get('Job_ID')}", r) for r, e in zip(raw, enriched)],
                           today)
        clean = finalize(enriched, scraper.key, scraper.name, today)
        save_csv(clean, out_dir / "clean" / f"{key}_clean_{today}.csv")
        open_rows, closed_rows = split_open_closed(clean)
        frames.append(open_rows)
        closed_frames.append(closed_rows[MASTER_COLUMNS])
        sources_ok.add(scraper.name)
        per_source[scraper.name] = {"open": len(open_rows), "closed_listed": len(closed_rows)}

    if not sources_ok:
        raise RuntimeError("No source returned data")
    master = pd.concat([f[MASTER_COLUMNS] for f in frames], ignore_index=True)
    closed_all = pd.concat(closed_frames, ignore_index=True)
    save_csv(master, out_dir / f"saskatchewan_public_sector_jobs_master_{today}.csv")
    summarize(master)

    if store:
        try:
            stats = store.sync(master, closed_all, sources_ok, today, close_missing=not max_jobs)
            log.info("MongoDB: %s", stats)
            store.log_run({
                "run_date": today, "started_at": started,
                "duration_s": round(_time.monotonic() - t0, 1),
                "test_run": bool(max_jobs), "max_jobs": max_jobs,
                "sources_ok": sorted(sources_ok), "sources_failed": failed,
                "per_source": per_source, "open_postings": len(master), **stats,
            })
        finally:
            store.close()
    elif max_jobs:
        log.info("MAX_JOBS is set (test run): history files not updated")
    else:
        update_history(master, closed_all, sources_ok, out_dir, today)
    return master


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sources", nargs="+", default=list(SOURCE_KEYS), choices=SOURCE_KEYS)
    p.add_argument("--out-dir", default="data")
    p.add_argument("--max-jobs", type=int, default=None, help="test run: cap postings per source")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--delay", type=float, default=0.5)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_pipeline(args.sources, args.out_dir, args.max_jobs, args.workers, args.delay)


if __name__ == "__main__":
    main()
