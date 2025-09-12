import traceback
import pandas as pd
from openpyxl import load_workbook
from tqdm import tqdm
import re, time, html
from typing import Optional, Tuple, List
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup, Tag
import os

# ==============================
# Configuration
# ==============================
CMP_FILENAME = "CMP Data Template (long format)_PA.xlsx"  # set to your workbook path when available
OUTPUT_CSV_FILENAME = "edna_output.csv"
CMP_SHEETS = ["School Pop. Data", "School CS Data"]
ENABLE_FUZZY_MATCH = False   # set to False to disable fuzzy matching

# ==============================
# Exact-match only by default; fuzzy fallback
# ==============================
try:
    # RapidFuzz is faster and actively maintained
    from rapidfuzz import process as _rf_process
    _USE_RAPIDFUZZ = True
except Exception:
    _USE_RAPIDFUZZ = False
    try:
        from fuzzywuzzy import process as _fw_process
    except Exception:
        _fw_process = None  # If absent, we'll detect and behave accordingly
        
FUZZY_THRESHOLD = 60

# ==============================
# Utilities
# ==============================
def norm(s: str) -> str:
    return s.strip() if isinstance(s, str) else ""

def _status_norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()

def _status_canonicalize(s: str) -> str:
    """
    Map various forms to 'Open' or 'Closed' when obvious.
    Falls back to the original string if unknown.
    """
    sn = _status_norm(s)
    if sn.startswith("open"):
        return "Open"
    if sn.startswith("closed"):
        return "Closed"
    return s or ""

def ensure_headers(df: pd.DataFrame, required: list, ctx: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{ctx}] Missing required columns: {missing}. Found: {list(df.columns)}")

def build_header_map(ws):
    """Return dict: header_text -> 1-based column index, from the first row in a worksheet."""
    header_cells = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    return {str(v): i + 1 for i, v in enumerate(header_cells) if v is not None}
   
def _extract_one(query: str, choices: list[str]):
    """
    Backend-agnostic extractOne(query, choices) -> (best_match, score)
    Uses RapidFuzz if available; otherwise fuzzywuzzy; otherwise returns (None, 0).
    Scores are on 0..100 scale in both libs.
    """
    if _USE_RAPIDFUZZ:
        # RapidFuzz returns (choice, score, index)
        res = _rf_process.extractOne(query, choices)
        if res:
            return res[0], float(res[1])
        return None, 0.0
    elif _fw_process is not None:
        res = _fw_process.extractOne(query, choices)
        if res:
            return res[0], float(res[1])
        return None, 0.0
    else:
        return None, 0.0
  
# ==============================
# Edna Lookup
# ==============================

EDNA_BASE = "http://www.edna.pa.gov"
CURRENTNAME_SEARCH_TEMPLATE = (
    "http://www.edna.pa.gov/Screens/wfSearchEntityResults.aspx?"
    "AUN=&SchoolBranch=&CurrentName={CURRENT}&City=&HistoricalName=&IU=-1&CID=-1&"
    "CategoryIDs=3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c"
    "46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c"
    "3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c&StatusIDs=1%2c"
)

# Support searching by SchoolBranch (LOCATION_ID) — expects a 4-digit, zero-padded code
SCHOOLBRANCH_SEARCH_TEMPLATE = (
    "http://www.edna.pa.gov/Screens/wfSearchEntityResults.aspx?"
    "AUN=&SchoolBranch={BRANCH}&CurrentName=&City=&HistoricalName=&IU=-1&CID=-1&"
    "CategoryIDs=3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c"
    "46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c"
    "3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c&StatusIDs=1%2c"
)

def _schoolbranch_search_url(branch_code: str) -> str:
    branch_code = (branch_code or "").strip()
    branch_code = re.sub(r"\\D+", "", branch_code)
    branch_code = branch_code.zfill(4) if branch_code else ""
    return SCHOOLBRANCH_SEARCH_TEMPLATE.format(BRANCH=quote_plus(branch_code))

def _search_schoolbranch(session: requests.Session, branch_code: str) -> Tuple[List[Tuple[str, str, str]], str]:
    """
    Perform an EDNA search with SchoolBranch=<branch_code> (expects 4-digit string).
    Returns (candidates, search_url) where candidates is a list of (inst_name, branch, href).
    """
    url = _schoolbranch_search_url(branch_code)
    r = session.get(url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table, inst_idx, branch_idx = _find_results_table_and_institution_col(soup)
    if not table or inst_idx is None:
        return [], url
    return _iter_institution_links(table, inst_idx, branch_idx), url

POSTBACK_RE = re.compile(r"""^javascript:\s*__doPostBack\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)\s*;?\s*$""")

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; edna-lookup/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "close",
    })
    orig = s.request
    
    def wrapped(method, url, **kwargs):
        kwargs.setdefault("timeout", (10, 30))
        return orig(method, url, **kwargs)
        
    s.request = wrapped
    return s

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def _normalize_key(s: str) -> str:
    # Lowercase + collapse spaces + strip; longer limit for safety
    if not s:
        return ""
    s_norm = re.sub(r"\s+", " ", str(s)).strip().lower()
    return s_norm[:120]

def _pair_key(school: str, district: str) -> str:
    return f"{_normalize_key(school)}||{_normalize_key(district)}"

def _derive_district7_from_12(nces12: str) -> str:
    d = _digits_only(nces12)
    return d[:7] if len(d) >= 7 else ""
            
def _append_if_new(web_row: dict):
    """
    Append or replace an entry in edna_output.csv using the rule:

      Key: (School Name, District Name)

      - If any existing row for the key has Status == 'Open' (case-insensitive), DO NOT replace.
      - Else if existing rows are present and all have Status blank or 'Closed', REPLACE them
        with the scraped web_row (and record scraped Status).
      - Else if no rows for the key, APPEND scraped web_row.

    Also ensures the CSV has a 'Status' column; if absent, it is created with blanks.
    """

    append_cols = [
        "School Name",
        "School/Branch",
        "NCES Code",
        "Detail URL",
        "District Name",
        "District NCES",
        "NCES 12-digit (District+Branch)",
        "Status",
    ]
    # Ensure incoming row has all keys
    for c in append_cols:
        web_row.setdefault(c, "")

    # Canonicalize incoming Status
    web_row["Status"] = _status_canonicalize(web_row.get("Status", ""))

    # Ensure both files exist with headers
    _ensure_csv_with_headers(OUTPUT_CSV_FILENAME, append_cols)

    # Load primary (authoritative) file
    existing = pd.read_csv(OUTPUT_CSV_FILENAME, dtype=str).fillna("")
    
    # Ensure columns (including Status) are present even for legacy files
    for c in append_cols:
        if c not in existing.columns:
            existing[c] = ""

    # Build pair key
    in_school   = _normalize_key(web_row["School Name"])
    in_district = _normalize_key(web_row["District Name"])

    mask = (existing["School Name"].map(_normalize_key) == in_school) & \
           (existing["District Name"].map(_normalize_key) == in_district)
    matches = existing[mask]

    if not matches.empty:
        any_open = any(_status_norm(s) == "open" for s in matches["Status"].tolist())
        if any_open:
            print(f"[ONLINE] Skipped update for {web_row['School Name']} / {web_row['District Name']}: existing Status is Open")
            return

        # All existing Closed or blank -> replace with scraped row
        remaining = existing[~mask].copy()
        updated = pd.concat([remaining, pd.DataFrame([web_row])[append_cols]], ignore_index=True)

        # Write to BOTH files (authoritative + stream)
        updated.to_csv(OUTPUT_CSV_FILENAME, index=False)

        print(f"[ONLINE] Replaced Closed/blank row(s) for {web_row['School Name']} / {web_row['District Name']} in {OUTPUT_CSV_FILENAME}")
        return

    # No match -> append to BOTH files
    _append_row_to_csv(OUTPUT_CSV_FILENAME, append_cols, web_row)

    print(f"[ONLINE] Appended new row: {web_row['School Name']} / {web_row['District Name']} "
          f"to {OUTPUT_CSV_FILENAME}")

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _currentname_search_url(school_name: str) -> str:
    return CURRENTNAME_SEARCH_TEMPLATE.format(CURRENT=quote_plus((school_name or "").strip()))

def _parse_postback_href(href: str) -> Optional[Tuple[str, str]]:
    if not href: return None
    m = POSTBACK_RE.match(href.strip())
    if not m: return None
    return html.unescape(m.group(1)), html.unescape(m.group(2))

def _collect_form_fields(form_tag: Tag) -> dict:
    data = {}
    for inp in form_tag.find_all("input"):
        name = inp.get("name")
        if name: data[name] = inp.get("value", "")
    for ta in form_tag.find_all("textarea"):
        name = ta.get("name")
        if name: data[name] = ta.get_text()
    for sel in form_tag.find_all("select"):
        name = sel.get("name")
        if not name: continue
        val = None
        for opt in sel.find_all("option"):
            if "selected" in opt.attrs:
                val = opt.get("value", opt.get_text()); break
        if val is None:
            first = sel.find("option")
            val = first.get("value", first.get_text()) if first else ""
        data[name] = val
    return data

def _do_postback(session: requests.Session, page_url: str, target: str, argument: str) -> Optional[BeautifulSoup]:
    """
    Perform an ASP.NET __doPostBack and return a BeautifulSoup of the *rendered content*.
    Handles both full-page HTML and Microsoft AJAX UpdatePanel delta responses.
    """
    # 1) GET the page so we can collect VIEWSTATE / EVENTVALIDATION fields
    r = session.get(page_url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        print("[postback] no <form> on page to post back to")
        return None

    # 2) Build the POST payload with hidden fields + target/argument
    action = urljoin(page_url, form.get("action") or page_url)
    data = _collect_form_fields(form)
    data["__EVENTTARGET"] = target
    data["__EVENTARGUMENT"] = argument

    # 3) Post back. ASP.NET UpdatePanel often returns text/plain “delta” format.
    pr = session.post(
        action,
        data=data,
        headers={
            "Referer": page_url,
            # These two help some servers decide to return delta fragments
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    pr.raise_for_status()

    # 4) Try to detect and unwrap UpdatePanel delta payloads.
    #    Typical format: pipe-delimited tokens, with one or more HTML fragments embedded.
    text = pr.text or ""
    ctype = pr.headers.get("Content-Type", "").lower()
    if ("text/plain" in ctype or "application/json" in ctype or "|updatepanel|" in text.lower() or "|pageRedirect|" in text) and "|" in text:
        parts = text.split("|")
        # Pick candidate HTML fragments
        html_frags = [p for p in parts if "<" in p and ">" in p]
        # Prefer a fragment that contains a table, otherwise the longest
        pick = None
        for frag in html_frags:
            if "<table" in frag.lower():
                pick = frag
                break
        if not pick and html_frags:
            pick = max(html_frags, key=len)
        if pick:
            bs = BeautifulSoup(pick, "html.parser")
            print(f"[postback] delta payload detected: picked fragment len={len(pick)} tables={len(bs.find_all('table'))}", flush=True)
            return bs
        # Fallback: parse whole delta as HTML (often empty, but harmless)
        print("[postback] delta payload had no obvious HTML fragment; returning raw parse", flush=True)
        return BeautifulSoup(text, "html.parser")

    # 5) Normal full HTML response
    return BeautifulSoup(text, "html.parser")

def _find_results_table_and_institution_col(soup: BeautifulSoup) -> Tuple[Optional[Tag], Optional[int], Optional[int]]:
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            ths = tr.find_all(["th", "td"])
            if not ths: continue
            hdrs = [_normalize_space(th.get_text(" ", strip=True)) for th in ths]
            inst_idx = branch_idx = None
            for i, h in enumerate(hdrs):
                hl = h.lower()
                if hl == "institution name": inst_idx = i
                if hl == "school/branch": branch_idx = i
            if inst_idx is not None:
                return table, inst_idx, branch_idx
    return None, None, None

def _iter_institution_links(table: Tag, inst_col_idx: int, branch_col_idx: Optional[int]) -> List[Tuple[str, str, str]]:
    out = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        if tr.find("th"): continue
        tds = tr.find_all("td")
        if not tds or inst_col_idx >= len(tds): continue
        inst_cell = tds[inst_col_idx]
        a = inst_cell.find("a", href=True)
        if not a: continue
        href = a["href"].strip()
        name = _normalize_space(a.get_text(" ", strip=True))
        branch = ""
        if branch_col_idx is not None and branch_col_idx < len(tds):
            branch = _normalize_space(tds[branch_col_idx].get_text(" ", strip=True))
        out.append((name, branch, href))
    return out

def _find_table_with_header(soup: BeautifulSoup, header_name: str) -> Tuple[Optional[Tag], Optional[int], Optional[Tag]]:
    target = _normalize_space(header_name).lower()
    for table in soup.find_all("table"):
        for tr in table.find_all("tr", recursive=True):
            headers = tr.find_all("th") or tr.find_all("td")
            if not headers: 
                continue
            hdrs = [_normalize_space(h.get_text(" ", strip=True)) for h in headers]
            for i, h in enumerate(hdrs):
                if _normalize_space(h).lower() == target:
                    return table, i, tr
    return None, None, None

def _extract_cell_below_header(soup: BeautifulSoup, header: str) -> str:
    table, col_idx, header_row = _find_table_with_header(soup, header)
    if not table or col_idx is None: 
        return ""
    # prefer tbody rows; else scan rows after header within same table
    tbody = table.find("tbody")
    rows = []
    if tbody:
        rows = [tr for tr in tbody.find_all("tr") if tr.find_all("td")]
    else:
        for tr in header_row.find_all_next("tr"):
            if tr.find_parent("table") != table: 
                break
            if tr.find_all("td"): 
                rows.append(tr)
    if not rows: 
        return ""
    tds = rows[0].find_all("td")
    if col_idx < len(tds):
        return _normalize_space(tds[col_idx].get_text(" ", strip=True))
    return ""

# ---------- Generic key–value extraction from detail page ----------
def _extract_kv_from_all_tables(soup: BeautifulSoup) -> dict:
    """
    Build a label->value dict by scanning every table and interpreting each row
    as alternating label/value pairs: [Label, Value, Label, Value, ...].
    Keys are returned exactly as shown on the page.
    """
    kv = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            texts = [_normalize_space(c.get_text(" ", strip=True)) for c in cells]
            for i in range(0, len(texts) - 1, 2):
                label = texts[i]
                value = texts[i + 1]
                if label:
                    kv[label] = value
    return kv

# ---------- Rewritten getters using the kv-dict ----------
def _extract_nces_code_from_details(soup: BeautifulSoup) -> str:
    kv = _extract_kv_from_all_tables(soup)
    for label in ("NCES Code", "School NCES", "NCES School Code"):
        if kv.get(label):
            return _normalize_space(kv[label])
    return ""

def _school7_to_school5(school7: str) -> str:
    """
    Convert a 7-digit school NCES to the 5-digit 'SCH' component:
      * strip all non-digits
      * strip leading zeros until length ≤ 5
      * then left-pad with zeros to length 5
    If (after stripping non-digits) the string is empty, return ''.
    If the nonzero part still exceeds 5 (unexpected), take the last 5 and log a breadcrumb.
    """
    import re as _re
    digits = _re.sub(r"\D+", "", school7 or "")
    if not digits:
        return ""
    # Remove leading zeros; this implements “removing leading 0s … to go from n>5 like 7 to 5”
    trimmed = digits.lstrip("0")
    if trimmed == "":
        return "00000"
    if len(trimmed) <= 5:
        return trimmed.zfill(5)
    # Still >5 after removing leading zeros (unusual) → keep the last 5
    print(f"[nces][warn] school7 '{school7}' reduced to >5='{trimmed}', using last 5")
    return trimmed[-5:]

def _extract_school_nces7_from_details(soup: BeautifulSoup) -> str:
    """
    Return a 7-digit School NCES code if present; else ''.
    Strategy:
      1) KV-style labels that commonly carry the school NCES (Entity tab)
      2) Demographics header cell under 'NCES Code'
      3) Proximity regex: a 7-digit number within ±100 chars of 'NCES'
    Only a *7-digit* sequence is returned from here.
    """
    import re as _re

    def _first_7(s: str) -> str:
        if not s:
            return ""
        m = _re.search(r"\b\d{7}\b", s)
        return m.group(0) if m else ""

    # 1) KV-style scan
    kv = _extract_kv_from_all_tables(soup)
    for label in ("NCES Code", "School NCES", "NCES School Code"):
        cand = kv.get(label)
        n7 = _first_7(cand or "")
        if n7:
            return n7

    # 2) Demographics header fallback
    demog = _extract_cell_below_header(soup, "NCES Code")
    n7 = _first_7(demog or "")
    if n7:
        return n7

    # 3) Regex proximity to 'NCES'
    text = soup.get_text(" ", strip=True)
    for m in _re.finditer(r"\b\d{7}\b", text):
        start, end = m.start(), m.end()
        window = text[max(0, start-100):min(len(text), end+100)].lower()
        if "nces" in window:
            return m.group(0)

    return ""

def _extract_district_name_from_details(soup: BeautifulSoup) -> str:
    """
    District/LEA name: usually 'LEA Name' on the Entity tab.
    """
    kv = _extract_kv_from_all_tables(soup)
    for label in ("LEA Name", "District Name", "School District", "LEA"):
        if kv.get(label):
            return _normalize_space(kv[label])
    return ""

def _extract_branch_from_details(soup: BeautifulSoup) -> str:
    kv = _extract_kv_from_all_tables(soup)
    return _digits_only(kv.get("Branch", ""))

def _extract_district_nces_from_details(soup: BeautifulSoup) -> str:
    """
    District NCES on a **district page**:
      1) KV labels if present (rare)
      2) Demographics header table under 'NCES Code' (common)
    Returns digits-only string or ''.
    """
    # Try KV first (handles sites where 'District NCES' appears as a KV)
    kv = _extract_kv_from_all_tables(soup)
    for label in ("District NCES", "NCES District Code", "District NCES Code", "LEA NCES"):
        if kv.get(label):
            return _digits_only(kv[label])

    # Fallback: Demographics header table
    return _digits_only(_extract_cell_below_header(soup, "NCES Code"))

def _normalize_strict(s: str) -> str:
    """
    Normalize a string for strict comparison:
    - Convert to string
    - Lowercase
    - Collapse internal whitespace
    - Strip leading/trailing whitespace
    - Truncate to the first 40 characters
    """
    if not s:
        return ""
    s_norm = re.sub(r"\s+", " ", str(s)).strip().lower()
    return s_norm[:40]

def _district_equals(a: str, b: str) -> bool:
    """Require strict equality after normalization (with 40-char truncation)."""
    return _normalize_strict(a) == _normalize_strict(b)
    
def _search_currentname(session: requests.Session, current_name: str) -> Tuple[List[Tuple[str, str, str]], str]:
    """
    Perform an EDNA search with CurrentName=<current_name>.
    Returns (candidates, search_url) where candidates is a list of (inst_name, branch, href).
    """
    url = _currentname_search_url(current_name)
    r = session.get(url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table, inst_idx, branch_idx = _find_results_table_and_institution_col(soup)
    if not table or inst_idx is None:
        return [], url
    return _iter_institution_links(table, inst_idx, branch_idx), url

def _fetch_detail_soup(session: requests.Session, search_url: str, href: str) -> Tuple[Optional[BeautifulSoup], str]:
    """
    Follow either a direct link or an ASP.NET postback from a search results row
    and return (detail_soup, detail_url_for_csv).
    """
    if href.lower().startswith("javascript:"):
        parsed = _parse_postback_href(href)
        if not parsed:
            return None, ""
        target, argument = parsed
        soup = _do_postback(session, search_url, target, argument)
        return soup, ""
    else:
        detail_url = urljoin(EDNA_BASE, href)
        r = session.get(detail_url); r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser"), detail_url

def edna_lookup_district_by_name(session: requests.Session, lea_name: str, delay_sec: float = 0.6) -> Tuple[str, str]:
    candidates, search_url = _search_currentname(session, lea_name)
    if not candidates:
        print(f"[online-lookup][district] no candidates for '{lea_name}'")
        return "", ""

    target_norm = _normalize_strict(lea_name)
    for i, (inst_name, _branch, href) in enumerate(candidates, start=1):
        if _normalize_strict(inst_name) != target_norm:
            continue
        try:
            time.sleep(delay_sec)
            detail_soup, detail_url = _fetch_detail_soup(session, search_url, href)
        except Exception as e:
            print(f"[online-lookup][district] candidate #{i} fetch failed: {e}")
            continue
        if not detail_soup:
            continue

        # Use the **hybrid** extractor here:
        district_nces_digits = _extract_district_nces_from_details(detail_soup)
        if len(district_nces_digits) == 7:
            print(f"[online-lookup][district] accepted '{inst_name}' → NCES {district_nces_digits}")
            return district_nces_digits, detail_url

        print(f"[online-lookup][district] '{inst_name}' has no 7-digit NCES on page (saw: '{district_nces_digits}')")
    print(f"[online-lookup][district] no exact Institution Name match for '{lea_name}'")
    return "", ""

def edna_lookup_by_name(school_name: str, expected_district: str, delay_sec: float = 0.6) -> Optional[dict]:
    """
    Online lookup by school name with exact district match (strict compare via _district_equals).
    Returns a dict with fields aligned to edna_output.csv schema, or None if no verified match.

    Produces (if resolvable):
      - 'NCES Code'    := school7 (7-digit) if present; else ''
      - 'District NCES': district7 (7-digit) if resolved; else ''
      - 'NCES 12-digit (District+Branch)': district7 + school5 (derived from school7) if both parts present; else ''
      - 'School/Branch': branch code (string) if present; else ''
      - 'Detail URL'   : detail URL for the school, if direct-link path used; else ''
    IMPORTANT: Never use SchoolBranch/Location to compose the 12-digit.
    """
    school_name = norm(school_name)
    expected_district = norm(expected_district)
    if not school_name:
        return None

    session = _make_session()
    try:
        candidates, search_url = _search_currentname(session, school_name)
    except Exception as e:
        print(f"[online-lookup][name] fetch search failed for '{school_name}': {e}")
        traceback.print_exc()
        return None

    if not candidates:
        print(f"[online-lookup][name] no candidates for '{school_name}'")
        return None

    for i, (inst_name, branch, href) in enumerate(candidates, start=1):
        # Require Institution Name === school_name (strict-normalized)
        if _normalize_strict(inst_name) != _normalize_strict(school_name):
            continue

        try:
            time.sleep(delay_sec)
            detail_soup, detail_url_for_csv = _fetch_detail_soup(session, search_url, href)
        except Exception as e:
            print(f"[online-lookup][name] candidate #{i} detail fetch failed: {e}")
            traceback.print_exc()
            continue
        if not detail_soup:
            print(f"[online-lookup][name] candidate #{i} detail soup empty")
            continue

        # Extract school NCES (7-digit) and district name from the school page
        school7 = _extract_school_nces7_from_details(detail_soup) or ""
        district_name = _extract_district_name_from_details(detail_soup) or ""

        # District must match the expected district strictly
        if not _district_equals(district_name, expected_district):
            print(f"[online-lookup][name] skip candidate #{i}: district mismatch "
                  f"(saw '{district_name}' vs expected '{expected_district}')")
            continue

        # Resolve district NCES (7-digit) from the district (LEA) page
        district7, district_detail_url = edna_lookup_district_by_name(session, district_name, delay_sec)

        # Compose strictly from district7 + school5 (if both present)
        school5 = _school7_to_school5(school7) if school7 else ""
        nces12 = f"{district7}{school5}" if (len(district7) == 7 and len(school5) == 5) else ""

        if not (school7 or nces12):
            print(f"[online-lookup][name] skip candidate #{i} '{inst_name}': no usable NCES parts "
                  f"(district7='{district7 or '—'}', school7='{school7 or '—'}')")
            continue

        row = {
            "School Name": inst_name,
            "School/Branch": f'="{branch}"' if branch else "",
            "NCES Code": f'="{school7}"' if school7 else "",
            "Detail URL": detail_url_for_csv,
            "District Name": district_name,
            "District NCES": f'="{district7}"' if district7 else "",
            "NCES 12-digit (District+Branch)": f'="{nces12}"' if nces12 else "",
        }
        print(f"[ONLINE][name] accepted: {inst_name} "
              f"(school7: {school7 or '—'}, district7: {district7 or '—'}, 12-digit: {nces12 or '—'})")
        return row

    print(f"[online-lookup][name] no verified candidate for '{school_name}' in district '{expected_district}'")
    return None

def edna_lookup_by_location_id(location_id: str, delay_sec: float = 0.6) -> Optional[dict]:
    """
    Online lookup by SchoolBranch=<LOCATION_ID> (zero-padded to 4).
    Produces the same fields as the name-based path.
    IMPORTANT: Never use SchoolBranch/Location to compose the 12-digit.
    """
    import re as _re

    loc = _digits_only(location_id).zfill(4)
    if not loc or len(loc) != 4:
        return None

    session = _make_session()
    try:
        candidates, search_url = _search_schoolbranch(session, loc)
    except Exception as e:
        print(f"[online-lookup][location_id] fetch search failed for '{loc}': {e}")
        return None

    if not candidates:
        print(f"[online-lookup][location_id] no candidates for '{loc}'")
        return None

    for i, (inst_name, branch, href) in enumerate(candidates, start=1):
        try:
            time.sleep(delay_sec)
            detail_soup, detail_url_for_csv = _fetch_detail_soup(session, search_url, href)
        except Exception as e:
            print(f"[online-lookup][location_id] candidate #{i} detail fetch failed: {e}")
            continue
        if not detail_soup:
            print(f"[online-lookup][location_id] candidate #{i} detail soup empty")
            continue

        # Extract school7 and district name from the school page
        school7 = _extract_school_nces7_from_details(detail_soup) or ""
        district_name = _extract_district_name_from_details(detail_soup) or ""

        # Resolve district7 (LEA page)
        district7, district_detail_url = edna_lookup_district_by_name(session, district_name, delay_sec)

        # Compose strictly from district7 + school5 (if both present)
        school5 = _school7_to_school5(school7) if school7 else ""
        nces12 = f"{district7}{school5}" if (len(district7) == 7 and len(school5) == 5) else ""

        if not nces12:
            print(f"[online-lookup][location_id] skip candidate #{i} '{inst_name}': no usable NCES parts "
                  f"(district7='{district7 or '—'}', school7='{school7 or '—'}'; SchoolBranch={loc})")
            continue

        row = {
            "School Name": inst_name,
            "School/Branch": f'="{branch}"' if branch else "",
            "NCES Code": f'="{school7}"' if school7 else "",
            "Detail URL": detail_url_for_csv,
            "District Name": district_name,
            "District NCES": f'="{district7}"' if district7 else "",
            "NCES 12-digit (District+Branch)": f'="{nces12}"',
        }
        print(f"[ONLINE][location_id] accepted: {inst_name} "
              f"(school7: {school7 or '—'}, district7: {district7 or '—'}, 12-digit: {nces12})")
        return row

    print(f"[online-lookup][location_id] no verified candidate for SchoolBranch={loc}")
    return None

# ==============================
# Edna District pre-crawl: collect schools into edna_output.csv
# ==============================

def _debug_dump_html_snippet(soup: BeautifulSoup, tag: str = "schools"):
    try:
        txt = str(soup)
        print(f"[debug:{tag}] html head:\n{txt[:1500]}\n--- [truncated len={len(txt)}] ---", flush=True)
    except Exception:
        pass

def _ensure_csv_with_headers(path: str, cols: list[str]):
    """Create CSV with given columns if it doesn't exist."""
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)

def _append_row_to_csv(path: str, cols: list[str], row: dict):
    """Append a single row (dict) to CSV, creating file with headers if needed."""
    _ensure_csv_with_headers(path, cols)
    pd.DataFrame([row])[cols].to_csv(path, mode="a", index=False, header=False)

def _ensure_output_csv_exists():
    """
    Guarantee that edna_output.csv exists with the proper columns.
    Also create a live stream mirror at output_edna.csv.
    """
    cols = [
        "School Name",
        "School/Branch",
        "NCES Code",
        "Detail URL",
        "District Name",
        "District NCES",
        "NCES 12-digit (District+Branch)",
        "Status",
    ]
    _ensure_csv_with_headers(OUTPUT_CSV_FILENAME, cols)

def _collect_unique_district_names_from_workbook(xlsx_path: str, sheets: list[str]) -> list[str]:
    """
    Load the specified sheets and return a de-duplicated, normalized list of District Names.
    """
    districts = []
    for sheet in sheets:
        try:
            df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str).fillna("")
            df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)
            if "District Name" in df.columns:
                districts.extend([norm(x) for x in df["District Name"].tolist() if norm(x)])
            else:
                print(f"[district-prep] Sheet '{sheet}' missing 'District Name' column")
        except Exception as e:
            print(f"[district-prep] Failed reading sheet '{sheet}': {e}")
            traceback.print_exc()
    # de-dupe with case/space normalization consistent with strict compare
    seen = set()
    out = []
    for d in districts:
        k = _normalize_strict(d)
        if k and k not in seen:
            seen.add(k)
            out.append(d)
    return out

def _find_link_by_text(soup: BeautifulSoup, target_text: str) -> Optional[Tag]:
    """
    Find an <a> whose visible text matches target_text (case-insensitive, trimmed).
    """
    tnorm = _normalize_space(target_text).lower()
    for a in soup.find_all("a", href=True):
        if _normalize_space(a.get_text(" ", strip=True)).lower() == tnorm:
            return a
    return None

def _click_link_or_postback(session: requests.Session, page_url: str, a_tag: Tag) -> Optional[BeautifulSoup]:
    """
    Follow a standard href OR execute __doPostBack for javascript: links.
    After navigation, if the resulting page contains a single <iframe>, fetch its src and return that DOM instead.
    Returns BeautifulSoup of the best candidate DOM.
    """
    href = (a_tag.get("href") or "").strip()
    if not href:
        print("[nav] anchor missing href")
        return None

    def _fetch(url: str, ref: str = "") -> Optional[BeautifulSoup]:
        try:
            r = session.get(url, headers={"Referer": ref} if ref else None)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            print(f"[nav] GET {url} failed: {e}")
            traceback.print_exc()
            return None

    # 1) Navigate
    if href.lower().startswith("javascript:"):
        parsed = _parse_postback_href(href)
        if not parsed:
            print(f"[postback] could not parse postback href: {href[:120]}...")
            return None
        target, argument = parsed
        try:
            soup = _do_postback(session, page_url, target, argument)
        except Exception as e:
            print(f"[postback] error: {e}")
            traceback.print_exc()
            return None
        nav_url = page_url  # postbacks usually retain the same url
    else:
        nav_url = urljoin(EDNA_BASE, href)
        soup = _fetch(nav_url, ref=page_url)
        if not soup:
            return None

    # 2) If tab uses UpdatePanel, sometimes a second GET returns a fully rendered view
    if soup:
        # quick signal to debug what we got
        print(f"[trace] after click/postback: url≈{nav_url} len(html)={len(str(soup))} tables={len(soup.find_all('table'))}", flush=True)

    # 3) If content is inside an iframe, follow it
    try:
        iframes = soup.find_all("iframe")
        if len(iframes) == 1 and iframes[0].get("src"):
            iframe_src = urljoin(nav_url, iframes[0].get("src"))
            print(f"[trace] following iframe src: {iframe_src}", flush=True)
            soup_iframe = _fetch(iframe_src, ref=nav_url)
            if soup_iframe:
                print(f"[trace] iframe fetch len(html)={len(str(soup_iframe))} tables={len(soup_iframe.find_all('table'))}", flush=True)
                return soup_iframe
        elif len(iframes) > 1:
            # pick the first that mentions school/branch in its attributes
            for fr in iframes:
                src = fr.get("src", "")
                title = fr.get("title", "")
                txt = (src + " " + title).lower()
                if "school" in txt or "branch" in txt:
                    iframe_src = urljoin(nav_url, src)
                    print(f"[trace] following probable schools iframe src: {iframe_src}", flush=True)
                    soup_iframe = _fetch(iframe_src, ref=nav_url)
                    if soup_iframe:
                        print(f"[trace] iframe fetch len(html)={len(str(soup_iframe))} tables={len(soup_iframe.find_all('table'))}", flush=True)
                        return soup_iframe
    except Exception as e:
        print(f"[trace] iframe follow error: {e}")
        traceback.print_exc()

    # 4) Fallback: return whatever we got
    return soup

def _schools_table_parse(soup: BeautifulSoup) -> list[dict]:
    """
    Robust parser for the district 'Schools/Branches' listing.
    Heuristics:
      - Accept headers that LOOK LIKE institution+branch even if formatted oddly
      - Accept optional NCES and Status columns with varied names
      - If headers fail, fall back to "row-shape" detection: an anchor name + a 4-digit branch cell
    Returns a list of dicts: {name, branch, nces7, status, detail_href}
    """

    def canon(s: str) -> str:
        # normalize header/label text aggressively
        s = _normalize_space(s).lower()
        s = re.sub(r"[^a-z0-9]+", " ", s)
        return s.strip()

    def header_index_map(header_cells: list[str]) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """Return (inst_idx, branch_idx, nces_idx, status_idx) using flexible matching."""
        inst_idx = branch_idx = nces_idx = status_idx = None
        H = [canon(h) for h in header_cells]

        # Institution column: look for tokens like 'institution name', 'school name', 'name'
        for i, h in enumerate(H):
            if "institution" in h and "name" in h:
                inst_idx = i; break
        if inst_idx is None:
            for i, h in enumerate(H):
                if "school" in h and "name" in h:
                    inst_idx = i; break
        if inst_idx is None:
            for i, h in enumerate(H):
                if h == "name":
                    inst_idx = i; break

        # Branch column: variants like 'school/branch', 'branch', 'school branch', 'location id'
        for i, h in enumerate(H):
            if "school" in h and "branch" in h:
                branch_idx = i; break
        if branch_idx is None:
            for i, h in enumerate(H):
                if "branch" == h or h == "branch code" or h == "location id" or ("branch" in h and "code" in h):
                    branch_idx = i; break

        # NCES column (optional)
        for i, h in enumerate(H):
            if "nces" in h:
                nces_idx = i; break

        # Status column (optional)
        for i, h in enumerate(H):
            if "status" in h:
                status_idx = i; break

        return inst_idx, branch_idx, nces_idx, status_idx

    def extract_headers_from_table(table: Tag) -> list[str]:
        # try thead, else first row
        thead = table.find("thead")
        if thead:
            tr = thead.find("tr")
            if tr:
                return [_normalize_space(th.get_text(" ", strip=True)) for th in tr.find_all(["th", "td"])]
        tr = table.find("tr")
        if tr:
            return [_normalize_space(th.get_text(" ", strip=True)) for th in tr.find_all(["th", "td"])]
        return []

    results: list[dict] = []
    tables = soup.find_all("table")
    print(f"[trace] _schools_table_parse: found {len(tables)} table(s) on page", flush=True)

    # Pass 1: header-driven extraction
    for table in tables:
        headers = extract_headers_from_table(table)
        if not headers:
            continue
        inst_idx, branch_idx, nces_idx, status_idx = header_index_map(headers)
        if inst_idx is None or branch_idx is None:
            continue

        body = table.find("tbody") or table
        row_count = 0
        for tr in body.find_all("tr"):
            if tr.find("th"):
                continue
            tds = tr.find_all("td")
            if not tds:
                continue
            # Bounds check
            mx = max(inst_idx, branch_idx)
            if len(tds) <= mx:
                continue

            name_cell = tds[inst_idx]
            name = _normalize_space(name_cell.get_text(" ", strip=True))
            a = name_cell.find("a", href=True)
            href = a["href"].strip() if a else ""
            branch = _normalize_space(tds[branch_idx].get_text(" ", strip=True))

            nces7 = ""
            if nces_idx is not None and nces_idx < len(tds):
                nces7 = _digits_only(tds[nces_idx].get_text(" ", strip=True))

            status = ""
            if status_idx is not None and status_idx < len(tds):
                status = _status_canonicalize(tds[status_idx].get_text(" ", strip=True))

            if name and branch:
                results.append({
                    "name": name,
                    "branch": branch,
                    "nces7": nces7,
                    "status": status,
                    "detail_href": href,
                })
                row_count += 1

        if row_count:
            print(f"[trace] header-parse hit: headers={headers} rows={row_count}", flush=True)
            # Usually there is only one relevant table; stop after first hit
            return results

    # Pass 2: shape-driven fallback (no good headers found)
    # Heuristic: a row where one cell is a 4-digit branch and another cell has an <a> (name link)
    def looks_like_branch(s: str) -> bool:
        return bool(re.fullmatch(r"\d{4}", _digits_only(s).zfill(4)))

    for table in tables:
        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            link_cell = None
            branch_cell_text = ""
            # find any anchor and any 4-digit cell
            for td in tds:
                if (not link_cell) and td.find("a", href=True):
                    link_cell = td
                text = _normalize_space(td.get_text(" ", strip=True))
                if not branch_cell_text and looks_like_branch(text):
                    branch_cell_text = _digits_only(text).zfill(4)
            if link_cell and branch_cell_text:
                name = _normalize_space(link_cell.get_text(" ", strip=True))
                href = link_cell.find("a", href=True)["href"].strip()
                results.append({
                    "name": name,
                    "branch": branch_cell_text,
                    "nces7": "",
                    "status": "",
                    "detail_href": href,
                })

    if results:
        print(f"[trace] shape-parse hit: rows={len(results)} (no reliable headers)", flush=True)
    else:
        # Dump first 3 tables' headers for debugging
        hdr_summaries = []
        for t in tables[:3]:
            hdr = extract_headers_from_table(t)
            hdr_summaries.append(hdr)
        print(f"[trace] no rows parsed; sample headers={hdr_summaries}", flush=True)

    return results

def _paginate_next(session: requests.Session, current_url: str, current_soup: BeautifulSoup) -> Optional[tuple[BeautifulSoup, str]]:
    """
    If a 'Next' link exists on the page (typical ASP.NET pager), click it and return (soup, url_or_referrer).
    Returns None when there is no next page.
    """
    # Most EDNA grids show 'Next' exactly; be forgiving on whitespace/case.
    next_link = None
    for a in current_soup.find_all("a", href=True):
        txt = _normalize_space(a.get_text(" ", strip=True)).lower()
        if txt == "next" or txt == "next >" or txt == ">":
            next_link = a
            break
    if not next_link:
        return None

    soup2 = _click_link_or_postback(session, current_url, next_link)
    if not soup2:
        return None
    return soup2, current_url

def _open_schools_branches_direct(session: requests.Session,
                                  detail_soup: BeautifulSoup,
                                  referer_url: str = "") -> Optional[tuple[BeautifulSoup, str]]:
    """
    Open the 'Schools/Branches' page directly via /Screens/Details/wfSchools.aspx?ID=NNNNN.
    We find the numeric ID from the district detail soup (or referer URL),
    build the schools URL, GET it, and return (soup, referer_url_for_next).
    """
    # 1) If the page already exposes a direct schools link, use it first.
    for a in detail_soup.find_all("a", href=True):
        href = a["href"]
        if "wfSchools.aspx" in href and "ID=" in href:
            schools_url = urljoin(EDNA_BASE, href)
            try:
                r = session.get(schools_url, headers={"Referer": referer_url} if referer_url else None)
                r.raise_for_status()
                soup2 = BeautifulSoup(r.text, "html.parser")
                return soup2, schools_url
            except Exception as e:
                print(f"[district] direct schools link failed: {e}")
                traceback.print_exc()
                return None

    # 2) Otherwise, extract the entity id and construct the URL.
    entity_id = _extract_entity_id_from_soup(detail_soup, fallback_urls=[referer_url])
    if not entity_id:
        print("[district] could not find entity ID to build wfSchools.aspx link")
        return None

    schools_url = _build_schools_url(entity_id)
    if not schools_url:
        print("[district] failed to build wfSchools.aspx URL")
        return None

    try:
        r = session.get(schools_url, headers={"Referer": referer_url} if referer_url else None)
        r.raise_for_status()
        soup2 = BeautifulSoup(r.text, "html.parser")
        return soup2, schools_url
    except Exception as e:
        print(f"[district] GET schools page failed: {e}")
        traceback.print_exc()
        return None

def _append_school_row_from_listing(district_name: str, district7: str,
                                    listing_row: dict, session: requests.Session, referer_url: str):
    """
    Convert one listing row to our CSV schema and append/replace per rules.
    """
    school_name = listing_row.get("name", "")
    branch = listing_row.get("branch", "")
    school7 = listing_row.get("nces7", "")
    href = listing_row.get("detail_href", "")
    status = _status_canonicalize(listing_row.get("status", ""))  # ensure canonical

    school5 = _school7_to_school5(school7) if school7 else ""
    nces12 = f"{district7}{school5}" if (len(_digits_only(district7)) == 7 and len(school5) == 5) else ""

    detail_url = ""
    if href and not href.lower().startswith("javascript:"):
        detail_url = urljoin(EDNA_BASE, href)

    web_row = {
        "School Name": school_name,
        "School/Branch": f'="{branch}"' if branch else "",
        "NCES Code": f'="{school7}"' if school7 else "",
        "Detail URL": detail_url,
        "District Name": district_name,
        "District NCES": f'="{district7}"' if district7 else "",
        "NCES 12-digit (District+Branch)": f'="{nces12}"' if nces12 else "",
        "Status": status,  # NEW
    }

    try:
        _append_if_new(web_row)
    except Exception as e:
        print(f"[district-prepend] {_normalize_space(school_name)} append failed: {e}")
        traceback.print_exc()

def _extract_entity_id_from_soup(detail_soup: BeautifulSoup, fallback_urls: list[str] = None) -> str:
    """
    Try to recover the numeric ?ID=XXXXX for the current district.
    Strategy:
      1) Look for any <a> with href containing '?ID=digits'
      2) If not found, scan the page text (rare)
      3) If still not found, try any fallback URLs provided
    Returns digits-only string or ''.
    """
    # from anchors
    for a in detail_soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]ID=(\d+)", href, flags=re.IGNORECASE)
        if m:
            return m.group(1)

    # from text (edge case)
    txt = detail_soup.get_text(" ", strip=True)
    m = re.search(r"[?&]ID=(\d+)", txt, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # check fallback URLs
    if fallback_urls:
        for u in fallback_urls:
            if not u:
                continue
            m = re.search(r"[?&]ID=(\d+)", u, flags=re.IGNORECASE)
            if m:
                return m.group(1)

    return ""


def _build_schools_url(entity_id: str) -> str:
    """
    Construct the Schools/Branches URL from the numeric entity id.
    """
    eid = _digits_only(entity_id)
    if not eid:
        return ""
    return urljoin(EDNA_BASE, f"/Screens/Details/wfSchools.aspx?ID={eid}")

def prepopulate_edna_from_districts(xlsx_path: str, sheets: list[str], delay_sec: float = 0.6):
    """
    1) Gather unique district names from the workbook.
    2) For each district:
       - Search CurrentName=<district>
       - From results, click entries whose 'School/Branch' == '0000' (district rows)
       - On the district detail page, read District NCES (7-digit)
       - Open Schools/Branches directly via /Screens/Details/wfSchools.aspx?ID=...
       - Iterate all pages (Next)
       - Append each school listing into edna_output.csv if missing
    """
    districts = _collect_unique_district_names_from_workbook(xlsx_path, sheets)
    if not districts:
        print("[district-prep] No districts found in workbook; skipping prepopulation.")
        return

    session = _make_session()
    print(f"[district-prep] Starting EDNA prepopulation for {len(districts)} districts")

    for lea_name in tqdm(districts, desc="Pre-crawling districts"):
        if not lea_name:
            continue

        # Search by CurrentName
        try:
            candidates, search_url = _search_currentname(session, lea_name)
        except Exception as e:
            print(f"[district-prep] search failed for '{lea_name}': {e}")
            traceback.print_exc()
            continue

        if not candidates:
            print(f"[district-prep] no candidates for '{lea_name}'")
            continue

        # filter to district-level rows: School/Branch == 0000
        district_rows = [(nm, br, href) for (nm, br, href) in candidates if _normalize_space(br) == "0000"]
        if not district_rows:
            print(f"[district-prep] no '0000' district rows for '{lea_name}'")
            continue

        for (inst_name, _branch, href) in district_rows:
            # Fetch district detail page (to read NCES + recover the ID)
            try:
                time.sleep(delay_sec)
                detail_soup, detail_url = _fetch_detail_soup(session, search_url, href)
            except Exception as e:
                print(f"[district-prep] detail fetch failed for '{inst_name}': {e}")
                traceback.print_exc()
                continue
            if not detail_soup:
                print(f"[district-prep] empty detail soup for '{inst_name}'")
                continue

            # District NCES (needed to build 12-digit later)
            district7 = _extract_district_nces_from_details(detail_soup)
            if len(_digits_only(district7)) != 7:
                print(f"[district-prep] '{inst_name}' missing 7-digit District NCES (saw '{district7}')")
                continue

            # Open the Schools/Branches page directly using the ID
            tab = _open_schools_branches_direct(session, detail_soup, referer_url=(detail_url or search_url))
            if not tab:
                print(f"[district-prep] could not open Schools/Branches for '{inst_name}'")
                continue
            schools_soup, referer_url = tab

            # Iterate pages
            page_num = 1
            while True:
                # Parse listings from this page
                rows = _schools_table_parse(schools_soup)
                if not rows and page_num == 1:
                    # Quick debug dump if first page had nothing
                    try:
                        txt = str(schools_soup)
                        print(f"[debug:schools_tab] head:\n{txt[:1500]}\n--- [truncated len={len(txt)}] ---", flush=True)
                    except Exception:
                        pass

                for row in rows:
                    # ignore the district row itself (branch 0000)
                    if _normalize_space(row.get("branch", "")) == "0000":
                        continue
                    _append_school_row_from_listing(inst_name, _digits_only(district7), row, session, referer_url)

                # Next page?
                try:
                    nxt = _paginate_next(session, referer_url, schools_soup)
                except Exception as e:
                    print(f"[district-prep] pagination error on '{inst_name}' page {page_num}: {e}")
                    traceback.print_exc()
                    nxt = None

                if not nxt:
                    break
                schools_soup, referer_url = nxt
                page_num += 1
                time.sleep(delay_sec)

# ==============================
# Main
# ==============================
def main():
    try:
        # 1) Load lookup table from edna_output.csv (stable headers)
        # Ensure output file exists before reading
        _ensure_output_csv_exists()
        
        lookup = pd.read_csv(OUTPUT_CSV_FILENAME, dtype=str).fillna("")
        ensure_headers(
            lookup,
            ["School Name", "District Name", "NCES 12-digit (District+Branch)"],
            "output.csv"
        )
        # Build two maps: (School, District) -> 12-digit, and -> District 7-digit
        # Only index rows that actually have a 12-digit so lookups are actionable.
        rows12 = []
        rowsDist7 = []
        for _, r in lookup.iterrows():
            k = _pair_key(r.get("School Name", ""), r.get("District Name", ""))
            code12 = _digits_only(r.get("NCES 12-digit (District+Branch)", ""))
            dist7_csv = _digits_only(r.get("District NCES", ""))
            if code12:
                rows12.append((k, code12))
                # Prefer District NCES column if present; else derive from the 12-digit
                rowsDist7.append((k, dist7_csv if len(dist7_csv) == 7 else _derive_district7_from_12(code12)))

        pair_to_nces12 = dict(rows12)
        pair_to_dist7  = dict(rowsDist7)

        # 2) Open workbook (preserves formatting/validations)
        wb = load_workbook(CMP_FILENAME)

        # Prepopulate edna_output.csv with all schools by district ***
        try:
            prepopulate_edna_from_districts(CMP_FILENAME, CMP_SHEETS, delay_sec=0.6)
        except Exception as e:
            print(f"[district-prep] {e}")
            traceback.print_exc()

        # 3) Process each sheet
        for sheet in CMP_SHEETS:
            print(f"Processing sheet: {sheet}")

            # Read via pandas
            df = pd.read_excel(CMP_FILENAME, sheet_name=sheet, dtype=str).fillna("")
            df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)

            # Ensure required columns and initialize writeback columns if missing
            ensure_headers(df, ["School Name", "District Name"], f"{sheet}")
            if "School Number (NCES)" not in df.columns:
                df["School Number (NCES)"] = ""
            if "District Number (NCES)" not in df.columns:
                df["District Number (NCES)"] = ""

            # 4) Match each row
            for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Matching names in {sheet}"):
                school = row.get("School Name", "")
                district = row.get("District Name", "")
                if not norm(school) or not norm(district):
                    print(f"[WARN] Missing School or District at row {idx+2} in '{sheet}'")
                    continue

                key = _pair_key(school, district)

                # ---- Exact (local CSV) match on 12-digit
                nces12 = pair_to_nces12.get(key, "")
                if nces12:
                    df.at[idx, "School Number (NCES)"]   = nces12
                    # Prefer explicit district7 mapping; else derive from the 12-digit
                    dist7 = pair_to_dist7.get(key, "") or _derive_district7_from_12(nces12)
                    df.at[idx, "District Number (NCES)"] = dist7
                    continue

                # No exact match: log and try online
                print(f"[INFO] No exact match for row {idx+2} in '{sheet}': "
                      f"School='{norm(school)}', District='{norm(district)}'")

                # ---- Online lookup on EDNA (by school name; require exact district)
                web_row = edna_lookup_by_name(norm(school), norm(district), delay_sec=0.6)
                if web_row and (web_row.get("NCES 12-digit (District+Branch)") or web_row.get("NCES Code")):
                    code12 = _digits_only(web_row.get("NCES 12-digit (District+Branch)", "")) or _digits_only(web_row.get("NCES Code", ""))
                    dist7  = _digits_only(web_row.get("District NCES", "")) or _derive_district7_from_12(code12)
                    df.at[idx, "School Number (NCES)"]   = code12
                    df.at[idx, "District Number (NCES)"] = dist7
                    try:
                        _append_if_new(web_row)
                    except Exception as e:
                        print(f"[online-append/name] {e}")
                        traceback.print_exc()
                    continue

                # ---- Fallback by LOCATION_ID (EDNA SchoolBranch)
                loc_candidates = [
                    row.get("LOCATION_ID", ""),
                    row.get("Location ID", ""),
                    row.get("EDNA Location ID", ""),
                    row.get("SchoolBranch", ""),
                    row.get("School Branch", ""),
                    row.get("School/Branch", ""),
                ]
                loc_raw = next((str(x) for x in loc_candidates if norm(x)), "")
                loc_digits = _digits_only(loc_raw)
                if loc_digits:
                    print(f"[INFO] Attempting location_id lookup for row {idx+2} "
                          f"in '{sheet}' using SchoolBranch={loc_digits.zfill(4)}")
                    web_row_loc = edna_lookup_by_location_id(loc_digits, delay_sec=0.6)
                    if web_row_loc and (web_row_loc.get("NCES 12-digit (District+Branch)") or web_row_loc.get("NCES Code")):
                        code12 = _digits_only(web_row_loc.get("NCES 12-digit (District+Branch)", "")) or _digits_only(web_row_loc.get("NCES Code", ""))
                        dist7  = _digits_only(web_row_loc.get("District NCES", "")) or _derive_district7_from_12(code12)
                        df.at[idx, "School Number (NCES)"]   = code12
                        df.at[idx, "District Number (NCES)"] = dist7
                        try:
                            _append_if_new(web_row_loc)
                        except Exception as e:
                            print(f"[online-append/location_id] {e}")
                            traceback.print_exc()
                        continue

                # ---- Optional fuzzy fallback (kept as-is; only fills School Number)
                # NOTE: If enabled and used, we also derive District Number from the chosen 12-digit.
                if ENABLE_FUZZY_MATCH and pair_to_nces12:
                    # Fuzzy over keys we have in CSV
                    csv_keys = list(pair_to_nces12.keys())
                    best_match, score = _extract_one(key, csv_keys)
                    if best_match and score >= FUZZY_THRESHOLD:
                        code12 = pair_to_nces12.get(best_match, "")
                        df.at[idx, "School Number (NCES)"]   = code12
                        df.at[idx, "District Number (NCES)"] = pair_to_dist7.get(best_match, "") or _derive_district7_from_12(code12)
                        print(f"[FUZZY] Using fuzzy match (score {score:.1f}) → {best_match}")
                        continue

                print(f"[WARN] No match found for row {idx+2} in '{sheet}': "
                      f"School='{norm(school)}', District='{norm(district)}'")

            # 5) Write NCES back to THIS sheet now (both School and District)
            ws = wb[sheet]
            header_to_col = build_header_map(ws)
            if "School Number (NCES)" not in header_to_col:
                col_idx = len(header_to_col) + 1
                ws.cell(row=1, column=col_idx, value="School Number (NCES)")
                header_to_col["School Number (NCES)"] = col_idx
            if "District Number (NCES)" not in header_to_col:
                col_idx = len(header_to_col) + 1
                ws.cell(row=1, column=col_idx, value="District Number (NCES)")
                header_to_col["District Number (NCES)"] = col_idx

            nces_school_col   = header_to_col["School Number (NCES)"]
            nces_district_col = header_to_col["District Number (NCES)"]

            for r in tqdm(range(len(df)), total=len(df), desc=f"Writing NCES in {sheet}", leave=False):
                excel_row = r + 2
                ws.cell(row=excel_row, column=nces_school_col,   value=df.iloc[r]["School Number (NCES)"])
                ws.cell(row=excel_row, column=nces_district_col, value=df.iloc[r]["District Number (NCES)"])

        # 6) Save updated workbook once after all sheets are written
        out_name = "CMP Data Template (long format)_PA - Updated.xlsx"
        wb.save(out_name)
        print(f"File saved as '{out_name}'")

    except Exception as e:
        print(f"[main] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
