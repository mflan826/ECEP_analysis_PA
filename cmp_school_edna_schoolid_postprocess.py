import traceback
import pandas as pd
from openpyxl import load_workbook
from tqdm import tqdm
import re, time, html
from typing import Optional, Tuple, List
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup, Tag

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

def ensure_headers(df: pd.DataFrame, required: list, ctx: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{ctx}] Missing required columns: {missing}. Found: {list(df.columns)}")

def build_header_map(ws):
    """Return dict: header_text -> 1-based column index, from the first row in a worksheet."""
    header_cells = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    return {str(v): i + 1 for i, v in enumerate(header_cells) if v is not None}

def make_pair_key(school: str, district: str) -> str:
    """
    Build a normalized composite key for (School Name, District Name).
    """
    return f"{norm(school)}||{norm(district)}"
    
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

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

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
    r = session.get(page_url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form: return None
    action = urljoin(page_url, form.get("action") or page_url)
    data = _collect_form_fields(form)
    data["__EVENTTARGET"] = target
    data["__EVENTARGUMENT"] = argument
    pr = session.post(action, data=data, headers={"Referer": page_url})
    pr.raise_for_status()
    return BeautifulSoup(pr.text, "html.parser")

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
    Online lookup by school name with exact district match.
    Produces:
      - 'NCES Code'    := school7 (7-digit) if present; else ''
      - 'District NCES': district7 (7-digit) if resolved; else ''
      - 'NCES 12-digit (District+Branch)': district7 + school5 (derived from school7) if both parts are present; else ''
    IMPORTANT: Never use SchoolBranch/Location to compose the 12-digit.
    """
    import re as _re

    school_name = norm(school_name)
    expected_district = norm(expected_district)
    if not school_name:
        return None

    session = _make_session()
    try:
        candidates, search_url = _search_currentname(session, school_name)
    except Exception as e:
        print(f"[online-lookup] fetch search failed for '{school_name}': {e}")
        return None

    if not candidates:
        print(f"[online-lookup] no candidates for '{school_name}'")
        return None

    for i, (inst_name, branch, href) in enumerate(candidates, start=1):
        try:
            time.sleep(delay_sec)
            detail_soup, detail_url_for_csv = _fetch_detail_soup(session, search_url, href)
        except Exception as e:
            print(f"[online-lookup] candidate #{i} detail fetch failed: {e}")

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
# Main
# ==============================
def main():
    try:
        # 1) Load lookup table from edna_output.csv (stable headers)
        lookup = pd.read_csv(OUTPUT_CSV_FILENAME, dtype=str).fillna("")
        ensure_headers(
            lookup,
            ["School Name", "District Name", "NCES 12-digit (District+Branch)"],
            "output.csv"
        )

        # Composite (School, District) → NCES 12-digit map for exact and fuzzy
        def _make_pair_key(s, d): 
            return f"{norm(s)}||{norm(d)}"

        pair_to_nces12 = dict(
            zip(
                (_make_pair_key(r["School Name"], r["District Name"]) for _, r in lookup.iterrows()),
                lookup["NCES 12-digit (District+Branch)"].astype(str).map(norm)
            )
        )
        csv_composite_keys = list(pair_to_nces12.keys())

        # 2) Open workbook (preserves formatting/validations)
        wb = load_workbook(CMP_FILENAME)

        # Helper to dedupe-append a discovered web_row to edna_output.csv
        def _append_if_new(web_row: dict):
            append_cols = [
                "School Name",
                "School/Branch",
                "NCES Code",
                "Detail URL",
                "District Name",
                "District NCES",
                "NCES 12-digit (District+Branch)",
            ]
            for c in append_cols:
                web_row.setdefault(c, "")
            existing = pd.read_csv(OUTPUT_CSV_FILENAME, dtype=str).fillna("")
            key_incoming = (
                norm(web_row["School Name"]),
                norm(web_row["District Name"]),
                norm(web_row.get("NCES 12-digit (District+Branch)")) or norm(web_row.get("NCES Code"))
            )
            def _row_key(r):
                return (
                    norm(r.get("School Name", "")),
                    norm(r.get("District Name", "")),
                    norm(r.get("NCES 12-digit (District+Branch)", "")) or norm(r.get("NCES Code", ""))
                )
            is_dup = any(_row_key(r) == key_incoming for _, r in existing.iterrows())
            if not is_dup:
                pd.DataFrame([web_row])[append_cols].to_csv(
                    OUTPUT_CSV_FILENAME, mode="a", index=False, header=False
                )
                print(f"[ONLINE] Appended new row to {OUTPUT_CSV_FILENAME}: "
                      f"{web_row['School Name']} / {web_row['District Name']}")
            else:
                print(f"[ONLINE] Skipped append; entry already exists for "
                      f"{web_row['School Name']} / {web_row['District Name']}")

        # 3) Process each sheet
        for sheet in CMP_SHEETS:
            print(f"Processing sheet: {sheet}")

            # Read via pandas
            df = pd.read_excel(CMP_FILENAME, sheet_name=sheet, dtype=str).fillna("")
            df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)

            # Only the fields needed for lookup/writeback
            ensure_headers(df, ["School Name", "District Name"], f"{sheet}")
            if "School Number (NCES)" not in df.columns:
                df["School Number (NCES)"] = ""

            # 4) Match each row
            for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Matching names in {sheet}"):
                school = norm(row.get("School Name", ""))
                district = norm(row.get("District Name", ""))

                if not school or not district:
                    print(f"[WARN] Missing School or District at row {idx+2} in '{sheet}'")
                    continue

                key = f"{school}||{district}"

                # ---- Exact (local CSV) match on 12-digit
                nces12 = pair_to_nces12.get(key, "")
                if nces12:
                    df.at[idx, "School Number (NCES)"] = nces12
                    continue

                # No exact match: log and try online
                print(f"[INFO] No exact match for row {idx+2} in '{sheet}': "
                      f"School='{school}', District='{district}'")

                # ---- Online lookup on EDNA (by school name; require exact district)
                web_row = edna_lookup_by_name(school, district, delay_sec=0.6)
                if web_row and (web_row.get("NCES 12-digit (District+Branch)") or web_row.get("NCES Code")):
                    chosen_nces = norm(web_row.get("NCES 12-digit (District+Branch)", ""))
                    df.at[idx, "School Number (NCES)"] = chosen_nces
                    try:
                        _append_if_new(web_row)
                    except Exception as e:
                        print(f"[online-append/name] {e}")
                        traceback.print_exc()
                    continue

                # ---- NEW: Online fallback by LOCATION_ID (EDNA SchoolBranch)
                # Look for a plausible location_id in common columns
                loc_candidates = [
                    row.get("LOCATION_ID", ""),
                    row.get("Location ID", ""),
                    row.get("EDNA Location ID", ""),
                    row.get("SchoolBranch", ""),
                    row.get("School Branch", ""),
                    row.get("School/Branch", ""),
                ]
                loc_raw = next((str(x) for x in loc_candidates if norm(x)), "")
                loc_digits = re.sub(r"\D+", "", loc_raw or "")
                if loc_digits:
                    print(f"[INFO] Attempting location_id lookup for row {idx+2} "
                          f"in '{sheet}' using SchoolBranch={loc_digits.zfill(4)}")                    
                    web_row_loc = edna_lookup_by_location_id(loc_digits, delay_sec=0.6)
                    if web_row_loc and (web_row_loc.get("NCES 12-digit (District+Branch)") or web_row_loc.get("NCES Code")):
                        chosen_nces = (
                            norm(web_row_loc.get("NCES 12-digit (District+Branch)", "")) or
                            norm(web_row_loc.get("NCES Code", ""))
                        )
                        df.at[idx, "School Number (NCES)"] = chosen_nces
                        try:
                            _append_if_new(web_row_loc)
                        except Exception as e:
                            print(f"[online-append/location_id] {e}")
                            traceback.print_exc()
                        continue

                # ---- Fuzzy fallback (only if all online lookups failed)
                if ENABLE_FUZZY_MATCH and csv_composite_keys:
                    best_match, score = _extract_one(key, csv_composite_keys)
                    if best_match and score >= FUZZY_THRESHOLD:
                        df.at[idx, "School Number (NCES)"] = pair_to_nces12.get(best_match, "")
                        print(f"[FUZZY] Using fuzzy match (score {score:.1f}) → {best_match}")
                        continue

                # If here, every option failed
                print(f"[WARN] No match found for row {idx+2} in '{sheet}': "
                      f"School='{school}', District='{district}'")

        # 5) Write NCES back via openpyxl
        ws = wb[sheet]
        header_to_col = build_header_map(ws)
        if "School Number (NCES)" not in header_to_col:
            col_idx = len(header_to_col) + 1
            ws.cell(row=1, column=col_idx, value="School Number (NCES)")
            header_to_col["School Number (NCES)"] = col_idx
        nces_col_idx = header_to_col["School Number (NCES)"]

        for r in tqdm(range(len(df)), total=len(df), desc=f"Writing NCES in {sheet}", leave=False):
            excel_row = r + 2
            ws.cell(row=excel_row, column=nces_col_idx, value=df.iloc[r]["School Number (NCES)"])

        # 6) Save updated workbook
        out_name = "CMP Data Template (long format)_PA - Updated.xlsx"
        wb.save(out_name)
        print(f"File saved as '{out_name}'")

    except Exception as e:
        print(f"[main] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
