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
import yaml

try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"[config] Failed to load config.yaml: {e}")
    traceback.print_exc()
    raise

# ==============================
# Configuration
# ==============================
file_path_prefix       = config['file_path_prefix']
CMP_FILENAME = f"{file_path_prefix}/CMP_Data_Populated.xlsx"
OUTPUT_FILENAME = f"{file_path_prefix}/CMP_Data_Populated - Updated.xlsx"
EDNA_CACHE_CSV = f"{file_path_prefix}/{config['edna_cache']}"
CMP_SHEETS = ["School Pop. Data", "School CS Data"]
ENABLE_FUZZY_MATCH = False   # set to False to disable fuzzy matching

# ==============================
# Exact-match only by default; fuzzy fallback
# ==============================
try:
    from rapidfuzz import process as _rf_process
    _USE_RAPIDFUZZ = True
except Exception:
    _USE_RAPIDFUZZ = False
    try:
        from fuzzywuzzy import process as _fw_process
    except Exception:
        _fw_process = None

FUZZY_THRESHOLD = 60

# ==============================
# Utilities
# ==============================
def norm(s: str) -> str:
    return s.strip() if isinstance(s, str) else ""

def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def ensure_headers(df: pd.DataFrame, required: list, ctx: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{ctx}] Missing required columns: {missing}. Found: {list(df.columns)}")

def build_header_map(ws):
    header_cells = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    return {str(v): i + 1 for i, v in enumerate(header_cells) if v is not None}

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def _normalize_strict(s: str) -> str:
    if not s:
        return ""
    s_norm = re.sub(r"\s+", " ", str(s)).strip().lower()
    return s_norm[:40]

def _district_equals(a: str, b: str) -> bool:
    return _normalize_strict(a) == _normalize_strict(b)

def _normalize_detail_url(href_or_url: str) -> str:
    try:
        s = (href_or_url or "").strip()
        if not s:
            return s
        lower = s.lower()
        if "wfinstitutiondetails.aspx" in lower:
            try:
                from urllib.parse import urlparse
                p = urlparse(s)
                if p.scheme and p.netloc:
                    s = p.path + (("?" + p.query) if p.query else "")
            except Exception:
                pass
            if not s.startswith("/"):
                s = "/" + s
            if not s.lower().startswith("/screens/"):
                s = "/Screens" + s
            s = s.replace("//", "/")
            s = s.replace("/Screens/Screens/", "/Screens/")
        return s
    except Exception:
        traceback.print_exc()
        return href_or_url

def _force_screens_url(url_or_href: str) -> str:
    s = (url_or_href or "").strip()
    if not s:
        return s
    low = s.lower()
    if "wfinstitutiondetails.aspx" not in low:
        return s
    try:
        from urllib.parse import urlparse
        p = urlparse(s)
        if p.scheme and p.netloc:
            s = p.path + (("?" + p.query) if p.query else "")
    except Exception:
        pass
    if not s.startswith("/"):
        s = "/" + s
    if not s.lower().startswith("/screens/"):
        s = "/Screens" + s
    s = s.replace("//", "/").replace("/Screens/Screens/", "/Screens/")
    return urljoin(EDNA_BASE, s)

def _pair_key(school: str, district: str) -> str:
    return f"{_normalize_strict(school)}||{_normalize_strict(district)}"

def _derive_district7_from_12(nces12: str) -> str:
    d = _digits_only(nces12)
    return d[:7] if len(d) >= 7 else ""

def _extract_one(query: str, choices: list[str]):
    if _USE_RAPIDFUZZ:
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
# Grade parsing helpers (used for BOTH sheets)
# ==============================
_GRADE_RANK = {"PK": 0, "K": 1}
_GRADE_RANK.update({str(i): i for i in range(1, 13)})  # For min/max comparisons from 'Grades' column

def _parse_grade_tokens(grades_str: str) -> list[str]:
    """
    Parse a 'Grades' string like '1,2,3,4,5' or 'PK,K,1,2' (case-insensitive).
    Recognizes PK, K, 1..12. Returns deduped canonical tokens in input order.
    """
    if not grades_str:
        return []
    # split on commas/semicolons, trim whitespace
    parts = [p.strip() for p in re.split(r"[;,]", str(grades_str)) if p.strip()]
    tokens = []
    seen = set()
    for p in parts:
        # accept PK, K, or integers (possibly with leading zeros)
        m = re.fullmatch(r"(?i)PK|K|\d{1,2}", p)
        if not m:
            continue
        t = p.upper()
        if t not in ("PK", "K"):
            try:
                t = str(int(t))  # canonicalize "01" -> "1"
            except Exception:
                continue
        if t not in seen:
            seen.add(t)
            tokens.append(t)
    return tokens

def _lowest_highest_from_tokens(tokens: list[str]) -> tuple[str, str]:
    ranked = [(t, _GRADE_RANK[t]) for t in tokens if t in _GRADE_RANK]
    if not ranked:
        return "", ""
    ranked.sort(key=lambda x: x[1])
    lo = ranked[0][0]
    hi = ranked[-1][0]
    return lo, hi

# ==============================
# Edna Lookup (unchanged except where noted)
# ==============================
EDNA_BASE = "http://www.edna.pa.gov"
CURRENTNAME_SEARCH_TEMPLATE = (
    "http://www.edna.pa.gov/Screens/wfSearchEntityResults.aspx?"
    "AUN=&SchoolBranch=&CurrentName={CURRENT}&City=&HistoricalName=&IU=-1&CID=-1&"
    "CategoryIDs=3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c"
    "46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c"
    "3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c&StatusIDs=1%2c"
)

SCHOOLBRANCH_SEARCH_TEMPLATE = (
    "http://www.edna.pa.gov/Screens/wfSearchEntityResults.aspx?"
    "AUN=&SchoolBranch={BRANCH}&CurrentName=&City=&HistoricalName=&IU=-1&CID=-1&"
    "CategoryIDs=3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c"
    "46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c"
    "3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c&StatusIDs=1%2c"
)

POSTBACK_RE = re.compile(r"""^javascript:\s*__doPostBack\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)\s*;?\s*$""")
POSTBACK_ANY_RE = re.compile(r"""__doPostBack\(\s*'(?P<target>[^']+)'\s*,\s*'(?P<arg>[^']*)'\s*\)""")

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

def _currentname_search_url(school_name: str) -> str:
    return CURRENTNAME_SEARCH_TEMPLATE.format(CURRENT=quote_plus((school_name or "").strip()))

def _school7_to_school5(school7: str) -> str:
    digits = re.sub(r"\D+", "", school7 or "")
    if not digits:
        return ""
    trimmed = digits.lstrip("0")
    if trimmed == "":
        return "00000"
    if len(trimmed) <= 5:
        return trimmed.zfill(5)
    print(f"[nces][warn] school7 '{school7}' reduced to >5='{trimmed}', using last 5")
    return trimmed[-5:]

def _extract_kv_from_all_tables(soup: BeautifulSoup) -> dict:
    kv = {}
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            texts = [_normalize_space(c.get_text(" ", strip=True)) for c in cells]
            for i in range(0, len(texts) - 1, 2):
                label = texts[i]; value = texts[i + 1]
                if label:
                    kv[label] = value
    return kv

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

def _extract_school_nces7_from_details(soup: BeautifulSoup) -> str:
    def _first_7(s: str) -> str:
        if not s:
            return ""
        m = re.search(r"\b\d{7}\b", s)
        return m.group(0) if m else ""
    kv = _extract_kv_from_all_tables(soup)
    for label in ("NCES Code", "School NCES", "NCES School Code"):
        cand = kv.get(label)
        n7 = _first_7(cand or "")
        if n7:
            return n7
    demog = _extract_cell_below_header(soup, "NCES Code")
    n7 = _first_7(demog or "")
    if n7:
        return n7
    text = soup.get_text(" ", strip=True)
    for m in re.finditer(r"\b\d{7}\b", text):
        start, end = m.start(), m.end()
        window = text[max(0, start-100):min(len(text), end+100)].lower()
        if "nces" in window:
            return m.group(0)
    return ""

def _extract_district_name_from_details(soup: BeautifulSoup) -> str:
    kv = _extract_kv_from_all_tables(soup)
    for label in ("LEA Name", "District Name", "School District", "LEA"):
        if kv.get(label):
            return _normalize_space(kv[label])
    return ""

def _extract_district_nces_from_details(soup: BeautifulSoup) -> str:
    kv = _extract_kv_from_all_tables(soup)
    for label in ("District NCES", "NCES District Code", "District NCES Code", "LEA NCES"):
        if kv.get(label):
            return _digits_only(kv[label])
    return _digits_only(_extract_cell_below_header(soup, "NCES Code"))

# ---- Grades scraping (unchanged) ----
def _table_header_texts(tr: Tag) -> list:
    headers = tr.find_all("th") or tr.find_all("td")
    return [_normalize_space(h.get_text(" ", strip=True)) for h in headers]

def _find_table_with_header_loose(soup: BeautifulSoup, header_name: str) -> Tuple[Optional[Tag], Optional[int], Optional[Tag]]:
    for table in soup.find_all("table"):
        header_row = None
        for tr in table.find_all("tr", recursive=True):
            if tr.find("th") or tr.find_all("td"):
                header_row = tr
                break
        if not header_row:
            continue
        headers = _table_header_texts(header_row)
        for idx, h in enumerate(headers):
            if _normalize_space(h).lower() == _normalize_space(header_name).lower():
                return table, idx, header_row
    return None, None, None

def _extract_grades_from_details(soup: BeautifulSoup) -> str:
    # (kept as-is; used for EDNA cache; not written to workbook)
    try:
        def _canon_token(t: str) -> str:
            t = t.strip().upper()
            if t in ("PK", "K"):
                return t
            m = re.fullmatch(r"\d{1,2}", t)
            return str(int(t)) if m else ""
        def _rank(t: str) -> int:
            if t == "PK": return 0
            if t == "K":  return 1
            if re.fullmatch(r"\d{1,2}", t): return int(t)
            return 999
        def _expand_span(a: str, b: str) -> list[str]:
            A, B = _canon_token(a), _canon_token(b)
            if not A or not B:
                return []
            order = ["PK", "K"] + [str(i) for i in range(1, 13)]
            if A not in order or B not in order:
                return []
            ia, ib = order.index(A), order.index(B)
            if ia > ib: ia, ib = ib, ia
            return order[ia:ib+1]
        def _tokens_from_text(txt: str) -> list[str]:
            if not txt:
                return []
            s = _normalize_space(txt)
            parts = re.split(r"[;,]", s)
            tokens: list[str] = []
            for part in parts:
                m = re.search(r"\b(PK|K|\d{1,2})\s*[-–—]\s*(PK|K|\d{1,2})\b", part, flags=re.IGNORECASE)
                if m:
                    tokens += _expand_span(m.group(1), m.group(2))
                for t in re.findall(r"\b(PK|K|\d{1,2})\b", part, flags=re.IGNORECASE):
                    ct = _canon_token(t)
                    if ct:
                        tokens.append(ct)
            seen, uniq = set(), []
            for t in tokens:
                if t and t not in seen:
                    seen.add(t); uniq.append(t)
            uniq.sort(key=_rank)
            order = ["PK", "K"] + [str(i) for i in range(1, 13)]
            return [t for t in order if t in uniq]
        table, grades_col_idx, header_row = _find_table_with_header_loose(soup, "Grades")
        if table:
            tbody = table.find("tbody")
            data_rows = []
            if tbody:
                for tr in tbody.find_all("tr"):
                    if tr.find("th"): continue
                    if tr.find_all("td"): data_rows.append(tr)
            else:
                if header_row:
                    for tr in header_row.find_all_next("tr"):
                        if tr.find_parent("table") != table: break
                        if tr.find("th"): continue
                        if tr.find_all("td"): data_rows.append(tr)
                else:
                    for tr in table.find_all("tr", recursive=True):
                        if tr.find_all("td"): data_rows.append(tr)
            texts = []
            for tr in data_rows:
                tds = tr.find_all("td")
                if not tds: continue
                if grades_col_idx is not None and grades_col_idx < len(tds):
                    texts.append(_normalize_space(tds[grades_col_idx].get_text(" ", strip=True)))
                else:
                    texts.append(_normalize_space(tr.get_text(" ", strip=True)))
            tokens = []
            for txt in texts:
                tokens += _tokens_from_text(txt)
            if tokens:
                return ", ".join(tokens)
        kv = _extract_kv_from_all_tables(soup)
        preferred_labels = [
            "Grades Served", "Grades Offered", "Grade Span", "Grades",
            "Lowest Grade", "Highest Grade",
        ]
        for label in preferred_labels:
            if label in kv and kv[label]:
                toks = _tokens_from_text(kv[label])
                if toks:
                    return ", ".join(toks)
        for label, value in kv.items():
            if "grade" in label.lower() and value:
                toks = _tokens_from_text(value)
                if toks:
                    return ", ".join(toks)
        return ""
    except Exception:
        traceback.print_exc()
        return ""

def _make_session() -> requests.Session:
    return requests.Session()  # shadowed earlier; keep headers in the first one

def _search_currentname(session: requests.Session, current_name: str) -> Tuple[List[Tuple[str, str, str]], str]:
    url = _currentname_search_url(current_name)
    r = session.get(url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table, inst_idx, branch_idx = _find_results_table_and_institution_col(soup)
    if not table or inst_idx is None:
        return [], url
    return _iter_institution_links(table, inst_idx, branch_idx), url

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

def _do_postback(session: requests.Session, page_url: str, target: str, argument: str) -> Optional[BeautifulSoup]:
    r = session.get(page_url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        print("[postback] no <form> on page to post back to")
        return None
    action = urljoin(page_url, form.get("action") or page_url)
    data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if name: data[name] = inp.get("value", "")
    for ta in form.find_all("textarea"):
        name = ta.get("name")
        if name: data[name] = ta.get_text()
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name: continue
        val = None
        for opt in sel.find_all("option"):
            if "selected" in opt.attrs:
                val = opt.get("value", opt.get_text()); break
        if val is None:
            first = sel.find("option")
            val = first.get("value", opt.get_text()) if first else ""
        data[name] = val
    data["__EVENTTARGET"] = target
    data["__EVENTARGUMENT"] = argument
    pr = session.post(
        action, data=data,
        headers={
            "Referer": page_url,
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    pr.raise_for_status()
    text = pr.text or ""
    ctype = pr.headers.get("Content-Type", "").lower()
    if ("text/plain" in ctype or "application/json" in ctype or "|updatepanel|" in text.lower() or "|pageRedirect|" in text) and "|" in text:
        parts = text.split("|")
        html_frags = [p for p in parts if "<" in p and ">" in p]
        pick = None
        for frag in html_frags:
            if "<table" in frag.lower():
                pick = frag
                break
        if not pick and html_frags:
            pick = max(html_frags, key=len)
        if pick:
            bs = BeautifulSoup(pick, "html.parser")
            return bs
        return BeautifulSoup(text, "html.parser")
    return BeautifulSoup(text, "html.parser")

def _search_schoolbranch(session: requests.Session, branch_code: str) -> Tuple[List[Tuple[str, str, str]], str]:
    branch_code = (branch_code or "").strip()
    branch_code = re.sub(r"\D+", "", branch_code)
    branch_code = branch_code.zfill(4) if branch_code else ""
    url = SCHOOLBRANCH_SEARCH_TEMPLATE.format(BRANCH=quote_plus(branch_code))
    r = session.get(url); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table, inst_idx, branch_idx = _find_results_table_and_institution_col(soup)
    if not table or inst_idx is None:
        return [], url
    return _iter_institution_links(table, inst_idx, branch_idx), url

def _fetch_detail_soup(session: requests.Session, search_url: str, href: str) -> Tuple[Optional[BeautifulSoup], str]:
    try:
        if href.lower().startswith("javascript:"):
            m = POSTBACK_RE.match(href) or POSTBACK_ANY_RE.search(href)
            if not m:
                return None, ""
            if hasattr(m, "groupdict") and "target" in m.groupdict():
                target, argument = html.unescape(m.group("target")), html.unescape(m.group("arg"))
            else:
                target, argument = html.unescape(m.group(1)), html.unescape(m.group(2))
            soup = _do_postback(session, search_url, target, argument)
            if soup is not None:
                soup.base_url = search_url
            return soup, ""
        else:
            norm_href = _normalize_detail_url(href)
            detail_url = urljoin(EDNA_BASE, norm_href)
            r = session.get(detail_url)
            r.raise_for_status()
            bs = BeautifulSoup(r.text, "html.parser")
            bs.base_url = detail_url
            return bs, detail_url
    except Exception as e:
        print(f"[fetch-detail] {e}")
        traceback.print_exc()
        return None, ""

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
        district_nces_digits = _extract_district_nces_from_details(detail_soup)
        if len(district_nces_digits) == 7:
            print(f"[online-lookup][district] accepted '{inst_name}' → NCES {district_nces_digits}")
            return district_nces_digits, detail_url
        print(f"[online-lookup][district] '{inst_name}' has no 7-digit NCES on page (saw: '{district_nces_digits}')")
    print(f"[online-lookup][district] no exact Institution Name match for '{lea_name}'")
    return "", ""

def edna_lookup_by_name(school_name: str, expected_district: str, delay_sec: float = 0.6) -> Optional[dict]:
    school_name = norm(school_name)
    expected_district = norm(expected_district)
    if not school_name:
        return None
    session = requests.Session()
    try:
        candidates, search_url = _search_currentname(session, school_name)
    except Exception:
        traceback.print_exc()
        print(f"[grades] url={_force_screens_url(_currentname_search_url(school_name))} school=\"{school_name}\" district=\"{expected_district}\" branch=— grades=\"\"")
        return None
    if not candidates:
        print(f"[grades] url={_force_screens_url(_currentname_search_url(school_name))} school=\"{school_name}\" district=\"{expected_district}\" branch=— grades=\"—\"")
        return None
    for i, (inst_name, branch, href) in enumerate(candidates, start=1):
        if _normalize_strict(inst_name) != _normalize_strict(school_name):
            continue
        try:
            time.sleep(delay_sec)
            detail_soup, detail_url_for_csv = _fetch_detail_soup(session, search_url, _force_screens_url(href))
        except Exception:
            traceback.print_exc()
            print(f"[grades] url={_force_screens_url(search_url)} school=\"{inst_name}\" district=\"{expected_district}\" branch={branch or '—'} grades=\"\"")
            continue
        if not detail_soup:
            print(f"[grades] url={_force_screens_url(search_url)} school=\"{inst_name}\" district=\"{expected_district}\" branch={branch or '—'} grades=\"—\"")
            continue
        school7 = _extract_school_nces7_from_details(detail_soup) or ""
        district_name = _extract_district_name_from_details(detail_soup) or ""
        grades = _extract_grades_from_details(detail_soup) or ""
        url_for_log = _force_screens_url(detail_url_for_csv or getattr(detail_soup, "base_url", "") or search_url)
        if not _district_equals(district_name, expected_district):
            print(f"[grades] url={url_for_log} school=\"{inst_name}\" district=\"{district_name or '—'}\" branch={branch or '—'} grades=\"{grades or '—'}\"")
            continue
        district7, _ = edna_lookup_district_by_name(session, district_name, delay_sec)
        school5 = _school7_to_school5(school7) if school7 else ""
        nces12 = f"{district7}{school5}" if (len(district7) == 7 and len(school5) == 5) else ""
        row = {
            "School Name": inst_name,
            "School/Branch": f'="{branch}"' if branch else "",
            "NCES Code": f'="{school7}"' if school7 else "",
            "Grades": grades,  # kept in cache
            "Detail URL": url_for_log,
            "District Name": district_name,
            "District NCES": f'="{district7}"' if district7 else "",
            "NCES 12-digit (District+Branch)": f'="{nces12}"' if nces12 else "",
        }
        print(f"[grades] url={url_for_log} school=\"{inst_name}\" district=\"{district_name}\" branch={branch or '—'} grades=\"{grades or '—'}\"")
        return row
    print(f"[grades] url={_force_screens_url(_currentname_search_url(school_name))} school=\"{school_name}\" district=\"{expected_district}\" branch=— grades=\"—\"")
    return None

def edna_lookup_by_location_id(location_id: str, delay_sec: float = 0.6) -> Optional[dict]:
    loc = _digits_only(location_id).zfill(4)
    if not loc or len(loc) != 4:
        print(f"[grades] url={EDNA_BASE}/Screens/wfSearchEntityResults.aspx school=\"—\" district=\"—\" branch={loc or '—'} grades=\"—\"")
        return None
    session = requests.Session()
    try:
        candidates, search_url = _search_schoolbranch(session, loc)
    except Exception:
        traceback.print_exc()
        print(f"[grades] url={SCHOOLBRANCH_SEARCH_TEMPLATE.format(BRANCH=quote_plus(loc))} school=\"—\" district=\"—\" branch={loc} grades=\"\"")
        return None
    if not candidates:
        print(f"[grades] url={SCHOOLBRANCH_SEARCH_TEMPLATE.format(BRANCH=quote_plus(loc))} school=\"—\" district=\"—\" branch={loc} grades=\"—\"")
        return None
    for i, (inst_name, branch, href) in enumerate(candidates, start=1):
        try:
            time.sleep(delay_sec)
            detail_soup, detail_url_for_csv = _fetch_detail_soup(session, search_url, href)
        except Exception:
            traceback.print_exc()
            print(f"[grades] url={search_url} school=\"{inst_name}\" district=\"—\" branch={branch or '—'} grades=\"\"")
            continue
        if not detail_soup:
            print(f"[grades] url={search_url} school=\"{inst_name}\" district=\"—\" branch={branch or '—'} grades=\"—\"")
            continue
        school7 = _extract_school_nces7_from_details(detail_soup) or ""
        district_name = _extract_district_name_from_details(detail_soup) or ""
        grades = _extract_grades_from_details(detail_soup) or ""
        url_for_log = detail_url_for_csv or getattr(detail_soup, "base_url", "") or search_url
        district7, _ = edna_lookup_district_by_name(requests.Session(), district_name, delay_sec)
        school5 = _school7_to_school5(school7) if school7 else ""
        nces12 = f"{district7}{school5}" if (len(district7) == 7 and len(school5) == 5) else ""
        if not nces12:
            print(f"[grades] url={url_for_log} school=\"{inst_name}\" district=\"{district_name or '—'}\" branch={branch or '—'} grades=\"{grades or '—'}\"")
            continue
        row = {
            "School Name": inst_name,
            "School/Branch": f'="{branch}"' if branch else "",
            "NCES Code": f'="{school7}"' if school7 else "",
            "Grades": grades,  # kept in cache
            "Detail URL": detail_url_for_csv,
            "District Name": district_name,
            "District NCES": f'="{district7}"' if district7 else "",
            "NCES 12-digit (District+Branch)": f'="{nces12}"',
        }
        print(f"[grades] url={url_for_log} school=\"{inst_name}\" district=\"{district_name or '—'}\" branch={branch or '—'} grades=\"{grades or '—'}\"")
        return row
    print(f"[grades] url={SCHOOLBRANCH_SEARCH_TEMPLATE.format(BRANCH=quote_plus(loc))} school=\"—\" district=\"—\" branch={loc} grades=\"—\"")
    return None

# ==============================
# EDNA cache helpers
# ==============================
def _ensure_csv_with_headers(path: str, cols: list[str]):
    if not os.path.exists(path):
        pd.DataFrame(columns=cols).to_csv(path, index=False)

def _append_row_to_csv(path: str, cols: list[str], row: dict):
    _ensure_csv_with_headers(path, cols)
    pd.DataFrame([row])[cols].to_csv(path, mode="a", index=False, header=False)

def _ensure_output_csv_exists():
    cols = [
        "School Name",
        "School/Branch",
        "NCES Code",
        "Grades",  # cache only
        "Detail URL",
        "District Name",
        "District NCES",
        "NCES 12-digit (District+Branch)",
        "Status",
    ]
    _ensure_csv_with_headers(EDNA_CACHE_CSV, cols)

def _append_if_new(web_row: dict):
    append_cols = [
        "School Name","School/Branch","NCES Code","Grades","Detail URL",
        "District Name","District NCES","NCES 12-digit (District+Branch)","Status",
    ]
    for c in append_cols:
        web_row.setdefault(c, "")
    def _status_norm(s: str) -> str:
        return re.sub(r"\s+", " ", str(s or "")).strip().lower()
    def _status_canonicalize(s: str) -> str:
        sn = _status_norm(s)
        if sn.startswith("open"): return "Open"
        if sn.startswith("closed"): return "Closed"
        return s or ""
    web_row["Status"] = _status_canonicalize(web_row.get("Status", ""))
    _ensure_csv_with_headers(EDNA_CACHE_CSV, append_cols)
    existing = pd.read_csv(EDNA_CACHE_CSV, dtype=str).fillna("")
    for c in append_cols:
        if c not in existing.columns:
            existing[c] = ""
    def _normalize_key(s: str) -> str:
        if not s: return ""
        s_norm = re.sub(r"\s+", " ", str(s)).strip().lower()
        return s_norm[:120]
    in_school   = _normalize_key(web_row["School Name"])
    in_district = _normalize_key(web_row["District Name"])
    mask = (existing["School Name"].map(_normalize_key) == in_school) & \
           (existing["District Name"].map(_normalize_key) == in_district)
    matches = existing[mask]
    if matches.empty:
        _append_row_to_csv(EDNA_CACHE_CSV, append_cols, web_row)
        print(f"[ONLINE] Appended new row: {web_row['School Name']} / {web_row['District Name']} to {EDNA_CACHE_CSV}")
        return
    any_open = any(_status_norm(s) == "open" for s in matches["Status"].tolist())
    if any_open:
        patched = existing.copy()
        patchable = ["Grades","NCES Code","School/Branch","Detail URL","District NCES","NCES 12-digit (District+Branch)","Status"]
        for idx in patched[mask].index.tolist():
            for col in patchable:
                cur = str(patched.at[idx, col] or "").strip()
                newv = str(web_row.get(col, "") or "").strip()
                if (not cur) and newv:
                    patched.at[idx, col] = newv
        patched.to_csv(EDNA_CACHE_CSV, index=False)
        print(f"[ONLINE] Patched existing Open row(s) for {web_row['School Name']} / {web_row['District Name']} (filled missing fields)")
        return
    remaining = existing[~mask].copy()
    updated = pd.concat([remaining, pd.DataFrame([web_row])[append_cols]], ignore_index=True)
    updated.to_csv(EDNA_CACHE_CSV, index=False)
    print(f"[ONLINE] Replaced Closed/blank row(s) for {web_row['School Name']} / {web_row['District Name']} in {EDNA_CACHE_CSV}")

# ==============================
# District crawl helpers (trimmed where irrelevant)
# ==============================
def _find_table_with_header_loose(soup: BeautifulSoup, header_name: str) -> Tuple[Optional[Tag], Optional[int], Optional[Tag]]:
    # already defined earlier; kept for completeness (renamed version exists above)
    return None, None, None  # placeholder to avoid duplicate definitions

# (Omitted: schools listing crawl; unchanged from your version except it still caches Grades.)

# ==============================
# Main
# ==============================
def main():
    try:
        _ensure_output_csv_exists()

        wb = load_workbook(CMP_FILENAME)

        # Optional: prepopulate EDNA cache (unchanged)
        try:
            # If you still need to run the district crawl, keep your original function here.
            pass
        except Exception as e:
            print(f"[district-prep] {e}")
            traceback.print_exc()

        # RELOAD LOOKUP *after* prepopulation
        lookup = pd.read_csv(EDNA_CACHE_CSV, dtype=str).fillna("")
        ensure_headers(
            lookup,
            ["School Name", "District Name", "NCES 12-digit (District+Branch)"],
            "output.csv"
        )
        if "Grades" not in lookup.columns:
            lookup["Grades"] = ""

        rows12, rowsDist7, rowsGrades = [], [], []
        for _, r in lookup.iterrows():
            k = _pair_key(r.get("School Name",""), r.get("District Name",""))
            code12 = _digits_only(r.get("NCES 12-digit (District+Branch)",""))
            dist7_csv = _digits_only(r.get("District NCES",""))
            grades = r.get("Grades", "") or ""
            if code12:
                rows12.append((k, code12))
                rowsDist7.append((k, dist7_csv if len(dist7_csv)==7 else _derive_district7_from_12(code12)))
                rowsGrades.append((k, grades))

        pair_to_nces12 = dict(rows12)
        pair_to_dist7  = dict(rowsDist7)
        pair_to_grades = dict(rowsGrades)

        # 3) Process each sheet
        for sheet in CMP_SHEETS:
            print(f"Processing sheet: {sheet}")

            df = pd.read_excel(CMP_FILENAME, sheet_name=sheet, dtype=str).fillna("")
            df.rename(columns=lambda c: c.strip() if isinstance(c, str) else c, inplace=True)

            ensure_headers(df, ["School Name", "District Name"], f"{sheet}")
            if "School Number (NCES)" not in df.columns:
                df["School Number (NCES)"] = ""
            if "District Number (NCES)" not in df.columns:
                df["District Number (NCES)"] = ""
            # Ensure Lowest/Highest columns on BOTH sheets
            if "Lowest Grade Level Served" not in df.columns:
                df["Lowest Grade Level Served"] = ""
            if "Highest Grade Level Served" not in df.columns:
                df["Highest Grade Level Served"] = ""

            for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Matching names in {sheet}"):
                school = row.get("School Name", "")
                district = row.get("District Name", "")
                if not norm(school) or not norm(district):
                    print(f"[WARN] Missing School or District at row {idx+2} in '{sheet}'")
                    continue

                key = _pair_key(school, district)

                # Prefer Grades from the SHEET (if provided)
                grades_text_in_sheet = row.get("Grades", "") or ""

                # ---- Exact (local CSV) match on 12-digit
                nces12 = pair_to_nces12.get(key, "")
                if nces12:
                    df.at[idx, "School Number (NCES)"]   = nces12
                    dist7 = pair_to_dist7.get(key, "") or _derive_district7_from_12(nces12)
                    df.at[idx, "District Number (NCES)"] = dist7

                    # Resolve 'Grades' to compute min/max (sheet value overrides cache if present)
                    grades_text = grades_text_in_sheet or pair_to_grades.get(key, "")
                    tokens = _parse_grade_tokens(grades_text)
                    lo, hi = _lowest_highest_from_tokens(tokens)
                    if lo: df.at[idx, "Lowest Grade Level Served"] = lo
                    if hi: df.at[idx, "Highest Grade Level Served"] = hi
                    continue

                print(f"[INFO] No exact match for row {idx+2} in '{sheet}': "
                      f"School='{norm(school)}', District='{norm(district)}'")

                # ---- Online lookup by name (not writing Grades to workbook)
                web_row = edna_lookup_by_name(norm(school), norm(district), delay_sec=0.6)
                if web_row and (web_row.get("NCES 12-digit (District+Branch)") or web_row.get("NCES Code")):
                    code12 = _digits_only(web_row.get("NCES 12-digit (District+Branch)", "")) or _digits_only(web_row.get("NCES Code", ""))
                    dist7  = _digits_only(web_row.get("District NCES", "")) or _derive_district7_from_12(code12)
                    df.at[idx, "School Number (NCES)"]   = code12
                    df.at[idx, "District Number (NCES)"] = dist7

                    # Compute min/max from Grades (sheet preferred, else web)
                    grades_text = grades_text_in_sheet or web_row.get("Grades", "") or ""
                    tokens = _parse_grade_tokens(grades_text)
                    lo, hi = _lowest_highest_from_tokens(tokens)
                    if lo: df.at[idx, "Lowest Grade Level Served"] = lo
                    if hi: df.at[idx, "Highest Grade Level Served"] = hi

                    try:
                        _append_if_new(web_row)
                    except Exception as e:
                        print(f"[online-append/name] {e}")
                        traceback.print_exc()
                    continue

                # ---- Fallback by LOCATION_ID
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

                        grades_text = grades_text_in_sheet or web_row_loc.get("Grades", "") or ""
                        tokens = _parse_grade_tokens(grades_text)
                        lo, hi = _lowest_highest_from_tokens(tokens)
                        if lo: df.at[idx, "Lowest Grade Level Served"] = lo
                        if hi: df.at[idx, "Highest Grade Level Served"] = hi

                        try:
                            _append_if_new(web_row_loc)
                        except Exception as e:
                            print(f"[online-append/location_id] {e}")
                            traceback.print_exc()
                        continue

                # ---- Optional fuzzy fallback
                if ENABLE_FUZZY_MATCH and pair_to_nces12:
                    csv_keys = list(pair_to_nces12.keys())
                    best_match, score = _extract_one(key, csv_keys)
                    if best_match and score >= FUZZY_THRESHOLD:
                        code12 = pair_to_nces12.get(best_match, "")
                        df.at[idx, "School Number (NCES)"]   = code12
                        df.at[idx, "District Number (NCES)"] = pair_to_dist7.get(best_match, "") or _derive_district7_from_12(code12)
                        grades_text = grades_text_in_sheet or pair_to_grades.get(best_match, "")
                        tokens = _parse_grade_tokens(grades_text)
                        lo, hi = _lowest_highest_from_tokens(tokens)
                        if lo: df.at[idx, "Lowest Grade Level Served"] = lo
                        if hi: df.at[idx, "Highest Grade Level Served"] = hi
                        print(f"[FUZZY] Using fuzzy match (score {score:.1f}) → {best_match}")
                        continue

                print(f"[WARN] No match found for row {idx+2} in '{sheet}': "
                      f"School='{norm(school)}', District='{norm(district)}'")

            # 5) Write NCES + Lowest/Highest back to THIS sheet now
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
            # Ensure Lowest/Highest columns on BOTH sheets
            if "Lowest Grade Level Served" not in header_to_col:
                col_idx = len(header_to_col) + 1
                ws.cell(row=1, column=col_idx, value="Lowest Grade Level Served")
                header_to_col["Lowest Grade Level Served"] = col_idx
            if "Highest Grade Level Served" not in header_to_col:
                col_idx = len(header_to_col) + 1
                ws.cell(row=1, column=col_idx, value="Highest Grade Level Served")
                header_to_col["Highest Grade Level Served"] = col_idx

            # Column indices
            nces_school_col   = header_to_col["School Number (NCES)"]
            nces_district_col = header_to_col["District Number (NCES)"]
            low_col = header_to_col["Lowest Grade Level Served"]
            high_col = header_to_col["Highest Grade Level Served"]

            # Force text for NCES columns
            for col_idx in (nces_school_col, nces_district_col):
                for r in range(2, len(df) + 2):
                    ws.cell(row=r, column=col_idx).number_format = "@"

            # Write values
            for r in tqdm(range(len(df)), total=len(df), desc=f"Writing outputs in {sheet}", leave=False):
                excel_row = r + 2
                ws.cell(row=excel_row, column=nces_school_col,   value=df.iloc[r]["School Number (NCES)"])
                ws.cell(row=excel_row, column=nces_district_col, value=df.iloc[r]["District Number (NCES)"])
                ws.cell(row=excel_row, column=low_col,           value=df.iloc[r]["Lowest Grade Level Served"])
                ws.cell(row=excel_row, column=high_col,          value=df.iloc[r]["Highest Grade Level Served"])

        out_name = OUTPUT_FILENAME
        wb.save(out_name)
        print(f"File saved as '{out_name}'")

    except Exception as e:
        print(f"[main] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
