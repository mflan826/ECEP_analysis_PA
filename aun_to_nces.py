#!/usr/bin/env python3
"""
Scrape EDNA PA search results for links in the 'Institution Name' column and,
for each linked details page, extract the NCES Code from the Demographics table.

Outputs CSV to stdout: School Name,School/Branch,NCES Code,Detail URL
"""

import sys
import time
import csv
import re
import traceback
from typing import Optional, Tuple
from urllib.parse import urljoin, quote_plus
import requests
from bs4 import BeautifulSoup, NavigableString, Tag
import argparse
from pathlib import Path
import html
from urllib.parse import urlparse
from tqdm.auto import tqdm
import os

# Build per-AUN (+ SchoolBranch) search URLs
SEARCH_URL_TEMPLATE = (
    "http://www.edna.pa.gov/Screens/wfSearchEntityResults.aspx?"
    "AUN={AUN}&SchoolBranch={SCHOOLBRANCH}&CurrentName=&City=&HistoricalName=&IU=-1&CID=-1&"
    "CategoryIDs=3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c"
    "3%2c4%2c2%2c1%2c6%2c7%2c29%2c27%2c28%2c55%2c30%2c31%2c14%2c11%2c12%2c9%2c17%2c15%2c18%2c46%2c47%2c40%2c34%2c56%2c45%2c36%2c44%2c35%2c38%2c59%2c999%2c32%2c33%2c37%2c49%2c57%2c52%2c22%2c20%2c19%2c58%2c53%2c&StatusIDs=1%2c"
)

def build_search_url(aun: str, school_branch: Optional[str] = None) -> str:
    """
    Build the EDNA search URL for a given AUN and optional SchoolBranch filter.
    If school_branch is None/empty, the parameter is left blank (broad search).
    """
    aun_clean = str(aun).strip()
    sb_clean = (school_branch or "").strip()
    sb_encoded = quote_plus(sb_clean)  # spaces -> '+', etc.
    return SEARCH_URL_TEMPLATE.format(AUN=aun_clean, SCHOOLBRANCH=sb_encoded)

BASE_URL = "http://www.edna.pa.gov"

# --- HTTP setup ---------------------------------------------------------------

POSTBACK_RE = re.compile(r"""^javascript:\s*__doPostBack\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)\s*;?\s*$""")

def parse_postback_href(href: str) -> Optional[Tuple[str, str]]:
    """
    If href looks like javascript:__doPostBack('target','argument'), return (target, argument),
    otherwise return None.
    """
    if not href:
        return None
    m = POSTBACK_RE.match(href.strip())
    if not m:
        return None
    # Unescape in case the attributes were HTML-encoded into the href
    target = html.unescape(m.group(1))
    argument = html.unescape(m.group(2))
    return target, argument

def _collect_form_fields(form_tag: Tag) -> dict:
    """
    Collect all input/select/textarea fields from a form into a dict: {name: value}
    Includes hidden ASP.NET fields like __VIEWSTATE, __EVENTVALIDATION, etc.
    """
    data = {}
    # inputs
    for inp in form_tag.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        # Use 'value' (empty string if missing)
        data[name] = inp.get("value", "")
    # textareas
    for ta in form_tag.find_all("textarea"):
        name = ta.get("name")
        if not name:
            continue
        data[name] = ta.get_text()
    # selects (choose selected option, or first if none selected)
    for sel in form_tag.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        sel_val = None
        for opt in sel.find_all("option"):
            if "selected" in opt.attrs:
                sel_val = opt.get("value", opt.get_text())
                break
        if sel_val is None:
            first = sel.find("option")
            if first:
                sel_val = first.get("value", first.get_text())
            else:
                sel_val = ""
        data[name] = sel_val
    return data

def do_postback(session: requests.Session, search_url: str, target: str, argument: str) -> Optional[BeautifulSoup]:
    """
    Perform an ASP.NET __doPostBack to the search_url page using the specified target/argument.
    Returns a BeautifulSoup of the response (often the detail page) or None on failure.
    """
    try:
        # Always GET the search page afresh to ensure current __VIEWSTATE & friends
        resp = session.get(search_url)
        resp.raise_for_status()
    except Exception as e:
        print(f"[postback-get] {e}", file=sys.stderr)
        traceback.print_exc()
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    # Find the main form (ASP.NET usually uses a single <form> for the page)
    form = soup.find("form")
    if not form:
        print("[postback] Could not find a <form> on the page.", file=sys.stderr)
        return None

    action = form.get("action") or search_url
    # Resolve relative action against search_url
    action = urljoin(search_url, action)

    data = _collect_form_fields(form)
    # Overwrite the critical postback fields
    data["__EVENTTARGET"] = target
    data["__EVENTARGUMENT"] = argument

    # If the page has validation groups or specific buttons, leaving their values intact is fine.
    # We do not remove anything; ASP.NET ignores irrelevant fields.

    try:
        post = session.post(action, data=data, headers={"Referer": search_url})
        post.raise_for_status()
        return BeautifulSoup(post.text, "html.parser")
    except Exception as e:
        print(f"[postback-post] {action} :: {e}", file=sys.stderr)
        traceback.print_exc()
        return None

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "close",
    })
    s.request = _wrap_request_with_timeout(s.request, timeout=(10, 30))
    return s

def _wrap_request_with_timeout(orig_func, timeout):
    def wrapped(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return orig_func(method, url, **kwargs)
    return wrapped

# --- Parsing helpers ----------------------------------------------------------

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def find_results_table_and_institution_col(soup: BeautifulSoup) -> Tuple[Optional[BeautifulSoup], Optional[int], Optional[int]]:
    """
    Return (table, institution_col_index, branch_col_index) where indices are zero-based
    for headers 'Institution Name' and 'School/Branch' (case-insensitive, normalized).
    If the table or the 'Institution Name' column is not found, return (None, None, None).
    'School/Branch' may be None if not present.
    """
    tables = soup.find_all("table")
    target_table = None
    inst_idx = None
    branch_idx = None

    for t in tables:
        header_rows = t.find_all("tr")
        for tr in header_rows:
            ths = tr.find_all(["th", "td"])
            if not ths:
                continue
            hdr_texts = [normalize_space(th.get_text(" ", strip=True)) for th in ths]
            # Try to locate both columns in the same row
            local_inst_idx = None
            local_branch_idx = None
            for i, h in enumerate(hdr_texts):
                hl = h.lower()
                if hl == "institution name":
                    local_inst_idx = i
                if hl == "school/branch":
                    local_branch_idx = i
            if local_inst_idx is not None:
                target_table = t
                inst_idx = local_inst_idx
                branch_idx = local_branch_idx  # may be None
                break
        if target_table is not None:
            break

    if target_table is None:
        return None, None, None
    return target_table, inst_idx, branch_idx

def iter_institution_links(table: BeautifulSoup, inst_col_idx: int, base_url: str, branch_col_idx: Optional[int] = None):
    """
    Yield (school_name, school_branch, absolute_detail_url) from the results table.

    - school_name is taken from the 'Institution Name' column's <a>.
    - school_branch is taken from the 'School/Branch' column text if available; otherwise "".
    """
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds or inst_col_idx >= len(tds):
            continue

        # Institution name and link
        inst_cell = tds[inst_col_idx]
        a = inst_cell.find("a", href=True)
        if not a:
            continue

        href = a["href"].strip()
        name = normalize_space(a.get_text(" ", strip=True))
        if not href or not name:
            continue

        # School/Branch (optional)
        branch = ""
        if branch_col_idx is not None and branch_col_idx < len(tds):
            branch = normalize_space(tds[branch_col_idx].get_text(" ", strip=True))

        yield name, branch, urljoin(base_url, href)

# --- Field extraction: table column version -----------------------------------

def _table_header_texts(tr: Tag) -> list:
    # Prefer THs; fall back to TDs if headers are rendered with TD
    headers = tr.find_all("th")
    if not headers:
        headers = tr.find_all("td")
    return [normalize_space(h.get_text(" ", strip=True)) for h in headers]

def _find_table_with_header(soup: BeautifulSoup, header_name: str) -> Tuple[Optional[Tag], Optional[int], Optional[Tag]]:
    """
    Search all tables for one whose header row contains `header_name` (case-insensitive).
    Returns (table, column_index, header_row). If not found, (None, None, None).
    """
    for table in soup.find_all("table"):
        # Identify a plausible header row: first TR that has TH (or TDs used as headers)
        header_row = None
        for tr in table.find_all("tr", recursive=True):
            if tr.find("th") or tr.find_all("td"):
                header_row = tr
                break
        if not header_row:
            continue

        headers = _table_header_texts(header_row)
        for idx, h in enumerate(headers):
            if normalize_space(h).lower() == normalize_space(header_name).lower():
                return table, idx, header_row
    return None, None, None

def get_district_info(aun: str, base_url: str = BASE_URL, delay_sec: float = 0.6) -> Tuple[str, str]:
    """
    For a given AUN, fetch the district (SchoolBranch=0000) and return (district_name, district_nces_raw).
    Caches should be used at the call site (by AUN). Returns ("","") on failure.
    """
    district_name = ""
    district_nces = ""
    search_url = build_search_url(aun, "0000")
    session = make_session()

    try:
        resp = session.get(search_url)
        resp.raise_for_status()
    except Exception as e:
        print(f"[district-get AUN={aun}] {e}", file=sys.stderr)
        traceback.print_exc()
        return district_name, district_nces

    soup = BeautifulSoup(resp.text, "html.parser")
    table, inst_col_idx, _ = find_results_table_and_institution_col(soup)
    if not table or inst_col_idx is None:
        print(f"[district-parse AUN={aun}] could not find results table", file=sys.stderr)
        return district_name, district_nces

    # Take the first data row (district row)
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds or inst_col_idx >= len(tds):
            continue

        inst_cell = tds[inst_col_idx]
        a = inst_cell.find("a", href=True)
        if not a:
            continue

        raw_href = a["href"].strip()
        district_name = normalize_space(a.get_text(" ", strip=True))
        if not raw_href or not district_name:
            break

        # politeness delay and then visit the details page to read NCES (raw)
        try:
            time.sleep(delay_sec)
            if not raw_href.lower().startswith("javascript:"):
                detail_url = urljoin(base_url, raw_href)
                dr = session.get(detail_url)
                dr.raise_for_status()
                dsoup = BeautifulSoup(dr.text, "html.parser")
                district_nces = extract_nces_code_from_details(dsoup) or ""
            else:
                parsed = parse_postback_href(raw_href)
                if parsed:
                    target, argument = parsed
                    dsoup = do_postback(session, search_url, target, argument)
                    if dsoup is not None:
                        district_nces = extract_nces_code_from_details(dsoup) or ""
                else:
                    print(f"[district-postback-parse AUN={aun}] failed for href", file=sys.stderr)
        except Exception as e:
            print(f"[district-details AUN={aun}] {e}", file=sys.stderr)
            traceback.print_exc()

        break  # only the first district row
    return district_name, district_nces

def build_full_nces_code(district_nces: str, school_branch: str) -> str:
    """
    Construct a 12-digit NCES code as:
        <7-digit district NCES><5-digit branch>
    We strip all non-digits first to be robust to formats like '="4202100"'.
    Returns Excel-friendly quoted string or "" if inputs are invalid.
    """
    dn_digits = re.sub(r"\D", "", (district_nces or ""))
    sb_digits = re.sub(r"\D", "", (school_branch or ""))

    # District must be exactly 7 digits; branch must have at least 1 digit
    if len(dn_digits) != 7 or len(sb_digits) == 0:
        return ""

    full = dn_digits + sb_digits.zfill(5)
    if len(full) != 12:
        return ""
    return f'="{full}"'
    
def attach_district_columns(rows: list, district_name: str, district_nces_raw: str) -> list:
    """
    Append district name, district NCES, and a derived 12-digit NCES (district+branch).
    """
    out = []
    district_nces_for_csv = f'="{district_nces_raw}"' if district_nces_raw else ""
    for r in rows:
        # r: [school_name, school_branch, school_nces, detail_url]
        school_branch = r[1]
        full_12digit = build_full_nces_code(district_nces_raw, school_branch)
        out.append(r + [district_name, district_nces_for_csv, full_12digit])
    return out

def extract_nces_code_from_details(soup: BeautifulSoup) -> str:
    """
    Extract NCES Code by locating the table that contains a column labeled 'NCES Code'
    (e.g., under the 'Demographics' section), then reading the first data-row cell under
    that column.
    """
    table, nces_col_idx, header_row = _find_table_with_header(soup, "NCES Code")
    if not table or nces_col_idx is None:
        return ""

    # Find the first data row AFTER the header_row within the same table
    # Prefer <tbody> rows if present; otherwise, iterate siblings after header_row.
    tbody = table.find("tbody")
    data_rows = []
    if tbody:
        for tr in tbody.find_all("tr"):
            # skip any header-like rows that might appear in tbody
            if tr.find("th"):
                continue
            tds = tr.find_all("td")
            if tds:
                data_rows.append(tr)
    else:
        # Walk following TR siblings after the header row
        for tr in header_row.find_all_next("tr"):
            # stop if we've exited the current table
            if tr.find_parent("table") != table:
                break
            if tr.find("th"):
                continue
            tds = tr.find_all("td")
            if tds:
                data_rows.append(tr)

    if not data_rows:
        return ""

    first = data_rows[0]
    tds = first.find_all("td")
    if nces_col_idx < len(tds):
        return normalize_space(tds[nces_col_idx].get_text(" ", strip=True))

    return ""

def apply_school_branch(rows: list, school_branch_value: str) -> list:
    """
    Given rows shaped like [School Name, School/Branch, NCES Code, Detail URL],
    overwrite the School/Branch column with the provided value.
    """
    out = []
    for r in rows:
        # Defensive: accept either list or tuple
        name = r[0] if len(r) > 0 else ""
        nces = r[2] if len(r) > 2 else ""
        detail = r[3] if len(r) > 3 else ""
        out.append([name, school_branch_value, nces, detail])
    return out

# --- Main workflow ------------------------------------------------------------

def scrape(search_url: str, requested_branch: Optional[str] = None, base_url: str = BASE_URL, delay_sec: float = 0.6):
    """
    Return rows from a single EDNA results page:
      [School Name(from results page), School/Branch(from results page), NCES Code(from details), Detail URL]

    If requested_branch is provided, only include rows whose 'School/Branch' cell
    exactly matches requested_branch (when the column exists).
    """
    rows = []
    session = make_session()

    try:
        resp = session.get(search_url)
        resp.raise_for_status()
    except Exception as e:
        print(f"[fetch-search] {e}", file=sys.stderr)
        traceback.print_exc()
        return rows

    soup = BeautifulSoup(resp.text, "html.parser")
    table, inst_col_idx, branch_col_idx = find_results_table_and_institution_col(soup)
    if table is None or inst_col_idx is None:
        print("[parse-search] Could not locate results table or 'Institution Name' column.", file=sys.stderr)
        return rows

    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds or inst_col_idx >= len(tds):
            continue

        # --- Institution Name cell: take the visible text here as School Name
        inst_cell = tds[inst_col_idx]
        a = inst_cell.find("a", href=True)
        if not a:
            continue

        raw_href = a["href"].strip()
        school_name = normalize_space(a.get_text(" ", strip=True))  # <-- authoritative name source
        
        if not raw_href or not school_name:
            continue

        # --- School/Branch cell (if present)
        school_branch = ""
        if branch_col_idx is not None and branch_col_idx < len(tds):
            school_branch = normalize_space(tds[branch_col_idx].get_text(" ", strip=True))

        # --- Optional filter by requested branch
        if requested_branch and school_branch and (school_branch != requested_branch):
            continue

        nces = ""
        detail_url_for_csv = ""

        # Visit the details page strictly to extract NCES
        try:
            time.sleep(delay_sec)
            if not raw_href.lower().startswith("javascript:"):
                detail_url = urljoin(base_url, raw_href)
                detail_url_for_csv = detail_url
                dr = session.get(detail_url)
                dr.raise_for_status()
                dsoup = BeautifulSoup(dr.text, "html.parser")
                nces = extract_nces_code_from_details(dsoup)
                if not nces:
                    print(f"[warn-nces-missing] {detail_url}", file=sys.stderr)
            else:
                parsed = parse_postback_href(raw_href)
                if not parsed:
                    print(f"[warn-postback-parse] Could not parse postback href: {raw_href}", file=sys.stderr)
                else:
                    target, argument = parsed
                    dsoup = do_postback(session, search_url, target, argument)
                    if dsoup is None:
                        print(f"[warn-postback-failed] target={target} argument={argument}", file=sys.stderr)
                    else:
                        detail_url_for_csv = ""  # canonical URL unknown for postback
                        nces = extract_nces_code_from_details(dsoup)
                        if not nces:
                            print(f"[warn-nces-missing-postback] target={target} argument={argument}", file=sys.stderr)
        except KeyboardInterrupt:
            print("[fetch-details] KeyboardInterrupt in detail retrieval loop", file=sys.stderr)
            traceback.print_exc()
            raise
        except Exception as e:
            print(f"[fetch-details] {raw_href} :: {e}", file=sys.stderr)
            traceback.print_exc()

        nces_for_csv = f'="{nces}"' if nces else ""
        school_branch_for_csv = f'="{school_branch}"' if school_branch else ""
        rows.append([school_name, school_branch_for_csv, nces_for_csv, detail_url_for_csv])

    return rows

def main():
    parser = argparse.ArgumentParser(description="Scrape EDNA NCES codes for AUNs from an input CSV.")
    parser.add_argument("input_csv", help="Path to input CSV file containing AUN values and SchoolBranch values.")
    parser.add_argument("aun_column", help="Name of the column in the input CSV that contains AUN numbers.")
    parser.add_argument("school_branch_column", help="Name of the column in the input CSV that contains School/Branch values to copy into the output.")
    parser.add_argument("output_csv", help="Path to write a single combined CSV of results.")
    parser.add_argument("--delay", type=float, default=0.6, help="Delay (seconds) between detail page requests. Default: 0.6")
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    if not input_path.exists():
        print(f"[main] Input CSV not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Read (AUN, SchoolBranch) pairs from the specified columns
    pairs = []  # list of (aun, school_branch)
    try:
        with input_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            missing = [c for c in (args.aun_column, args.school_branch_column) if c not in fieldnames]
            if missing:
                print(f"[main] Missing column(s): {missing}. Available columns: {fieldnames}", file=sys.stderr)
                sys.exit(1)

            for row in reader:
                aun = (row.get(args.aun_column) or "").strip()
                sch_branch = (row.get(args.school_branch_column) or "").strip()
                if aun:
                    pairs.append((aun, sch_branch))
    except Exception as e:
        print(f"[main-read] {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    # Dedupe (AUN, SchoolBranch) while preserving order
    seen_keys = set()
    unique_pairs = []
    for k in pairs:
        if k not in seen_keys:
            seen_keys.add(k)
            unique_pairs.append(k)

    # Caches
    results_cache_by_key = {}  # (aun, school_branch) -> rows with branch applied
    results_cache_district = {}  # aun -> (district_name_raw, district_nces_raw)
    results_cache_by_pair = {}  # (aun, school_branch) -> raw rows (no district columns)

    # Scrape for each (AUN, SchoolBranch) pair and aggregate rows
    all_rows = []

    # Auto-disable the progress bar when not attached to a TTY (e.g., piped logs)
    disable_pb = not sys.stderr.isatty()

    # --- Open output for streaming rows as we go ---
    try:
        out_f = output_path.open("w", newline="", encoding="utf-8", buffering=1)  # line-buffered
    except Exception as e:
        print(f"[main-open-output] {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

    writer = csv.writer(out_f, quoting=csv.QUOTE_ALL)
    writer.writerow([
        "School Name",
        "School/Branch",
        "NCES Code",
        "Detail URL",
        "District Name",
        "District NCES",
        "NCES 12-digit (District+Branch)"
    ])

    def _flush_out():
        try:
            out_f.flush()
            # On some filesystems fsync is helpful to make content visible immediately
            os.fsync(out_f.fileno())
        except Exception as e:
            print(f"[flush] {e}", file=sys.stderr)
            traceback.print_exc()

    with tqdm(
        unique_pairs,
        total=len(unique_pairs),
        desc="AUN/SchoolBranch",
        unit="pair",
        dynamic_ncols=True,
        leave=True,
        disable=disable_pb,
    ) as pbar:
        for aun, sch_branch in pbar:
            try:
                key = (aun, sch_branch)

                # district cache (once per AUN)
                district_info = results_cache_district.get(aun)
                if district_info is None:
                    d_name, d_nces = get_district_info(aun, delay_sec=args.delay)
                    results_cache_district[aun] = (d_name, d_nces)
                    pbar.write(f"[district] AUN={aun} name='{d_name or '-'}' nces='{d_nces or '-'}'")
                d_name, d_nces = results_cache_district.get(aun, ("", ""))

                # PAIR cache (AUN, SchoolBranch)
                raw_rows = results_cache_by_pair.get(key)
                if raw_rows is None:
                    search_url = build_search_url(aun, sch_branch)

                    # --- DIAGNOSTIC: prove we’re calling scrape per pair
                    pbar.write(f"[call-scrape] AUN={aun} branch='{sch_branch or '-'}' url={search_url}")

                    raw_rows = scrape(
                        search_url=search_url,
                        requested_branch=sch_branch,       # IMPORTANT: filter/verify branch in scrape
                        delay_sec=args.delay,
                    ) or []
                    results_cache_by_pair[key] = raw_rows
                    pbar.write(f"[scrape] AUN={aun} branch='{sch_branch or '-'}' rows={len(raw_rows)}")

                    if sch_branch and not raw_rows:
                        pbar.write(f"[note] AUN={aun} branch='{sch_branch}' -> no matching detail page with that branch; site may not map it.")
                else:
                    pbar.write(f"[pair-cache-hit] AUN={aun} branch='{sch_branch}' rows={len(raw_rows)}")

                # Attach district columns, then stream or accumulate
                final_rows = attach_district_columns(raw_rows, d_name, d_nces)

                results_cache_by_key[key] = final_rows
                writer.writerows(final_rows)   # if streaming
                _flush_out()                   # if streaming
                all_rows.extend(final_rows)    # optional in-memory tally

                pbar.write(f"[emit] AUN={aun} branch='{sch_branch}' rows={len(final_rows)} (streamed)")
            except KeyboardInterrupt:
                print(f"[main-scrape AUN={aun}] KeyboardInterrupt", file=sys.stderr)
                traceback.print_exc()
                raise
            except Exception as e:
                print(f"[main-scrape AUN={aun}] {e}", file=sys.stderr)
                traceback.print_exc()
                continue
               
    out_f.close()

if __name__ == "__main__":
    main()

