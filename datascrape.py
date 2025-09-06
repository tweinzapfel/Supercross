# Python code to pull data on Motocross data.

import time
import re
import itertools
from pathlib import Path

import requests
import requests_cache
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://vault.racerxonline.com"
SESSION = requests_cache.CachedSession("racerx_cache", expire_after=86400)
HEADERS = {"User-Agent": "Research script (contact: your_email@example.com)"}

# Tweak these to control scope
SERIES_MAP = {
    "sx": "Supercross",
    "mx": "Motocross",
    # SMX pages use class names like 250SMX/450SMX; list pages still live under the vault site.
    # We'll harvest any event links that start with /YYYY- and let the parser handle the class.
    # If you want to include SMX, set include_smx=True below and add years that exist.
}
YEARS = list(range(1974, 2025 + 1))  # adjust upper bound as new seasons post
INCLUDE_SMX = True  # also collect SMX/SMX Next if found on race pages

out_dir = Path("racerx_export")
out_dir.mkdir(exist_ok=True, parents=True)

def get_soup(url):
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def list_year_races(year, series_slug):
    """Return list of absolute hrefs for all class result pages for a given year/series."""
    url = f"{BASE}/{year}/{series_slug}/races"
    soup = get_soup(url)

    # Any link pointing to a dated event (e.g., /2025-05-10/450sx/...)
    # Grab all; we’ll dedupe downstream.
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        # normalize relative links
        if href.startswith("/"):
            full = f"{BASE}{href}"
        elif href.startswith("http"):
            full = href
        else:
            continue

        # match /YYYY-MM-DD/... pattern
        if re.search(r"/\d{4}-\d{2}-\d{2}/", full):
            links.append(full)

    return sorted(set(links))

def parse_result_page(url):
    """Parse a single class result page into metadata + table rows."""
    soup = get_soup(url)

    # Header block: date, venue, class are usually in the H1/H2 and breadcrumb
    # Examples: /2025-05-10/450sx/rice-eccles-stadium or /2025-08-23/250/budds-creek-motocross-park
    # Try to find the date shown as Month D, YYYY
    h = soup.find(["h1", "h2"])
    title = h.get_text(strip=True) if h else ""

    # Look for a line like "May 10, 2025" nearby
    date_text = ""
    for tag in soup.find_all(["h2", "h3", "p", "div"]):
        txt = tag.get_text(" ", strip=True)
        if re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", txt):
            date_text = re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", txt).group(0)
            break

    # Infer class from URL segment after the date (e.g., 450sx / 250 / 250smx / smx-next)
    m = re.search(r"/\d{4}-\d{2}-\d{2}/([^/]+)/", url)
    race_class = m.group(1).lower() if m else ""

    # Venue slug is the last path part
    m2 = re.search(r"/\d{4}-\d{2}-\d{2}/[^/]+/([^/]+)/?$", url)
    venue_slug = m2.group(1).replace("-", " ").title() if m2 else ""

    # Grab first HTML table on page with results
    tables = pd.read_html(str(soup))
    if not tables:
        return None, None

    df = tables[0].copy()
    # Standardize columns (they vary: Moto 1/Moto 2 present for MX/SMX; SX often has “Machine” only)
    df.columns = [str(c).strip() for c in df.columns]

    # Add metadata columns
    df.insert(0, "event_url", url)
    df.insert(1, "title", title)
    df.insert(2, "date", date_text)
    df.insert(3, "class", race_class)
    df.insert(4, "venue", venue_slug)

    # Try to build a “year/series guess” from URL
    m3 = re.search(r"https://vault\.racerxonline\.com/(\d{4})/", url)
    df.insert(5, "year", int(m3.group(1)) if m3 else None)

    return {
        "url": url,
        "title": title,
        "date": date_text,
        "class": race_class,
        "venue": venue_slug,
    }, df

def main():
    all_links = set()

    # Crawl SX/MX by year
    for series_slug in SERIES_MAP.keys():
        for y in YEARS:
            try:
                links = list_year_races(y, series_slug)
                all_links.update(links)
                time.sleep(0.5)
            except requests.HTTPError as e:
                # Some (older/newer) years/series combos may not exist
                print(f"Skip {y}/{series_slug}: {e}")
            except Exception as e:
                print(f"Error {y}/{series_slug}: {e}")

    # Optionally sweep **any** SMX event links discovered on SX/MX list pages
    # (Many years will include SMX links mixed in; our regex already captured them.)

    # Parse every result page and concatenate
    meta_rows = []
    frames = []
    for i, url in enumerate(sorted(all_links)):
        try:
            meta, df = parse_result_page(url)
            if df is not None:
                meta_rows.append(meta)
                frames.append(df)
                print(f"[{i+1}/{len(all_links)}] {url} -> {len(df)} rows")
            time.sleep(0.5)  # be polite
        except Exception as e:
            print(f"Failed {url}: {e}")

    if frames:
        results = pd.concat(frames, ignore_index=True)
        results.to_csv(out_dir / "racerx_results.csv", index=False)
        print(f"Saved results: {len(results)} rows")

    if meta_rows:
        pd.DataFrame(meta_rows).to_csv(out_dir / "racerx_events.csv", index=False)
        print(f"Saved events: {len(meta_rows)} events")

if __name__ == "__main__":
    main()
