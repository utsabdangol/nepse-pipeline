# this file is for backfilling historical data by scraping sharesansar.com
# it does not work at the moment
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, datetime, timedelta
import time
import sys
import os
from urllib.parse import unquote

sys.path.append(os.path.dirname(__file__))
from db import create_table, save_to_postgres

BASE_URL = "https://www.sharesansar.com"
AJAX_URL = f"{BASE_URL}/ajaxtodayshareprice"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{BASE_URL}/today-share-price",
}


def get_csrf_token():
    session = requests.Session()
    
    response = session.get(
        f"{BASE_URL}/today-share-price",
        headers=HEADERS,
        timeout=30
    )
    
    soup = BeautifulSoup(response.text, "html.parser")
    token = soup.find("meta", {"name": "_token"})
    if token is None:
        raise ValueError("Could not find CSRF token")
    
    # verify session cookies were set
    print(f"Session cookie: {session.cookies.get('sharesansar_session', 'NOT FOUND')[:20]}...")
    print(f"XSRF cookie: {session.cookies.get('XSRF-TOKEN', 'NOT FOUND')[:20]}...")
    
    return session, token["content"]


def fetch_data_for_date(session, token, trading_date):
    date_str = trading_date.strftime("%Y-%m-%d")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE_URL,
        "Referer": f"{BASE_URL}/today-share-price",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    payload = {
        "date": date_str,
        "sector": "all sec",
        "_token": token,
    }

    response = session.post(
        AJAX_URL,
        headers=headers,
        data=payload,
        timeout=30
    )

    if response.status_code != 200:
        raise ValueError(f"Request failed for {date_str}: status {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    date_span = soup.find("span", {"class": "text-org"})
    if date_span is None:
        print(f"  No date found for {date_str} — skipping")
        return None

    page_date_str = date_span.text.strip()
    page_date = datetime.strptime(page_date_str, "%Y-%m-%d").date()

    if page_date != trading_date:
        print(f"  Requested {date_str} but page shows {page_date_str} — market closed, skipping")
        return None

    table = soup.find("table")
    if not table:
        print(f"  No table found for {date_str}, skipping")
        return None

    headers_row = table.find_all("th")
    columns = [th.text.strip() for th in headers_row]

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    if not rows:
        print(f"  No rows found for {date_str}, skipping")
        return None

    if len(rows[0]) == 1 and "No Record" in rows[0][0]:
        print(f"  No record found for {date_str} — public holiday or no trading, skipping")
        return None

    df = pd.DataFrame(rows, columns=columns)
    df["scraped_date"] = page_date
    df = df.drop_duplicates()

    print(f"  Fetched {len(df)} rows for {page_date_str}")
    return df

def get_trading_days(start_date, end_date):
    # NEPSE trades Sunday to Thursday
    trading_days = []
    current = start_date
    while current <= end_date:
        # 0=Monday, 6=Sunday in Python
        # NEPSE trades: Sunday(6), Monday(0), Tuesday(1), Wednesday(2), Thursday(3)
        if current.weekday() in [6, 0, 1, 2, 3]:
            trading_days.append(current)
        current += timedelta(days=1)
    return trading_days


def backfill(start_date, end_date):
    print(f"Backfilling from {start_date} to {end_date}")

    # get csrf token once and reuse session
    print("Fetching CSRF token...")
    session, token = get_csrf_token()
    print(f"Token obtained: {token[:20]}...")

    trading_days = get_trading_days(start_date, end_date)
    print(f"Found {len(trading_days)} potential trading days")

    create_table()

    success = 0
    skipped = 0
    failed = 0

    for trading_date in trading_days:
        print(f"\nFetching {trading_date}...")
        try:
            df = fetch_data_for_date(session, token, trading_date)

            if df is None:
                skipped += 1
                continue

            # save to csv backup
            filename = f"data/raw/nepse_{trading_date}.csv"
            df.to_csv(filename, index=False)

            save_to_postgres(df)
            success += 1

            # be polite — wait 2 seconds between requests
            # avoids overwhelming sharesansar's server
            time.sleep(2)

        except Exception as e:
            print(f"  ERROR for {trading_date}: {e}")
            failed += 1
            continue

    print(f"\nBackfill complete:")
    print(f"  Success:  {success}")
    print(f"  Skipped:  {skipped} (holidays/closed days)")
    print(f"  Failed:   {failed}")


# if __name__ == "__main__":
#     # backfill last 6 months
#     end = date(2026, 3, 26)      # last known good date in your DB
#     start = date(2025, 10, 1)    # 6 months back

#     backfill(start, end)
if __name__ == "__main__":
    session, token = get_csrf_token()
    from datetime import date
    df = fetch_data_for_date(session, token, date(2026, 3, 26))
    if df is not None:
        print(f"SUCCESS: {len(df)} rows")
        print(df.head())
    else:
        print("FAILED")