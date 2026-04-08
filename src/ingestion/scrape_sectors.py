import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_sector_mapping():
    url = "https://www.merolagani.com/CompanyList.aspx"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.text, "html.parser")

    # find the accordion div containing all sectors
    accordion = soup.find("div", {"id": "accordion"})
    if not accordion:
        raise ValueError("Could not find sector accordion on page")

    records = []

    # each panel is one sector
    panels = accordion.find_all("div", {"class": "panel"})

    for panel in panels:
        # get sector name from the panel heading
        heading = panel.find("a", {"data-toggle": "collapse"})
        if not heading:
            continue
        sector = heading.text.strip()

        # skip sectors we don't care about for ML
        skip_sectors = [
            "Corporate Debenture",
            "Government Bond",
            "Preferred Stock",
            "Promotor Share",
            "Mutual Fund"
        ]
        if sector in skip_sectors:
            continue

        # find all symbol links inside this panel
        table = panel.find("table")
        if not table:
            continue

        rows = table.find_all("tr")[1:]  # skip header row
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue
            symbol_link = cells[0].find("a")
            if symbol_link:
                symbol = symbol_link.text.strip()
                records.append({
                    "symbol": symbol,
                    "sector": sector
                })

    df = pd.DataFrame(records)
    print(f"Found {len(df)} symbols across {df['sector'].nunique()} sectors")
    print(df['sector'].value_counts())
    return df


if __name__ == "__main__":
    df = scrape_sector_mapping()

    # save to CSV
    df.to_csv("data/sector_mapping.csv", index=False)
    print(f"\nSaved to data/sector_mapping.csv")
    print(df.head(20))
