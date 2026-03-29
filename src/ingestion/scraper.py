import requests
# this library is used to make http request to the website and fetch the html content of the page. 
# It is a popular library for making HTTP requests in Python and provides a simple and intuitive API for sending HTTP requests and handling responses.
from bs4 import BeautifulSoup
# BeautifulSoup is a Python library used for navigating HTML and XML documents. 
# It provides a convenient way to navigate and search through the structure of a web page, making it easier to extract specific data from the HTML content.
# for my project, it helps me to extract relevant data from tables that hav information about the share price, volume, and other details of the stocks listed on the Nepal Stock Exchange (NEPSE) website.
import pandas as pd
# pandas is going to help me clean and manipulate the data that I have scraped from the NEPSE website.
from datetime import date
# used to get time stamps

def fetch_nepse_data():
    # wraping the code in a function allows us to easily call it whenever we wan to fetch the data.
    # most probably for airflow
    url = "https://www.sharesansar.com/today-share-price"

    # The User-Agent header is used to identify the client software making the request.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        # 200 status code means the request was successful, if it's not 200, it means there was an error fetching the page.
        # response.staus is printed to know what the error was, for example 404 400 etc.
        print(f"Failed to fetch page. Status code: {response.status_code}")
        return None
    
    # html.parser is a inbuilt parser in python 
    # that is used to parse the html content of the page and 
    # create a BeautifulSoup object that we can use to navigate and extract data from the page.
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table")

    if not table:
        print("No table found on page")
        return None
    #th is used to define the header of the table 
    #header like share symbol, company name, last price, change, percentage change, volume etc.
    headers_row = table.find_all("th")
    columns = [th.text.strip() for th in headers_row]

    rows = []
    #using array to store the data of each row/share in the table
    for tr in table.find_all("tr")[1:]:
        # [1:] this silce is used becuase 
        # most of the time the first row of the table contains the header, 
        # so we skip it by slicing the list of rows starting from index 1.
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    #create a pandas DataFrame using the extracted data, with the columns defined by the headers we extracted earlier.
    df = pd.DataFrame(rows, columns=columns)
    df["scraped_date"] = date.today()
    df = df.drop_duplicates()
    return df

# This block of code is used to test the function fetch_nepse_data() when the script is run directly.
# It calls the function to fetch the data, and if the data is successfully fetched 
# runs only if the script is explicitly executed (not imported as a module in another script).
if __name__ == "__main__":
    # this import is used to import the functions that we have defined in db.py, 
    # which is responsible for handling all database interactions, including creating tables and saving data to PostgreSQL.
    from db import save_to_postgres, create_table

    df = fetch_nepse_data()
    if df is not None:
        print(df.head())
        print(f"\nShape: {df.shape}")
        print(f"\nColumns: {df.columns.tolist()}")

        # save to CSV as backup
        filename = f"data/raw/nepse_{date.today()}.csv"
        df.to_csv(filename, index=False)
        print(f"\nData saved to {filename}")

        # save to PostgreSQL
        create_table()
        save_to_postgres(df)