from typing import Any
import sys
import os

import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import timedelta
import datetime



def get_weekend_date(date: str) -> str:
        today_date = datetime.datetime.strptime(date, "%Y-%m-%d")
        today_date_corrected = today_date + timedelta(7)
        weekend_date = (today_date_corrected - timedelta(4)).strftime('%Y/%m/%d')
        return weekend_date

def get_soup_content(main_url: str, weekend_date: str) -> Any:

        url = f"{main_url}{weekend_date}"

        html_content = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'})

        soup = BeautifulSoup(html_content.content.decode('utf-8'), 'lxml')
        return soup

def extract_table_data(soup) -> Any:
        data = []
        table = soup.find('table', id='box_office_weekend_table')
        rows = table.find_all('tr')
        header_row = [td.get_text(strip=True) for td in rows[0].find_all('th')]
        data.append(["".join(row.split(" ")) for row in header_row[2::]])
        for row in rows[1::]:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([str(ele) for ele in cols[2::]])  # Get rid of empty values

        return data

def clean_table_data(data: list, weekend_date:str) -> pd.DataFrame:

        df = pd.DataFrame(data[1::], columns=data[0])
        df = df.reset_index()
        df = df.rename(columns={"index": "Ranking"})
        df['Ranking'] = df.index + 1
        df.rename(columns={'%LW': 'Per_LW'}, inplace=True)
        df['WeekendDate'] = weekend_date.replace(f'/', '-')
        df.drop(df.tail(2).index, inplace=True)
        return df

def export_data(df: pd.DataFrame, weekend_date: str) -> None:
        weekend_date_cleaned = weekend_date.replace(f'/', "")
        df.to_csv(f"{weekend_date_cleaned}.csv", index=False)

def main():

        weekend_date = get_weekend_date(date=sys.argv[1])
        soup = get_soup_content(main_url='https://www.the-numbers.com/box-office-chart/weekend/', weekend_date=weekend_date)

        data = extract_table_data(soup)

        df = clean_table_data(data=data, weekend_date=weekend_date)
        export_data(df=df, weekend_date=weekend_date)



if __name__ == "__main__":
        main()