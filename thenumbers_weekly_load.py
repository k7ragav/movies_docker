from thenumbers_weekly_extract import get_weekend_date
from netflix_top10 import sql_connection
import os
import sys
import csv


def extract_data_from_csv(filename: str):

    with open(filename, encoding='utf-8') as file:
        csv_file = csv.reader(file, delimiter=",")
        next(csv_file, None)
        result_data = [(tuple(line)) for line in csv_file]

    os.remove(filename)
    return result_data

def get_filename(weekend_date: str) -> str:
    weekend_date_cleaned = weekend_date.replace(f'/', "")
    filename = f"{weekend_date_cleaned}.csv"
    return filename

def insert_result_in_table(result):
    mydb, mycursor = sql_connection()
    sql_query = "INSERT INTO weekly_boxoffice_numbers_US (`Ranking`,MovieTitle,Distributor,`Gross`,Per_LW,Theaters,TheatersChange,PerTheater,TotalGross,WeekendsInRelease,WeekendDate) " \
                "VALUES (%s, %s, %s, %s, %s ,%s, %s, %s, %s ,%s, %s)"

    mycursor.executemany(sql_query, result)
    mydb.commit()

def main():
    weekend_date = get_weekend_date(date=sys.argv[1])
    filename = get_filename(weekend_date=weekend_date)
    result = extract_data_from_csv(filename=filename)
    insert_result_in_table(result)

if __name__ == "__main__":
    main()
