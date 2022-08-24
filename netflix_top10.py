from __future__ import annotations

import csv
import requests
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

from bs4 import BeautifulSoup
# open .tsv file

def sql_connection():
    mydb = mysql.connector.connect(host="87.106.113.205",
                                   user = os.getenv('DB_USER'),
                                   password = os.getenv('DB_PASSWORD'),
                                   database = 'movie_db'
                                   )

    mycursor = mydb.cursor(prepared=True)
    return mydb, mycursor

def netflix_global():
    url = f"https://top10.netflix.com/data/all-weeks-global.tsv"
    with open('netflix_global.tsv', 'wb') as out_file:
        content = requests.get(url, stream=True).content
        out_file.write(content)
    # soup = BeautifulSoup(html_content, "html.parser")
    #
    result_data = []
    with open("netflix_global.tsv") as file:

        tsv_file = csv.reader(file, delimiter="\t")
        next(tsv_file, None)
        # printing data line by line
        for line in tsv_file:
            result_data.append(tuple(line))

    os.remove('netflix_global.tsv')
    return result_data

def insert_result_in_table(result):
    mydb, mycursor = sql_connection()
    sql_query= "INSERT INTO netflix_top10 (`week`, category, weekly_rank, title, season, weekly_hours, weeks_in_top_10) VALUES (%s, %s, %s, %s, %s ,%s, %s)"

    mycursor.executemany(sql_query, result)
    mydb.commit()

def truncate_table():
    mydb, mycursor = sql_connection()
    sql_query = "TRUNCATE TABLE movie_db.netflix_top10"

    mycursor.execute(sql_query)
    mydb.commit()

def main():
    truncate_table()
    result = netflix_global()
    insert_result_in_table(result)

if __name__ == "__main__":
    main()