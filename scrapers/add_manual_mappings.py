"""Add RottenTomatoes ids that had to be looked up manually."""

import csv
import sqlite3

from movie_scraper import MovieScraper


conn = sqlite3.connect(MovieScraper.DB_PATH)
cursor = conn.cursor()

with open('mappings.csv', 'r') as f:
  reader = csv.reader(f)
  for row in reader:
    print row
    query = ('UPDATE %s ' 
             'SET rotten_tomatoes = \'%s\' '
             'WHERE box_office_mojo = \'%s\'' % (
              MovieScraper.ID_TABLE, row[1], row[0]))
    cursor.execute(query)

conn.commit()
conn.close()
