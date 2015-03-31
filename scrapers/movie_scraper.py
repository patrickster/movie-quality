"""Superclass for the scrapers."""

import os
import sqlite3


class MovieScraper:

  DB_PATH = '%s/databases/movies.db' % os.getenv('HOME')

  BOX_OFFICE_TABLE = 'weekly_box_office'

  ID_TABLE = 'movie_ids'

  ROTTEN_TOMATOES_TABLE = 'rotten_tomatoes'

  REQUEST_HEADERS = {
      # 'User-Agent' : 'Mozilla/4.0 (compatible; MSIE 6.1; Windows XP)',
      'User-Agent': ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 '
                     '(KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36')
  }


  def __init__(self):
    self.conn = sqlite3.connect(self.DB_PATH)
    self.cursor = self.conn.cursor()


  def MaybeCreateBoxOfficeTable(self):
    """Creates the weekly box office table if it does not already exist."""
    try:
      self.cursor.execute(
          ('CREATE TABLE %s '
           '(week TEXT, id TEXT, title TEXT, studio TEXT, gross REAL, '
           ' theaters INTEGER, budget REAL)') % self.BOX_OFFICE_TABLE)
    except sqlite3.OperationalError:
      pass


  def MaybeCreateIdTable(self):
    """Creates the movie ID table if it does not already exist."""
    try:
      self.cursor.execute(
          ('CREATE TABLE %s '
           '  (box_office_mojo TEXT PRIMARY KEY, rotten_tomatoes TEXT, '
           '   imdb TEXT, metacritic TEXT)') % self.ID_TABLE)
    except sqlite3.OperationalError:
      pass


  def MaybeCreateRottenTomatoesTable(self):
    """Creates the RottenTomatoes table if it does not already exist."""
    try:
      self.cursor.execute(
          ('CREATE TABLE %s ('
           '  id TEXT PRIMARY KEY, '
           '  title TEXT, '
           '  year INTEGER, '
           '  tomatometer INTEGER, '
           '  avg_critic_rating REAL, '
           '  reviews INTEGER, '
           '  fresh INTEGER, '
           '  rotten INTEGER, '
           '  audience_score INTEGER, '
           '  avg_user_rating REAL, '
           '  user_ratings INTEGER, '
           '  mpaa_rating TEXT, '
           '  genre TEXT, '
           '  release_date TEXT, '
           '  runtime INTEGER)') % self.ROTTEN_TOMATOES_TABLE)
    except sqlite3.OperationalError:
      pass
