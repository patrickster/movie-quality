"""Superclass for the scrapers."""

import os
import sqlite3


class MovieScraper:

  DB_NAME = '%s/databases/movies.db' % os.getenv('HOME')

  BOX_OFFICE_TABLE_NAME = 'weekly_box_office'

  ID_TABLE_NAME = 'movie_ids'

  REQUEST_HEADERS = {
      'User-Agent' : 'Mozilla/4.0 (compatible; MSIE 6.1; Windows XP)',
  }


  def __init__(self):
    self.conn = sqlite3.connect(self.DB_NAME)
    self.cursor = self.conn.cursor()


  def MaybeCreateBoxOfficeTable(self):
    """Creates the weekly box office table if it does not already exist."""
    try:
      self.cursor.execute(
          ('CREATE TABLE %s '
           '(week text, id text, title text, studio text, gross real, '
           ' theater real, budget real)') % self.BOX_OFFICE_TABLE_NAME)
    except sqlite3.OperationalError:
      pass


  def MaybeCreateIdTable(self):
    """Creates the movie ID table if it does not already exist."""
    try:
      self.cursor.execute(
          ('CREATE TABLE %s '
           '  (box_office_mojo text, rotten_tomatoes text, imdb text, '
           '   metacritic text)') % self.ID_TABLE_NAME)
    except sqlite3.OperationalError:
      pass
