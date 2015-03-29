"""Scrapes weekly box office charts from BoxOfficeMojo.com."""

import datetime as dt
import os
import math
import random
import re
import requests
import sqlite3
import time

from bs4 import BeautifulSoup

from movie_scraper import MovieScraper


class BoxOfficeMojoScraper(MovieScraper):

  HTML_DIR = '../html/box_office_mojo'

  START_DATE = dt.date(1999, 12, 31)


  def __init__(self):
    self.MaybeCreateBoxOfficeTable()
    self.MaybeCreateIdTable()


  def run(self):
    """Runs the scraper."""
    weeks = self.GetListOfWeeks()
    for week in weeks:
      chart = self.LoadWeeklyChart(week)
      self.ParseWeeklyChart(chart, week)
    self.UpdateIdTable()
    self.conn.commit()
    self.conn.close()


  def GetFridayOfLastFullWeek(self):
    """Returns the date of the Friday of the last full week."""
    today = dt.date.today()
    dow = today.weekday()
    if (dow >= 4):
      return today + dt.timedelta(days=-(7 + dow - 4))
    else:
      return today + dt.timedelta(days=-(7 + (7 + 4 - dow)))


  def GetListOfWeeks(self):
    """Returns a list of weeks to scrape."""
    delta_days = (self.GetFridayOfLastFullWeek() - self.START_DATE).days
    delta_weeks = int(math.floor(delta_days / 7))
    weeks = [self.START_DATE + dt.timedelta(days=7 * x) 
             for x in range(0, delta_weeks + 1)]
    weeks = [week.strftime('%Y-%m-%d') for week in weeks]
    self.cursor.execute(
        'SELECT DISTINCT week FROM %s' % self.BOX_OFFICE_TABLE_NAME)
    weeks_in_table = [x[0] for x in self.cursor.fetchall()]
    weeks = list(set(weeks) - set(weeks_in_table))
    weeks.sort()    
    return weeks


  def LoadWeeklyChart(self, week):
    """Loads the chart for the specified week, either from disk or the web."""
    file_name = '%s.html' % week
    if file_name in os.listdir(self.HTML_DIR):
      print 'Loading chart for week of %s' % week
      with open(os.path.join(self.HTML_DIR, file_name), 'r') as f:
        chart = f.read()
    else:
      chart = self.DownloadWeeklyChart(week)
      self.SaveWeeklyChart(chart, file_name)
    return chart


  def GetWeekNum(self, date):
    """Gets the week number of the specified date relative to the start date."""
    (y, m, d) = date.split('-')
    return (dt.date(int(y), int(m), int(d)) - self.START_DATE).days / 7


  def DownloadWeeklyChart(self, week):
    """Downloads the chart for the specified week."""
    print 'Downloading chart for week of %s' % week
    url = ('http://www.boxofficemojo.com/weekly/chart/?yr=2000&wk=%d&p=.htm'
           % self.GetWeekNum(week))
    response = requests.get(url, headers=self.REQUEST_HEADERS)
    time.sleep(2)
    return response.content


  def SaveWeeklyChart(self, chart, file_name):
    """Saves a downloaded chart to disk."""
    with open(os.path.join(self.HTML_DIR, file_name), 'w') as f:
      f.write(chart)


  def ParseWeeklyChart(self, html, week):
    """Extracts the contents of a weekly chart."""
    print 'Parsing chart for week of %s' % week
    chart = []
    soup = BeautifulSoup(html)
    table = soup.findAll('table')[3]
    table_rows = table.findAll('tr')[3:]
    for tr in table_rows:
      row = {}
      cols = tr.findAll('td')
      # Check whether the first cell in the row has a colspan attribute,
      # in which case we've reached the end of the table.
      try:
        cols[0]['colspan']
        break
      except KeyError:
        pass
      title = cols[2].text
      title = title.replace('\'', '\'\'')  # Escape single quotes.
      row['title'] = title
      link = cols[2].find('a')
      m = re.match('.*id=(?P<id>.*)\.htm.*', str(link).lower())
      row['id'] = m.group('id')
      row['studio'] = cols[3].text
      row['gross'] = re.sub('[^\d\.]', '', cols[4].text)
      row['theaters'] = re.sub('[^\d]', '', cols[6].text)
      row['budget'] = re.sub('[^\d]', '', cols[10].text) or 'NULL'
      row['week'] = week
      self.InsertChartRow(row)


  def InsertChartRow(self, row):
    """Writes a row to the database."""
    query = ('INSERT INTO %s VALUES '
             '  (\'%s\', \'%s\', \'%s\', \'%s\', %s, %s, %s)' %
             (self.BOX_OFFICE_TABLE_NAME, row['week'], row['id'], row['title'],
              row['studio'], row['gross'], row['theaters'], row['budget']))
    self.cursor.execute(query)


  def UpdateIdTable(self):
    """Adds new BoxOfficeMojo IDs to the ID table."""
    query = ('INSERT INTO %s '
             '  SELECT DISTINCT title, \'\', \'\', \'\' '
             '  FROM %s '
             '  WHERE title NOT IN (SELECT box_office_mojo FROM %s)' %
             (self.ID_TABLE_NAME, self.BOX_OFFICE_TABLE_NAME,
              self.ID_TABLE_NAME))
    self.cursor.execute(query)
      

if __name__ == '__main__':
  BoxOfficeMojoScraper().run()


