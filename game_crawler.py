import bz2
import configparser
import os
from random import randrange
from sys import exit
from time import sleep
import mlbgame
from pandas import date_range
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as chrome_options
from selenium.common.exceptions import UnexpectedAlertPresentException

class Crawler:

    def __init__(self):

        config = configparser.ConfigParser()
        config.read("./crawler.conf")
        self.year = config.get("crawling", "year")
        self.start_date = config.get("crawling", "start_date") if config.has_option("crawling", "start_date") else None
        self.chrome_path = config.get("browser", "chrome_path")
        self.chromedriver_path = config.get("browser", "chromedriver_path")

        self.max_retry = 5

    def get_useragent(self):
        useragent_list = []
        return randrange(useragent_list)

    def setup_browser(self):

        options = chrome_options()
        options.binary_location = self.chrome_path
        options.add_argument("--proxy-server=socks5://127.0.0.1:9050");
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")

        browser = webdriver.Chrome(chrome_options=options,
                                   executable_path=self.chromedriver_path)
        return browser

    def fetch_xml(self, url, path):

        sleep(randrange(10))
        print("URL: " + url)

        for count in range(self.max_retry):

            try:
                browser.get(url)
                source = browser.page_source
                with open(path.replace("inning/", "") + ".bz2", "wb") as f:
                    f.write(bz2.compress(source.encode("utf-8")))
                print("Got it!")
                break
            
            except Exception as err:
                print(err)

                if type(err) == UnexpectedAlertPresentException:
                    print("Finish to Check?")
                    if input():
                        alert = browser.switch_to_alert()
                        alert.accept()
                        break

                if count == self.max_retry - 1:
                    print("Reach Max Limit of Request!")
                    exit()
                    
                print("Request Error %d" % (count))
                sleep(randrange(5))
                continue

    def crawl_xmls(self, browser):

        xmls_to_fetch = ["boxscore.xml",
                         "rawboxscore.xml",
                         "game_events.xml",
                         "linescore.xml",
                         "players.xml",
                         "inning/inning_all.xml",
                         "game.xml"]

        event_dates = mlbgame.important_dates(year=self.year)
        last_date = event_dates.last_date_seas
        first_date = event_dates.first_date_seas if self.start_date is None else self.start_date
        dates = date_range(first_date, last_date)

        for date in dates:
            game_scoreboards = mlbgame.day(date.year, date.month, date.day)
            game_scoreboards = [[match] for match in game_scoreboards]
            games = mlbgame.combine_games(game_scoreboards)

            year = date.year
            month = "{0:02d}".format(date.month)
            day = "{0:02d}".format(date.day)

            for game in games:
                gameid = game.game_id

                if not os.path.exists("xml/" + gameid):
                    os.makedirs("xml/" + gameid)

                dir_path = "http://gd2.mlb.com/components/game/mlb/year_%s/month_%s/day_%s/gid_%s/" % \
                          (year, month, day, gameid)

                for xml in xmls_to_fetch:
                    url_fetch = dir_path + xml
                    path_write = "xml/" + gameid + "/" + xml
                    self.fetch_xml(url_fetch, path_write)


if __name__ == "__main__":

    crawler = Crawler()
    browser = crawler.setup_browser()
    crawler.crawl_xmls(browser=browser)

    if "browser" in globals():
        browser.close()
        browser.quit()

    print("finish!")
