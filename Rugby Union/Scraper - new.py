import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path
import datetime
from datetime import timedelta
import logging
import requests
import json
import csv
import os
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding="utf-8")

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 2000)

team_list = pd.read_csv('Teams.csv')
comp_list = pd.read_csv('Comps.csv')
channel_list = pd.read_csv('C:/Users/Shanda/Google Drive/Python Scripts/sport-tv-guide-to-TSDB-Scraper/Channels.csv')

class TSDB:
    with open("C:/Users/Shanda/Google Drive/Python Scripts/sport-tv-guide-to-TSDB-Scraper/TSDB_credentials.txt", "r") as f:
        for details in f:
            username, password, api_key = details.split(":")
    url = "https://www.thesportsdb.com/"
    TV = (f"{url}edit_event_tv.php?e=")
    api = (f"{url}api/v1/json/{api_key}/searchfilename.php?e=")
    login = (f"{url}user_login.php")
    login_data = {"username": username, "password": password}

yr = 2022
data = []

count = 0
o_count = 0

current_date = datetime.datetime.now()
day1 = current_date.strftime("%d")
month1 = current_date.strftime("%m")
year1 = current_date.strftime("%Y")
adjust_1 = timedelta(days = 1)
adjust_2 = timedelta(days = 2)
yesterday = current_date - adjust_1
two_days = current_date - adjust_2
day2 = yesterday.strftime("%d")
month2 = yesterday.strftime("%m")
year2 = yesterday.strftime("%Y")
day3 = two_days.strftime("%d")
month3 = two_days.strftime("%m")
year3 = two_days.strftime("%Y")

comps = ['Aviva Premiership Rugby', 'Top 14', 'Currie Cup', 'Super Rugby', 'United Rugby Championship', 'Greene King IPA Championship', 'National Rugby League', 'Super League Rugby', 'Championship', 'Six Nations', 'Rugby World Cup', 'European Rugby Champions Cup', 'Rugby Europe Championship']

file = Path(f"C:/Users/Shanda/Google Drive/Python Scripts/sport-tv-guide-to-TSDB-Scraper/scraped_{year1}_{month1}_{day1}.csv")

if file.is_file():
    pass
else:
    sys.exit("No scrape has been conducted.")

event_list = pd.read_csv(f"C:/Users/Shanda/Google Drive/Python Scripts/sport-tv-guide-to-TSDB-Scraper/scraped_{year1}_{month1}_{day1}.csv")
df = pd.DataFrame(event_list)

df[["ht", "at"]] = df.teams.str.split(" - ", 1, expand=True)
df.drop(columns="teams", inplace=True)
df = df[(df['comp'].isin(comps))]
df["date"] = df["date"] + (" ") + str(yr) + (" ") + df["time"]
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["date"] = df["date"].dt.date
merge1 = df.merge(channel_list, left_on='channel', right_on='STG', how='outer')
merge2 = merge1.merge(comp_list, left_on='comp', right_on='STG', how='outer')
merge3 = merge2.merge(team_list, left_on='ht', right_on='STG', how='outer')
df = merge3.merge(team_list, left_on='at', right_on='STG', how='outer')
df.drop(["comp", "channel", "ht", "at", "STG_x", "STG_y"], axis=1, inplace=True)
df.dropna(subset=['date'], inplace=True)
df.dropna(subset=['TSDB_x'], inplace=True)
df.dropna(subset=['TSDB_y'], inplace=True)
df = df[df['sport'].isin(['Rugby Union', 'Rugby League'])]
df.columns = ["date", "time", "sport", "channel", "comp", "ht", "at"]
df.sort_values(["date", "comp", "ht"], ascending=[True, True, True], inplace=True)
df.reset_index(inplace=True, drop=True)
df.to_csv(f"scraped_{year1}_{month1}_{day1}_df.csv", mode="w", index=False, header=True)

df3 = pd.read_csv(f"scraped_{year3}_{month3}_{day3}_df.csv")
df2 = pd.read_csv(f"scraped_{year2}_{month2}_{day2}_df.csv")
df1 = pd.read_csv(f"scraped_{year1}_{month1}_{day1}_df.csv")

frames = [df1, df2, df3]
df = pd.concat(frames)

df["dupecheck"] = df["channel"] + df["ht"] + df["at"]
df.drop_duplicates(subset="dupecheck", keep=False, inplace=True)
df.drop(["dupecheck"], axis=1, inplace=True)
df.sort_values(["date", "comp", "ht"], ascending=[True, True, True], inplace=True)
df = df[~df['date'].isin([f'{year2}-{month2}-{day2}', f'{year3}-{month3}-{day3}'])]
df.reset_index(inplace=True, drop=True)
df.to_csv(f"scraped_{year1}_{month1}_{day1}_df.csv", mode="w", index=False, header=True)
print(df)

driver = webdriver.Chrome(ChromeDriverManager().install())

driver.get(TSDB.login)

driver.find_element_by_name("username").send_keys(TSDB.username)
driver.find_element_by_name("password").send_keys(TSDB.password)
driver.find_element_by_xpath("/html/body/section/div/div[3]/div/form/div[4]/input").click()

with open(f"scraped_{year1}_{month1}_{day1}_df.csv", "r", encoding="UTF-8") as fp:
    lines = csv.reader(fp)
    for line in lines:
        try:
            api_call = requests.get(f"{TSDB.api}{line[4]}_{line[0]}_{line[5]}_vs_{line[6]}")
            storage = api_call.json()
            for match in storage["event"]:
                pp = match["strPostponed"]
                if pp == "yes":
                    count -= 1
                    print(f"*** {line[5]} vs {line[6]} is postponed. ***\n")
                    pass
                else:
                    idEvent = match["idEvent"]
                    driver.get(f"{TSDB.TV}{idEvent}")
                    driver.find_element_by_id("channel").send_keys(f"{line[3]}")
                    driver.find_element_by_name("submit").click()
                    count += 1
                    o_count += 1
        except TypeError:
            count -= 1
            logging.exception(f"*** {line[5]} vs {line[6]} on {line[3]} not found. ***\n")
            pass
        except json.decoder.JSONDecodeError:
            count -= 1
            logging.exception(f"*** JSON error - {line[5]} vs {line[6]} ***\n")
            pass

print(f"{count} of {o_count} rugby TV updated.")

driver.quit()