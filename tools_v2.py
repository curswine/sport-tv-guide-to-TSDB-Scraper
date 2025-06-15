import datetime
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import glob
import logging
import requests
import json
import csv
import os
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding="utf-8")

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 2000)

service = Service()
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument('--incognito')
options.add_argument('--headless')
options.add_argument('--start-maximized')

class TSDB:
    with open("_config/TSDB_credentials.txt", "r") as f:
        for details in f:
            username, password, api_key = details.split(":")
    url = "https://www.thesportsdb.com/"
    TV = (f"{url}edit_event_tv.php?e=")
    api = (f"{url}api/v1/json/{api_key}/searchfilename.php?e=")
    login = (f"{url}user_login.php")
    login_data = {"username": username, "password": password}

class STG:
    url = "https://sport-tv-guide.live/"
    channel = (f"{url}tv-guide-live/")

class date_info:
    yr = 2025

    current_date = datetime.datetime.now()
    adjust_1 = timedelta(days = 1)
    adjust_2 = timedelta(days = 2)
    yesterday = current_date - adjust_1
    two_days = current_date - adjust_2

    d1 = current_date.strftime("%d")
    m1 = current_date.strftime("%m")
    y1 = current_date.strftime("%Y")

    d2 = yesterday.strftime("%d")
    m2 = yesterday.strftime("%m")
    y2 = yesterday.strftime("%Y")

    d3 = two_days.strftime("%d")
    m3 = two_days.strftime("%m")
    y3 = two_days.strftime("%Y")

class files:
    log_file = f'_config/daily_errors.csv'
    count_file = f'_config/add_count.csv'

    file_csv = Path(f"_logs/scraped_{date_info.y1}_{date_info.m1}_{date_info.d1}.csv")
    file = Path(f"_logs/scraped_{date_info.y1}_{date_info.m1}_{date_info.d1}")
    file1 = Path(f"/logs/scraped_{date_info.y1}_{date_info.m1}_{date_info.d1}")
    file2 = Path(f"/logs/scraped_{date_info.y2}_{date_info.m2}_{date_info.d2}")
    file3 = Path(f"/logs/scraped_{date_info.y3}_{date_info.m3}_{date_info.d3}")

def log_adding(found_msg):
    logging.basicConfig(filename= files.count_file, level=logging.INFO,
                        format='%(asctime)s - %(message)s')

    logging.info(found_msg)

    with open(files.count_file, 'a', newline='', encoding="UTF-8") as csvfile:
        fieldnames = ['Timestamp', 'AddedMessage']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if csvfile.tell() == 0:
            writer.writeheader()

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow({'Timestamp': timestamp, 'AddedMessage': found_msg})

def daily_errors(error_msg):
    logging.basicConfig(filename=files.log_file, level=logging.ERROR,
                        format='%(asctime)s - %(message)s')

    logging.error(error_msg)

    with open(files.log_file, 'a', newline='', encoding="UTF-8") as csvfile:
        fieldnames = ['Timestamp', 'ErrorMessage']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if csvfile.tell() == 0:
            writer.writeheader()

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow({'Timestamp': timestamp, 'ErrorMessage': error_msg})

def login():
    global driver
    service = Service()
    driver = webdriver.Chrome(service=service)
    driver.get(TSDB.login)
    driver.find_element(By.NAME, "username").send_keys(TSDB.username)
    driver.find_element(By.NAME, "password").send_keys(TSDB.password)
    driver.find_element(By.XPATH, "/html/body/section/div/div/div[2]/form/div[4]/input").click()
    return

def scrape():
    channel_list = pd.read_csv('_config/channels.csv')

    data = []

    if files.file.is_file():
        pass
    else:
        blank = pd.DataFrame(list())
        blank.to_csv(f"{files.file}")
        clear_csv = open(f"{files.file}", 'w')
        clear_csv.truncate()

    filesize = os.path.getsize(f"{files.file}")

    if filesize > 1:
        df = pd.read_csv(f"{files.file}")
        print(f"Scrape has already been was conducted.")
        pass
    else:
        driver = webdriver.Chrome(options=options, service=service)
        for channel in channel_list["STG"]:
            try:
                driver.get(f"{STG.channel}{channel}")
                html = driver.page_source
                soup  = BeautifulSoup(html, features="lxml")
                for date in soup.select('.dateSeparator'):
                    for item in date.find_next_siblings():
                        if item.name == 'div':
                            break
                        text = tuple(item.stripped_strings)
                        if len(text) <= 3:
                            pass
                        else:
                            data.append({
                                'date':date.span.text.strip(),
                                'time':text[0],
                                'sport':text[1],
                                'teams':text[3] if len(text) > 4 else text[3].split(':')[-1],
                                'comp':text[4] if len(text) > 4 else text[3].split(':')[0],
                                'channel':channel 
                            })
            except AttributeError as ex:
                print(ex.Message)
                driver.navigate().refresh()

        df = pd.DataFrame(data)
        df.to_csv(f"_logs/scraped_{date_info.y1}_{date_info.m1}_{date_info.d1}.csv", mode="w", index=False, header=True)

        driver.quit()

def parse(sport):
    file_path = f"{files.file}.csv"
    if not os.path.isfile(file_path):
        sys.exit("No scrape has been conducted.")

    event_list = pd.read_csv(f"{files.file}.csv")
    team_list = pd.read_csv(f'{sport}/teams.csv')
    comp_list = pd.read_csv(f'{sport}/comps.csv')
    chan_list = pd.read_csv('_config/channels.csv')
    comps = comp_list['STG'].tolist()

    event_list['teams'] = event_list['teams'].str.replace(' vs. ', ' - ', regex=False)
    event_list[['ht', 'at']] = event_list['teams'].str.split(' - ', n=1, expand=True)
    event_list.drop(columns='teams', inplace=True)
    if 'Football' in sport:
        event_list = event_list[event_list['sport'].str.contains('Football')]
    else:
        event_list = event_list[event_list['sport'].str.contains(sport)]
    event_list = event_list[event_list['comp'].isin(comps)]
    event_list['date'] = pd.to_datetime(event_list['date'] + ' ' + str(date_info.yr) + ' ' + event_list['time'], format="mixed")
    event_list["date"] = event_list["date"].dt.date

    merge1 = event_list.merge(chan_list, left_on='channel', right_on='STG', how='left')
    merge2 = merge1.merge(comp_list, left_on='comp', right_on='STG', how='left')
    merge3 = merge2.merge(team_list, left_on='ht', right_on='STG', how='left')
    df = merge3.merge(team_list, left_on='at', right_on='STG', how='left', suffixes=["_1", "_2"])

    columns_to_drop = ['comp', 'channel', 'ht', 'at', 'STG_x', 'STG_y', 'STG_1', 'STG_2']
    df.drop(columns=columns_to_drop, inplace=True)
    df.dropna(subset=['date', 'TSDB_x', 'TSDB_1', 'TSDB_2'], inplace=True)

    df.columns = ["date", "time", "sport", "channel", "comp", "ht", "at"]
    df.sort_values(["date", "comp", "ht"], ascending=[True, True, True], inplace=True)
    df.drop_duplicates(subset=['channel', 'comp', 'ht', 'at'], keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv(f"{sport}/logs/scraped_{date_info.y1}_{date_info.m1}_{date_info.d1}_df.csv", mode="w", index=False, header=True)

    return

def dupe_check(sport):
    parse(sport)
    dfs = []

    file_paths = [
        f'{sport}{files.file1}_df.csv',
        f'{sport}{files.file2}_df.csv',
        f'{sport}{files.file3}_df.csv'
    ]

    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path)
            dfs.append(df)
        except FileNotFoundError:
            print(f"No file found at '{file_path}'...")

    if not dfs:
        print("No files found.")
        return None

    df2 = pd.concat(dfs, ignore_index=True)
    today = pd.Timestamp.today().date()

    df2 = df2[pd.to_datetime(df2['date']).dt.date >= today]
    df2.sort_values(["date", "comp", "ht"], ascending=[True, True, True], inplace=True)
    df2.drop_duplicates(subset=['date', 'channel', 'comp', 'ht', 'at'], keep=False, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    # print(df2)

    return df2

def add_tv(sport):
    df = dupe_check(sport)
    tv_added = 0
    tv_present = 0

    print(df)

    if len(df.index) == 0:
        print(f"No events found for {sport}.\n")
    else:
        login()

        for row in df.itertuples():
            try:
                api_call = requests.get(f"{TSDB.api}{row.comp}_{row.date}_{row.ht}_vs_{row.at}")
                storage = api_call.json()
                for match in storage["event"]:
                    pp = match["strPostponed"]
                    if pp == "yes":
                        print(f"*** {row.ht} vs {row.at} is postponed. ***\n")
                        pass
                    else:
                        idEvent = match["idEvent"]
                        driver.get(f"{TSDB.TV}{idEvent}")
                        driver.find_element(By.ID, "channel").send_keys(f"{row.channel}")
                        driver.find_element(By.NAME, "submit").click()

                        search = "TV Channel added to event"
                        source = driver.page_source
                        if search in source:
                            found_msg = f"*** {row.ht} vs {row.at} on {row.channel} added."
                            print(found_msg)
                            logging.info(found_msg)
                            tv_added += 1
                        if search not in source:
                            tv_present += 1

            except TypeError as t:
                error_msg = f"*** {row.ht} vs {row.at} on {row.channel} cannot be found. Error Code: {t.errno if hasattr(t, 'errno') else 'T'} ***"
                logging.error(error_msg)
                pass
            except KeyError as k:
                error_msg = f"*** {row.ht} vs {row.at} on {row.channel} cannot be found. Error Code: {t.errno if hasattr(k, 'errno') else 'K'} ***"
                logging.error(error_msg)
                pass
            except json.decoder.JSONDecodeError as j:
                error_msg = f"*** {row.ht} vs {row.at} on {row.channel} cannot be found. Error Code: {j.errno if hasattr(j, 'errno') else 'J'} ***"
                logging.error(error_msg)
                pass
            except NoSuchWindowException as w:
                error_msg = f"*** Windows close while adding {row.ht} vs {row.at} on {row.channel} Error Code: {w.errno if hasattr(w, 'errno') else 'W'} ***"
                print(f'{tv_added} {sport} events added.')
                logging.error(error_msg)

        driver.quit()

        print(f'\n{tv_added} {sport} events added.\n{tv_present} {sport} events already added.')