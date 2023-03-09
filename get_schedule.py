from datetime import datetime
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from x_league_utils import raise_html_status_code

def parse_x_league_schedule_website(url:str) -> pd.DataFrame():

    row_df = pd.DataFrame()
    finished_df = pd.DataFrame()

    game_id = 0
    game_status = ""

    game_stadium_id = 0
    game_stadium_name = ""
    
    away_team_id = ""
    away_team_name = ""
    away_team_score = 0
    
    home_team_id = ""
    home_team_name = ""
    home_team_score = 0

    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    response = requests.get(url,headers=headers)
    raise_html_status_code(response.status_code)
    
    soup = BeautifulSoup(response.text, features='lxml')

    scoreboxes = soup.find_all('div',{'class':'col-sm-6 col-xs-12'})
    for i in scoreboxes:
        
        data = i.find('tbody')
        data = data.find_all('tr')

        game_id = int(str(data[0].find('td',{'class':'status'}).find('a').get('href')).replace('https://xleague.jp/score/',''))
        game_status = data[0].find('td',{'class':'status'}).find('a').text
        
        game_stadium_id = int(str(data[2].find('a').get('href')).replace('https://xleague.jp/stadium/',''))
        game_stadium_name = data[2].find('a').text
        try:
            away_team_id = str(data[0].find('a').get('href')).replace('https://xleague.jp/team/','')
        except:
            away_team_id = None
        try:
            away_team_name = data[0].find('a').text
        except:
            away_team_name = None
        
        try:
            away_team_score = int(data[0].find('td',{'class':'gamescore'}).text)
        except:
            away_team_score = None
        
        try:
            home_team_id = str(data[1].find('a').get('href')).replace('https://xleague.jp/team/','')
        except:
            home_team_id = None
        
        try:
            home_team_name = data[1].find('a').text
        except:
            home_team_name = None
        
        try:
            home_team_score = int(data[1].find('td',{'class':'gamescore'}).text)
        except:
            home_team_score = None

        row_df = pd.DataFrame({
            'game_id':game_id,
            'game_status':game_status,
            'game_stadium_id':game_stadium_id,
            'game_stadium_name':game_stadium_name,
            'away_team_id':away_team_id,
            'away_team_name':away_team_name,
            'home_team_id':home_team_id,
            'home_team_name':home_team_name,
            'home_team_score':home_team_score,
            'away_team_score':away_team_score
        },
        index=[0]
        )

        finished_df = pd.concat([finished_df,row_df],ignore_index=True)
    time.sleep(5)
    #print(finished_df)
    return finished_df


def get_x_league_schedule(season:int) -> pd.DataFrame():
    
    current_year = int(datetime.now().year)
    row_df = pd.DataFrame()
    finished_df = pd.DataFrame()
    urls_arr = []
    week_desc_arr = []
    print(season)
    
    if season < 2019 or season > current_year:
        raise ValueError(f'The inputted season is invalid. Valid inputs are any integer between 2019 and {current_year}.')
    
    base_url = f"https://xleague.jp/schedule/{season}"
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    response = requests.get(base_url,headers=headers)
    raise_html_status_code(response.status_code)
    
    soup = BeautifulSoup(response.text, features='lxml')

    urls = soup.find_all('div',{'class':'tabs is-toggle is-fullwidth'})
    for i in urls:
        sub_urls = i.find('ul').find_all('a')

        for j in tqdm(sub_urls):
            sub = j.get('href')
            sub_str = j.text
            url = f'https://xleague.jp/{sub}'
            row_df = parse_x_league_schedule_website(url)
            row_df['season'] = season
            row_df['week_str'] = sub_str
            finished_df = pd.concat([finished_df,row_df],ignore_index=True)

    print(finished_df)
    return finished_df

    

def main():
    print('Starting up!')
    for i in range(2019,2023):
        print(i)
        df = get_x_league_schedule(i)
        df.to_csv(f'schedule/{i}_schedule.csv',index=False)

if __name__ == "__main__":
    main()