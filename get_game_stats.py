import time
import warnings
#from datetime import datetime

#import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
#from deep_translator import GoogleTranslator
from tqdm import tqdm

from x_league_utils import raise_html_status_code

warnings.simplefilter(action='ignore', category=FutureWarning)

def get_x_league_game_stats(game_id:int) -> pd.DataFrame():
    ## パス = Pass
    ## ラン = Run
    ## レシーブ = Receive (receiving)
    ## ファンブル = Fumble
    ## タックル = Tackle
    ## インターセプト = Intercept (Interceptions)
    ## フィールドゴール = Field Goal
    ## パント = Punt
    ## キックリターン = Kick Return
    ## パントリターン = Punt Return

    print(f'\nGetting data from game ID {game_id}')

    finished_df = pd.DataFrame()

    pass_df = pd.DataFrame()
    rush_df = pd.DataFrame()
    rec_df = pd.DataFrame()
    fumble_df = pd.DataFrame()
    tackle_df = pd.DataFrame()
    interceptions_df = pd.DataFrame()
    fg_df = pd.DataFrame()
    punt_df = pd.DataFrame()
    kr_df = pd.DataFrame()
    pr_df = pd.DataFrame()

    row_df = pd.DataFrame()

    url = f"https://xleague.jp/score/{game_id}"
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    response = requests.get(url,headers=headers)
    raise_html_status_code(response.status_code)
    
    soup = BeautifulSoup(response.text, features='lxml')

    scoreboard_head  = soup.find("div", {"class":"contentbody-socoreboad-head"})
    scoreboard_head = scoreboard_head.find_all("p",{"class":"info"})[0].text
    scoreboard_head = str(scoreboard_head).replace('\t','')
    game_day_time = scoreboard_head.split('\n')[1]
    game_day_location = scoreboard_head.split('\n')[2]
    
    del scoreboard_head

    scorebox = soup.find("div", {"class":"contentbody-socoreboad-body"})
    scorebox = scorebox.find("tbody")
    scorebox = scorebox.find_all("th")
    away_team = scorebox[0].text
    away_team_abv = str(scorebox[0].find("a").get("href")).replace('https://xleague.jp/team/','')
    home_team = scorebox[1].text
    home_team_abv = str(scorebox[1].find("a").get("href")).replace('https://xleague.jp/team/','')

    scorebox = soup.find("div", {"class":"contentbody-socoreboad-body"})
    scorebox = scorebox.find("tbody")
    #scorebox = scorebox.find_all("tr")
    
    
    score = scorebox.find_all("td")
    away_score = int(score[5].text)
    home_score = int(score[11].text)
    #print(away_score,home_score)
    
    del scorebox
    
    full_stat_tables = soup.find_all("div", {"class":"col-sm-6 col-xs-12"})

    count = 0
    for i in full_stat_tables:
        count += 1
        #print(i)
        #table_soup = BeautifulSoup(i.text, features='lxml')
        tables = i.find_all("table")
        for j in tables:
            
            team = ""
            team_abv = ""
            team_score = 0
            opponent = ""
            opponent_abv = ""
            opponent_score = 0
            result = "U"
            score_dif = 0

            if count == 1: ## team = Away Team
                team = away_team
                team_abv = away_team_abv
                team_score = away_score
                opponent = home_team
                opponent_abv = home_team_abv
                opponent_score = home_score
            elif count == 2: ## team = Home Team
                team = home_team
                team_abv = home_team_abv
                team_score = home_score
                opponent = away_team
                opponent_abv = away_team_abv
                opponent_score = away_score
            else: ## This should never happen. If it does, raise an exception
                raise ValueError("There are more than 2 teams playing a game of American Football.")
            
            score_dif = team_score - opponent_score

            if score_dif > 0:
                result = "W"
            elif score_dif <0:
                result = "L"
            elif score_dif == 0:
                result = "T"
            else: ## This should never happen. If it does, raise an exception
                raise SystemError("There is a very serious problem with your system's ability to process numbers. "+\
                "Restart your computer, and if you continue to see this error, consider replacing this computer.")
            

            
            row_df = pd.DataFrame(columns=['game_id','game_day_time','game_day_location','team','team_abv','opponent','opponent_abv','team_score','opponent_score','score_dif','result','player_name_en'],\
                data=[[game_id,game_day_time,game_day_location,team,team_abv,opponent,opponent_abv,team_score,opponent_score,score_dif,result,None]])
            
            cols = []
            t_head = j.find("thead")
            t_head = t_head.find_all("th")
            for k in t_head:
                cols.append(k.text)

            table_type = cols[0]
            cols[0] = 'player_name_jp'
            rows = j.find("tbody")
            rows = rows.find_all("tr")

            for k in rows:
                row_columns = ['game_id','game_day_time','game_day_location','team','team_abv','opponent','opponent_abv','team_score','opponent_score','score_dif','result','player_name_en']
                row_data = [game_id,game_day_time,game_day_location,team,team_abv,opponent,opponent_abv,team_score,opponent_score,score_dif,result,None]
                
                row_columns += cols
                row_data.append(k.find("th").text)

                for x in k.find_all("td"):
                    data = str(x.text)
                    row_data.append(data)
                    del data
                #print('')
                row_df = pd.DataFrame(columns=row_columns,data=[row_data])
                row_df = row_df.replace(r'^\s*$',None,regex=True)
                match table_type:
                    case "パス": ## パス = Pass
                        pass_df = pd.concat([pass_df,row_df],ignore_index=True)
                    case "ラン": ## ラン = Run
                        rush_df = pd.concat([rush_df,row_df],ignore_index=True)
                    case "レシーブ": ## レシーブ = Receive (receiving)
                        rec_df = pd.concat([rec_df,row_df],ignore_index=True)
                    case "ファンブル": ## ファンブル = Fumble
                        fumble_df = pd.concat([fumble_df,row_df],ignore_index=True)
                    case "タックル": ## タックル = Tackle
                        tackle_df = pd.concat([tackle_df,row_df],ignore_index=True)
                    case "インターセプト": ## インターセプト = Intercept (Interceptions)
                        interceptions_df = pd.concat([interceptions_df,row_df],ignore_index=True)
                    case "フィールドゴール": ## フィールドゴール = Field Goal
                        fg_df = pd.concat([fg_df,row_df],ignore_index=True)
                    case "パント": ## パント = Punt
                        punt_df = pd.concat([punt_df,row_df],ignore_index=True)
                    case "キックリターン": ## キックリターン = Kick Return
                        kr_df = pd.concat([kr_df,row_df],ignore_index=True)
                    case "パントリターン": ## パントリターン = Punt Return
                        pr_df = pd.concat([pr_df,row_df],ignore_index=True)
                    case default:
                        raise SyntaxError(f'Unhandled table type. Table type is {table_type}')

                del row_df,row_columns,row_data

    ######################################################################################################################################################################################
    ## Parse Passing Stats
    ######################################################################################################################################################################################
    pass_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'COMP', 'ATT', 'COMP%', 'PASS_YDS', 'PASS_YPA', 'PASS_TD', 'PASS_INT', 'PASS_LONG', 'SACKED','SACKED_YDS','NFL_QBR']
    
    pass_df[['Yds','TD','Lng']] = pass_df[['Yds','TD','Lng']].fillna(0)
    pass_df[['COMP','ATT']] = pass_df['Comp/Att'].str.split("/",expand=True)
    pass_df = pass_df.drop(columns=['Comp/Att'])
    pass_df[['COMP','ATT']] = pass_df[['COMP','ATT']].replace('',0)
    pass_df = pass_df.astype({'COMP':'int32','ATT':'int32','Yds':'int32','Lng':'int32'})
    pass_df['COMP%'] = round(pass_df['COMP'] / pass_df['ATT'],4)
    pass_df['PASS_YPA'] = round(pass_df['Yds']/pass_df['ATT'],3)
    pass_df = pass_df.rename(columns={'Yds':'PASS_YDS','TD':'PASS_TD','Lng':'PASS_LONG'})
    pass_df = pass_df.reindex(columns=pass_cols)
    
    del pass_cols

    ######################################################################################################################################################################################
    ## Parse Rushing Stats
    ######################################################################################################################################################################################
    rush_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'RUSH','RUSH_YDS','RUSH_AVG','RUSH_TD','RUSH_LONG']
    rush_df[['Run','Yds','TD','Lng']] = rush_df[['Run','Yds','TD','Lng']].fillna(0)
    rush_df = rush_df.astype({'Run':'int32','Yds':'int32','TD':'int32','Lng':'int32'})
    rush_df['RUSH_AVG'] = round(rush_df['Yds']/rush_df['Run'],3)
    rush_df = rush_df.rename(columns={'Run':'RUSH','Yds':'RUSH_YDS','TD':'RUSH_TD','Lng':'RUSH_LONG'})
    rush_df = rush_df.reindex(columns=rush_cols)

    del rush_cols

    finished_df = pd.merge(
        pass_df,
        rush_df,
        left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        how='outer'
    )

    del pass_df,rush_df

    ######################################################################################################################################################################################
    ## Parse Receiving Stats
    ######################################################################################################################################################################################
    rec_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'TARGETS','REC','REC_YDS','REC_AVG','REC_TD','REC_LONG','CATCH%']
    rec_df[['Rec','Yds','TD','Lng']] = rec_df[['Rec','Yds','TD','Lng']].fillna(0)
    rec_df = rec_df.astype({'Rec':'int32','Yds':'int32','TD':'int32','Lng':'int32'})
    rec_df['REC_AVG'] = rec_df['Yds'] / rec_df['Rec']
    rec_df = rec_df.rename(columns={'Rec':'REC','Yds':'REC_YDS','TD':'REC_TD','Lng':'REC_LONG'})
    rec_df = rec_df.reindex(columns=rec_cols)

    del rec_cols

    finished_df = finished_df.merge(
        rec_df,
        left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        how='outer'
    )

    del rec_df

    ######################################################################################################################################################################################
    ## Parse Fumble Stats
    ######################################################################################################################################################################################
    if len(fumble_df) > 0:
        fum_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
            'FUMBLES','FUMBLES_LOST','FF','FR','FR_YDS','FR_TD']
        fumble_df[['F','Lost','Fr','Yds']] = fumble_df[['F','Lost','Fr','Yds']].fillna(0)
        fumble_df = fumble_df.rename(columns={'F':'FUMBLES','Lost':'FUMBLES_LOST','Fr':'FR','Yds':'FR_YDS'})
        fumble_df = fumble_df.reindex(columns=fum_cols)

        del fum_cols

        finished_df = finished_df.merge(
            fumble_df,
            left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            how='outer'
        )

        del fumble_df

    ######################################################################################################################################################################################
    ## Parse Tackle Stats
    ######################################################################################################################################################################################
    tackle_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'COMB','SOLO','AST','SACKS','SACKS_YDS','TFL','TFL_YDS']
    tackle_df[['Tack','Sack','SackYds']] = tackle_df[['Tack','Sack','SackYds']].fillna(0)
    tackle_df = tackle_df.rename(columns={'Tack':'COMB','Sack':'SACKS','SackYds':'SACKS_YDS'})
    tackle_df = tackle_df.reindex(columns=tackle_cols)

    del tackle_cols

    finished_df = finished_df.merge(
        tackle_df,
        left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        how='outer'
    )

    del tackle_df

    ######################################################################################################################################################################################
    ## Parse INT Stats
    ######################################################################################################################################################################################
    if len(interceptions_df) > 0:
        int_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
            'INT','INT_YDS','INT_TD','INT_LONG','PD']
        interceptions_df[['Int','Yds','TD','Lng']] = interceptions_df[['Int','Yds','TD','Lng']].fillna(0)
        interceptions_df = interceptions_df.rename(columns={'Int':'INT','Yds':'INT_YDS','TD':'INT_TD','Lng':'INT_LONG'})
        interceptions_df = interceptions_df.reindex(columns=int_cols)

        del int_cols

        finished_df = finished_df.merge(
            interceptions_df,
            left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv','team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            right_on=['game_id', 'game_day_time', 'game_day_location','team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            how='outer'
        )

        del interceptions_df

    ######################################################################################################################################################################################
    ## Parse FG Stats
    ######################################################################################################################################################################################
    if len(fg_df) > 0:
        fg_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
            'FGA','FGM','FG%','FG_LONG','XPA','XPM','XP%']
        #rec_df = rec_df.astype({'Rec':'int32','Yds':'int32','TD':'int32','Lng':'int32'})
        fg_df[['FGA','FGM','Yds','Lng']] = fg_df[['FGA','FGM','Yds','Lng']].fillna(0)
        fg_df = fg_df.astype({'FGM':'int32','FGA':'int32','Yds':'int32','Lng':'int32'})
        fg_df['FG%'] = round(fg_df['FGM'] / fg_df['FGA'],3)
        fg_df = fg_df.rename(columns={'Lng':'FG_LONG'})
        fg_df = fg_df.reindex(columns=fg_cols)

        del fg_cols

        finished_df = finished_df.merge(
            fg_df,
            left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            how='outer'
        )

        del fg_df

    ######################################################################################################################################################################################
    ## Parse Punting Stats
    ######################################################################################################################################################################################
    punt_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'PUNTS','PUNT_GROSS_YDS','PUNT_GROSS_AVG','PUNT_NET_YDS','PUNT_NET_AVG','PUNT_TB','PUNT_IN20','PUNT_LONG']
    punt_df[['Punt','Yds','PNY','Lng']] = punt_df[['Punt','Yds','PNY','Lng']].fillna(0)
    punt_df = punt_df.astype({'Punt':'int32','Yds':'int32','PNY':'int32','Lng':'int32'})
    punt_df['PUNT_GROSS_AVG'] = round(punt_df['Yds'] / punt_df['Punt'],3)
    punt_df['PUNT_NET_AVG'] = round(punt_df['PNY'] / punt_df['Punt'],3)
    punt_df = punt_df.rename(columns={'Punt':'PUNTS','Yds':'PUNT_GROSS_YDS','PNY':'PUNT_NET_YDS','Lng':'PUNT_LONG'})
    punt_df = punt_df.reindex(columns=punt_cols)

    del punt_cols

    finished_df = finished_df.merge(
        punt_df,
        left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        how='outer'
    )

    del punt_df

    ######################################################################################################################################################################################
    ## Parse Punt Returning Stats
    ######################################################################################################################################################################################
    if len(pr_df) > 0:
        pr_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
            'PR','PR_YDS','PR_AVG','PR_TD','PR_LONG']
        pr_df[['PR','Yds','TD','Lng']] = pr_df[['PR','Yds','TD','Lng']].fillna(0)
        pr_df = pr_df.astype({'PR':'int32','Yds':'int32','TD':'int32','Lng':'int32'})
        pr_df['PR_AVG'] = round(pr_df['Yds'] / pr_df['PR'],3)
        pr_df = pr_df.rename(columns={'Yds':'PR_YDS','PNY':'PUNT_NET_YDS','Lng':'PUNT_LONG'})
        pr_df = pr_df.reindex(columns=pr_cols)

        del pr_cols

        finished_df = finished_df.merge(
            pr_df,
            left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
            how='outer'
        )

        del pr_df

    ######################################################################################################################################################################################
    ## Parse Kick Returning Stats
    ######################################################################################################################################################################################
    kr_cols = ['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp', \
        'KR','KR_YDS','KR_AVG','KR_TD','KR_LONG']
    kr_df[['KR','Yds','TD','Lng']] = kr_df[['KR','Yds','TD','Lng']].fillna(0)

    kr_df = kr_df.astype({'KR':'int32','Yds':'int32','TD':'int32','Lng':'int32'})
    kr_df['KR_AVG'] = round(kr_df['Yds'] / kr_df['KR'],3)
    kr_df = kr_df.rename(columns={'Yds':'KR_YDS','PNY':'PUNT_NET_YDS','Lng':'PUNT_LONG'})
    kr_df = kr_df.reindex(columns=kr_cols)

    del kr_cols

    finished_df = finished_df.merge(
        kr_df,
        left_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        right_on=['game_id', 'game_day_time', 'game_day_location', 'team','team_abv','opponent','opponent_abv', 'team_score', 'opponent_score', 'score_dif', 'result', 'player_num', 'player_name_en', 'player_name_jp'],
        how='outer'
    )

    del kr_df
    time.sleep(1)
    #finished_df['player_name_en'].apply(lambda x: GoogleTranslator(source='ja',target='en').translate(x) if x != np.NaN else None)
    return finished_df

def get_season_game_stats(season:int):

    finished_df = pd.DataFrame()
    row_df = pd.DataFrame()

    schedule_df = pd.read_csv(f'schedule/{season}_schedule.csv')
    schedule_df = schedule_df[schedule_df['game_status'] != '中止']
    game_ids_arr = schedule_df['game_id'].to_list()
    print(season)
    for i in tqdm(game_ids_arr):
        game_id = int(i)
        row_df = get_x_league_game_stats(game_id)
        row_df.to_csv(f'game_stats/raw/csv/{season}_{game_id}.csv',index=False)
        finished_df = pd.concat([finished_df,row_df],ignore_index=True)
        del row_df

    return finished_df

def main():
    for i in range(2020,2023):
        get_season_game_stats(i)

if __name__ == "__main__":
    main()