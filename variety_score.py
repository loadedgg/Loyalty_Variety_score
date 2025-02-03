import pandas as pd
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import numpy as np
from loyalty import execute_query

def df_1():
    query = """
    SELECT 
        sp.twitch_channel_id, 
        sp.twitch_game_id, 
        c.name AS channel_name, 
        g.title AS game_title, 
        SUM(sp.viewers) / 6 AS hours_watched
    FROM stream_peeks sp
    JOIN channels c ON c.twitch_channel_id = sp.twitch_channel_id
    JOIN games g ON g.twitch_game_id = sp.twitch_game_id
    WHERE sp.pulled_at >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY sp.twitch_channel_id, sp.twitch_game_id, c.name, g.title
    """

    # Step : read the SQL query
    data = execute_query(query)

    data['percentage_played'] = data.groupby('twitch_channel_id')['hours_watched'].transform(lambda x: x / x.sum())
    data['percentage_played_sq'] = data['percentage_played'] ** 2

    result = data[data['game_title'].notnull()]

    pd.options.display.float_format = '{:.6f}'.format

    df_1 = result.rename(
    columns={
        'channel_name': 'name',
            'game_title': 'title'
        }
    )[['twitch_channel_id', 'twitch_game_id', 'name', 'title', 
        'hours_watched', 'percentage_played', 'percentage_played_sq']]
    
    print(df_1.shape[0])

    df = df_4(df_1)
    
    return df

def df_4(df_1):
    
    # Step : Load genres data from CSV
    # genres = pd.read_csv("/app/game_genres.csv")  # File with columns: Game, Primary Genre
    genres = pd.read_csv("game_genres.csv")
    genre_playtime = pd.merge(df_1, genres, how='left', left_on='title', right_on='Game')

    genre_playtime = genre_playtime[genre_playtime['Primary Genre'].notnull()]
    genre_playtime['genre'] = genre_playtime['Primary Genre']  # Rename Primary Genre to genre

    genre_playtime['sum_hours_watched'] = genre_playtime.groupby('twitch_channel_id')['hours_watched'].transform('sum')
    genre_playtime['genre_percentage_played'] = genre_playtime['hours_watched'] / genre_playtime['sum_hours_watched']
    genre_playtime['genre_percentage_played_sq'] = genre_playtime['genre_percentage_played'] ** 2
    genre_percentages = genre_playtime.groupby(['name', 'genre'], as_index=False).agg(
        genre_percentage_played=('genre_percentage_played', 'sum')
    )
    genre_percentages['genre_percentage_played_sq'] = genre_percentages['genre_percentage_played'] ** 2

    genre_percentages['genre_rank'] = genre_percentages.groupby('name')['genre_percentage_played'].rank(method='first', ascending=False)

    genre_rank_schema = genre_percentages.pivot_table(index='name', 
                                                    columns='genre_rank', 
                                                    values='genre', 
                                                    aggfunc='first')
    genre_rank_schema = genre_rank_schema.rename(columns={1: 'genre_rank_1', 2: 'genre_rank_2', 3: 'genre_rank_3'})

    variety_scores = genre_percentages.groupby('name', as_index=False).agg(
        variety_genre_score=('genre_percentage_played_sq', 'sum')
    )

    game_playtime = df_1[df_1['title'].notnull()]
    game_playtime['sum_hours_watched'] = game_playtime.groupby('twitch_channel_id')['hours_watched'].transform('sum')
    game_playtime['game_percentage_played'] = game_playtime['hours_watched'] / game_playtime['sum_hours_watched']
    game_playtime['game_percentage_played_sq'] = game_playtime['game_percentage_played'] ** 2

    game_percentages = game_playtime.groupby(['name', 'title'], as_index=False).agg(
        game_percentage_played=('game_percentage_played', 'sum'),
        game_percentage_played_sq=('game_percentage_played_sq', 'sum')
    )

    game_percentages['game_rank'] = game_percentages.groupby('name')['game_percentage_played'].rank(method='first', ascending=False)

    game_rank_schema = game_percentages.pivot_table(index='name', 
                                                columns='game_rank', 
                                                values='title', 
                                                aggfunc='first')
    game_rank_schema = game_rank_schema.rename(columns={i: f'game_rank_{i}' for i in range(1, 11)})

    game_variety_scores = game_percentages.groupby('name', as_index=False).agg(
        variety_game_score=('game_percentage_played_sq', 'sum')
    )

    final_result = pd.merge(variety_scores, game_variety_scores, on='name', how='left')
    final_result = pd.merge(final_result, genre_rank_schema, on='name', how='left')
    final_result = pd.merge(final_result, game_rank_schema, on='name', how='left')

    # Step : Keep only the required columns
    df_4 = final_result[[
        'name',
        'variety_game_score',
        'game_rank_1', 'game_rank_2', 'game_rank_3', 'game_rank_4', 'game_rank_5',
        'game_rank_6', 'game_rank_7', 'game_rank_8', 'game_rank_9', 'game_rank_10',
        'variety_genre_score',
        'genre_rank_1', 'genre_rank_2', 'genre_rank_3'
    ]]

    return df_4

def df_5(df_2, df_4):
    df = df_2.merge(df_4, on='name', how='left')

    query = """
        select twitch_channel_id,
        avg(viewers) as acv
        from stream_peeks
        where pulled_at >= CURRENT_DATE - INTERVAL '90 days'
        group by twitch_channel_id 
        """

    # Step : Use pandas to read the SQL query result directly into a DataFrame
    df_5 = execute_query(query)

    df_f = df.merge(df_5, on='twitch_channel_id', how='left')
    df = df_f_fun(df_f)

    return df

def df_f_fun(df_f):
    def categorize_shooter(pct_shooter_airtime):
        if 0.2 <= pct_shooter_airtime <= 0.5:
            return 'enthusiast_2050'
        elif pct_shooter_airtime > 0.5:
            return 'shooter50plus'
        elif 0.0001 <= pct_shooter_airtime < 0.2:
            return 'nonshooter1to20'
        elif pct_shooter_airtime == 0:
            return 'never_played_shooter'
        else:
            return None 

    df_f['shooter_group'] = df_f['pct_shooter_airtime'].apply(categorize_shooter)

    # Selecting required columns
    selected_columns = [
        
        'twitch_channel_id', 'name', 'acv', 'lang', 'shooter_group',
        'pct_shooter_airtime', 'pct_nonshooter_airtime', 'pct_non_gaming', 'total_airtime',
        'variety_game_score', 'genre_rank_1', 'genre_rank_2', 'genre_rank_3',
        'game_rank_1', 'game_rank_2', 'game_rank_3', 'game_rank_4', 'game_rank_5',
        'game_rank_6', 'game_rank_7', 'game_rank_8', 'game_rank_9', 'game_rank_10'
    ]

    return df_f[selected_columns]
    