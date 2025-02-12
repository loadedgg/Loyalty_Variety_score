import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
import numpy as np

def db_connection():
    """Establish SSH tunnel & database connection."""
    load_dotenv()

    ssh_host = os.getenv("ssh_host")
    ssh_port = int(os.getenv("ssh_port"))
    ssh_username = os.getenv("ssh_username")
    ssh_private_key_path = os.getenv("ssh_private_key_path")

    db_host = os.getenv("prod_host")
    db_port = int(os.getenv("prod_port"))
    db_user = os.getenv("prod_user")
    db_password = os.getenv("prod_password")
    db_name = os.getenv("prod_db_name")

    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_pkey=ssh_private_key_path,
        remote_bind_address=(db_host, db_port)
    )
    
    tunnel.start()
    print(f"Tunnel established at localhost:{tunnel.local_bind_port}")

    db_url = f'postgresql://{db_user}:{db_password}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{db_name}'
    engine = create_engine(db_url, echo=False)
    
    return engine, tunnel

def execute_query(query):
    """Execute a SQL query and return a Pandas DataFrame."""
    engine, tunnel = db_connection()

    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, connection)  
            return result
    finally:
        engine.dispose()
        tunnel.stop()
        print("Database connection closed.")

def df_2():
    """Process gaming data and compute shooter/non-shooter metrics."""
   
    stream_peeks_query = text("""
        SELECT a.twitch_channel_id, channels.name, channels.language AS lang, 
               a.twitch_game_id, games.title, AVG(viewers) AS acv, 
               COUNT(*)::decimal/6 AS airtime, 
               (COUNT(*)::decimal/6) * AVG(viewers) AS hours_watched
        FROM stream_peeks a
        LEFT JOIN games ON games.twitch_game_id = a.twitch_game_id
        LEFT JOIN channels ON channels.twitch_channel_id = a.twitch_channel_id 
        WHERE a.pulled_at >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY a.twitch_channel_id, a.twitch_game_id, games.title, channels.name, channels.language
    """)

    data = execute_query(stream_peeks_query)
    print(f"Row count: {data.shape[0]}")
    
    # Define Game Categories
    nongaming_title = {'Just Chatting'}
    shooter_titles = {
        'Valorant', 'Fortnite', 'Apex Legends', 'Counter-Strike: Global Offensive',
        'Escape From Tarkov', 'Overwatch 2', "PLAYERUNKNOWN'S BATTLEGROUNDS",
        "Tom Clancy's Rainbow Six: Siege", 'Rust', 'Call of Duty: Modern Warfare II',
        'DayZ', 'Destiny 2', 'HELLDIVERS II', 'Call of Duty: Black Ops 6',
        'Deadlock', 'Counter-Strike', 'Call of Duty: Warzone',
        "Tom Clancy's Rainbow Six Siege", 'Escape from Tarkov: Arena'
    }

    # Precompute Shooter/Non-Shooter Flags
    data['is_shooter'] = data['title'].isin(shooter_titles)
    data['is_nongaming'] = data['title'].isin(nongaming_title)

    # Aggregations
    grouped = data.groupby(['twitch_channel_id', 'name', 'lang']).agg(
        nongaming_airtime=('airtime', lambda x: x[data.loc[x.index, 'is_nongaming']].sum()),
        shooter_airtime=('airtime', lambda x: x[data.loc[x.index, 'is_shooter']].sum()),
        nongaming_hw=('hours_watched', lambda x: x[data.loc[x.index, 'is_nongaming']].sum()),
        shooter_hw=('hours_watched', lambda x: x[data.loc[x.index, 'is_shooter']].sum()),
        total_airtime=('airtime', 'sum'),
        total_hw=('hours_watched', 'sum')
    ).reset_index()

    grouped['pct_shooter'] = grouped['shooter_hw'].fillna(0) / grouped['total_hw']
    grouped['pct_nonshooter'] = 1 - grouped['pct_shooter']
    grouped['pct_non_gaming'] = grouped['nongaming_hw'].fillna(0) / grouped['total_hw']
    grouped['pct_shooter_airtime'] = grouped['shooter_airtime'].fillna(0) / grouped['total_airtime']
    grouped['pct_nonshooter_airtime'] = 1 - grouped['pct_shooter_airtime']
    df_final = grouped[grouped['total_airtime'] > 30]

    return df_final

def get_date_batches(start_date, intervals=5, days_per_interval=90 // 5):
    """Generate start & end date pairs for batching queries."""
    return [
        (
            (start_date - timedelta(days=(i + 1) * days_per_interval - 1)).strftime('%Y-%m-%d'),
            (start_date - timedelta(days=i * days_per_interval)).strftime('%Y-%m-%d')
        )
        for i in range(intervals)
    ]

def fetch_df_6(start_date, end_date):
    """Fetch gaming data for given date range."""
    print(f"Fetching data from {start_date} to {end_date}")
    
    query = text(f"""
        SET TIME ZONE 'America/Los_Angeles';
        SELECT
            channels.name,
            games.title,
            stream_peeks.twitch_channel_id,
            stream_peeks.viewers,
            stream_peeks.twitch_game_id
        FROM stream_peeks
        JOIN channels ON channels.twitch_channel_id = stream_peeks.twitch_channel_id
        JOIN games ON games.twitch_game_id = stream_peeks.twitch_game_id
        WHERE stream_peeks.pulled_at BETWEEN '{end_date} 00:00:00' AND '{start_date} 23:59:59'
        ORDER BY channels.name;
    """)

    return execute_query(query)

def process_df_6(df_2):
    """Process df_6 with batching, outlier removal, and filtering."""
    today = datetime.today()
    periods = get_date_batches(today)

    df_6 = pd.DataFrame()
    for start, end in periods:
        period_data = fetch_df_6(end, start)
        df_6 = pd.concat([df_6, period_data], ignore_index=True)

    print(f"Total Row count after fetching: {df_6.shape[0]}")

    # Load df_2 for filtering
    df_6 = df_6[df_6['name'].isin(df_2['name'].tolist())].reset_index(drop=True)
    print(f"Filtered df_6 shape: {df_6.shape}")

    def percentile(n):
        """Return a function that computes the nth percentile."""
        def percentile_(x):
            return np.percentile(x, n)
        percentile_.__name__ = f'percentile_{n}'
        return percentile_

    df_stats = df_6.groupby('name')['viewers'].agg(
        iqr=lambda x: np.percentile(x, 75) - np.percentile(x, 25),
        percentile_25=percentile(25),
        percentile_75=percentile(75)
    ).reset_index()

    # Merge statistics back to df_6
    df_6 = df_6.merge(df_stats, on='name')

    df_6['upper_bound'] = df_6['percentile_75'] + (3 * df_6['iqr'])
    df_6['lower_bound'] = df_6['percentile_25'] - (3 * df_6['iqr'])

    df_6_filtered = df_6[
        (df_6['viewers'] <= df_6['upper_bound']) & (df_6['viewers'] >= df_6['lower_bound'])
    ]

    df_6_final = df_6_filtered[['name', 'title', 'twitch_channel_id', 'viewers', 'twitch_game_id']]
    print(f"Final df_6 shape after outlier removal: {df_6_final.shape}")

    return df_6_final

def calculate_loyalty_score(df_removed_outliers):

    final_df = df_removed_outliers[['name', 'title', 'viewers']]
    first_group = final_df.groupby(['name', 'title'])['viewers'].agg(['mean', 'count']).reset_index()
    print(f"Shape after first grouping: {first_group.shape}")
 
    first_group = first_group[first_group['count'] > 18]
    print(f"Shape after filtering: {first_group.shape}")

    second_group = first_group.groupby(['name'])['mean'].agg(['mean', 'std']).reset_index()
    second_group.columns = ['name', 'mean', 'std']
    second_group['score'] = second_group['mean'] / second_group['std']
    final_group = second_group[['name', 'mean', 'std', 'score']]
    print(f"Shape after second grouping: {final_group.shape}")

    # Step : Handling NaN in score
    final_group = final_group.dropna(subset=['score'])
    
    # Step : Calculating final loyalty score
    final_group['final_loyalty_score'] = (final_group['score'] - 3.119576806) / 1.62
    final_group = final_group.dropna(subset=['final_loyalty_score'])

    # Inverting final_loyalty_score to ensure higher scores indicate more loyalty
    final_group['final_loyalty_score'] = 1 - final_group['final_loyalty_score']
    
    unique_scores = final_group['final_loyalty_score'].nunique()

    if unique_scores >= 3:
        try:
            final_group['loyalty_category'] = pd.qcut(final_group['final_loyalty_score'], q=[0, 0.2, 0.5, 1], labels=['Low', 'Medium', 'High'])
        except ValueError:
            final_group['loyalty_category'] = pd.qcut(final_group['final_loyalty_score'], q=[0, 0.2, 0.5, 1], labels=['Low', 'Medium', 'High'], duplicates='drop')
    else:
        print("Not enough unique loyalty scores, adjusting to two categories.")
        final_group['loyalty_category'] = pd.cut(final_group['final_loyalty_score'], bins=2, labels=['Low', 'High'])

    final_group = final_group[['name', 'loyalty_category', 'mean', 'std', 'score', 'final_loyalty_score']]
    
    loyalty = final_group[['name','loyalty_category','final_loyalty_score']]
    print(f"Final group shape: {loyalty.shape}")
    
    return loyalty
