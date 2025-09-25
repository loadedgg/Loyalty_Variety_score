import os
import gc
import time
import json
import logging
import psycopg2
import requests
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from variety_score import df_1, df_5
from loyalty import df_2, process_df_6, calculate_loyalty_score, db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_slack_message(message):
    load_dotenv()
    webhook_url = os.getenv("webhook_url")
    payload = {
        "text": message
    }
    response = requests.post(webhook_url, data=json.dumps(payload))
    if response.status_code != 200:
        logging.error("Failed to send message to Slack: %s", response.text)

def function_call():

    # Loyalty score
    df_2_result = df_2()
    gc.collect()     # Trigger garbage collection to clean up any unused memory
    df_6_result = process_df_6(df_2_result)
    gc.collect()    # Trigger garbage collection to clean up any unused memory
    loyalty = calculate_loyalty_score(df_6_result)
    gc.collect()    # Trigger garbage collection to clean up any unused memory

    # Variety score
    df_4_result = df_1()
    gc.collect()    # Trigger garbage collection to clean up any unused memory
    variety = df_5(df_2_result, df_4_result)
    gc.collect()    # Trigger garbage collection to clean up any unused memory

    return loyalty, variety

def process_final_data(loyalty, variety):

    final_df = variety.merge(loyalty, on='name', how='left')
    final_df = final_df[final_df['loyalty_category'].notnull()]

    # Add 'variety_cat' column
    conditions = [
        (final_df['variety_game_score'] > 0.75),
        (final_df['variety_game_score'] > 0.5) & (final_df['variety_game_score'] <= 0.75),
        (final_df['variety_game_score'] > 0.25) & (final_df['variety_game_score'] <= 0.5),
        (final_df['variety_game_score'] <= 0.25)
    ]

    # Correct 'variety_cat' classification by flipping the conditions
    choices = [ 'Very Variety', 'Moderate Variety', 'Mostly One Category', 'One Category']
    final_df['variety_cat'] = np.select(conditions, choices, default='Unknown')

    # Selecting required columns from final_df
    selected_columns = [
        'twitch_channel_id', 'name', 'lang', 'acv', 'pct_shooter_airtime', 
        'genre_rank_1', 'game_rank_1', 'variety_game_score', 
        'final_loyalty_score', 'variety_cat', 'loyalty_category'
    ]

    return final_df[selected_columns]

def create_redshift_connection():
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT")
    )
    logging.info("Connected to Redshift successfully!")

    return conn

def insert_to_twitch(df):
    """Truncate table and insert DataFrame into PostgreSQL table."""
    engine = db_connection()
    table_name = 'loyalty_variety_scores'

    # Truncate the table before inserting new data
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_name};"))
        connection.commit()
    logging.info(f"Table {table_name} truncated successfully from Twitch.")

    # Insert DataFrame into the table
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logging.info(f"Data inserted successfully into {table_name} table in Twitch.")

# Function to insert DataFrame into Redshift
def insert_data_to_redshift(df):
    conn = create_redshift_connection()

    insert_query = """
    INSERT INTO loyalty_variety_scores ( twitch_channel_id, name, lang, acv, pct_shooter_airtime, genre_rank_1, 
                                 game_rank_1, variety_game_score, final_loyalty_score, 
                                 variety_cat, loyalty_category)
    VALUES %s
    """
    
    # Convert DataFrame rows to list of tuples
    data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

    with conn.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE loyalty_variety_scores;")
        conn.commit()
        logging.info(f"The table was truncated from Redshift before loading the data!")
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        logging.info(f"Inserted {len(df)} rows into Redshift!")
    conn.close()

if __name__ == "__main__":
    try:
        start_time = time.time()

        loyalty, variety = function_call()
        final_data = process_final_data(loyalty, variety)

        insert_data_to_redshift(final_data)
        insert_to_twitch(final_data)

        end_time = time.time()
        elapsed_time_minutes = (end_time - start_time) / 60
        logging.info(f"The script ran for {elapsed_time_minutes:.2f} minutes.")
        success_message = (
            f"\U00002705 Success! \n\n"
            f"       Name: loyalty_variety_scores \n"
            f"       script_execution_time: {elapsed_time_minutes} Minutes \n"
            f"       Status: The data has been successfully loaded into table. \n"
            )
        send_slack_message(success_message)

    except Exception as e:
        failure_message = (
            f"\u274C Alert! \n\n"
            f"       Name: loyalty_variety_scores \n"
            f"       Error: {e} \n"
        )
        send_slack_message(failure_message)
        logging.error("Error:", e)
