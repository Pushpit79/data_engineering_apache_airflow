from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'genre_parquet_pipeline',
    default_args=default_args,
    description='A simple pipeline to split a Parquet file into multiple Parquet files by genre',
    schedule_interval=timedelta(days=1),
)

def split_parquet_by_genre():
    # Step 1: Read the existing Parquet file
    df = pd.read_parquet('task1.parquet')

    # Step 2: Create a folder called 'genres'
    os.makedirs('genres', exist_ok=True)

    # Step 3: Explode the 'genres' column so each genre gets its own row
    df['genres'] = df['genres'].str.split(',')
    df = df.explode('genres')

    # Step 4: Remove leading/trailing whitespace from genre names
    df['genres'] = df['genres'].str.strip()

    # Step 5: Get the list of unique genres
    unique_genres = df['genres'].unique()

    # Step 6: Filter data for each genre and write to separate Parquet files
    for genre in unique_genres:
        # Filter the DataFrame for the current genre
        genre_df = df[df['genres'] == genre]
        
        # Define the file path for the current genre
        file_path = f'genres/{genre}.parquet'
        
        # Convert the DataFrame to an Apache Arrow table
        table = pa.Table.from_pandas(genre_df)
        
        # Write the table to a Parquet file
        pq.write_table(table, file_path)
        
        print(f'Written {genre} films to {file_path}')

with dag:
    split_parquet_task = PythonOperator(
        task_id='split_parquet_by_genre',
        python_callable=split_parquet_by_genre,
    )

split_parquet_task
