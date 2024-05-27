import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# Step 1: Read the existing Parquet file
df = pd.read_parquet('../task1output.parquet')
df.to_csv('output.csv', index=False)
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
