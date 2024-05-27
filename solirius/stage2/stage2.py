import pandas as pd

# Load your dataset into a DataFrame
films_df = pd.read_parquet("../task1output.parquet")

# Define the function to find similar films
def find_similar_films(film_id, threshold=50):
    try:
        # Retrieve genres for the film based on film_id
        film_genres = films_df.loc[films_df['id'] == film_id, 'genres'].iloc[0]
        
        # Calculate similarity score based on genres
        films_df['similarity_score'] = films_df['genres'].apply(lambda x: len(set(x.split(', ')) & set(film_genres.split(', '))) / len(set(x.split(', ')) | set(film_genres.split(', '))) * 100)
        
        # Filter films based on similarity threshold
        similar_films_above_threshold = films_df[films_df['similarity_score'] > threshold]
        
        return similar_films_above_threshold
    except KeyError as e:
        print(f"KeyError occurred: {e}")
        return pd.DataFrame()  # Return empty DataFrame if error occurs

# Example usage
film_id = 123  # Assuming film ID you want to find similar films for
threshold = 50  # Similarity threshold percentage

similar_films = find_similar_films(film_id, threshold)

# Save the similar films DataFrame to a CSV file
similar_films.to_csv("similar_films.csv", index=False)

print("Similar films saved to 'similar_films.csv'")
