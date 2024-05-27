import pandas as pd

# Load your dataset into a DataFrame
films_df = pd.read_csv("../stage1/output.csv")

# Define the function to query the dataset
def query_films(**kwargs):
    try:
        query = films_df.copy()  # Create a copy of the original DataFrame for filtering
        
        # Apply filters based on user input
        for column, value in kwargs.items():
            if isinstance(value, list):  # Handle list of values (e.g., for genres)
                query = query[query[column].isin(value)]
            elif isinstance(value, tuple):  # Handle range of values (e.g., for release years)
                query = query[(query[column] >= value[0]) & (query[column] <= value[1])]
            else:  # Handle single value
                query = query[query[column] == value]
        
        return query
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()  # Return empty DataFrame if error occurs

# Define a function to save DataFrame to CSV file
def save_to_csv(dataframe, filename):
    try:
        dataframe.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error occurred while saving to CSV: {e}")

# Define a function to print DataFrame to console
def print_to_console(dataframe, message):
    print(message)
    print(dataframe)

# Example test cases
# Query films directed by Quentin Tarantino released between 1979 and 2000
query1 = query_films(director="Quentin Tarantino", release_year=(1979, 2000))
print_to_console(query1, "Query 1 Results (Quentin Tarantino films released between 1979 and 2000):")
save_to_csv(query1, "query1_results.csv")

# Query films directed by George Lucas released after 1990
query2 = query_films(director="George Lucas", release_year=(1990, float('inf')))
print_to_console(query2, "Query 2 Results (George Lucas films released after 1990):")
save_to_csv(query2, "query2_results.csv")

# Query films with specific genres
query3 = query_films(genres=["Action", "Adventure"])
print_to_console(query3, "Query 3 Results (Films with genres Action or Adventure):")
save_to_csv(query3, "query3_results.csv")
