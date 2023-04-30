import pandas as pd

# Load the CSV file
df = pd.read_csv('output.csv')

# Get the unique count of values for the specified column
unique_count = df['item_id'].nunique()

# Print the result
print(f'The unique count of values for item_id is {unique_count}.')
