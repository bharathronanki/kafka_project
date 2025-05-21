import pandas as pd 
import os

df = pd.read_csv("/Users/bharathronanki/Desktop/Projects/kafka/kafka_producer/movies.csv")

output_dir = "/Users/bharathronanki/Desktop/Projects/kafka/kafka_producer/data"

# print(df.head)
chunk_size = 1000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i: i+chunk_size]
    output_path =  os.path.join(output_dir,f"chunk{i+1}.csv")
    chunk.to_csv(output_path, index=False, header=True)
    print(f"Saved to {output_path}")

