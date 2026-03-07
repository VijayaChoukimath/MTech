import pandas as pd
import os

input_path = "data/input/spotify-tracks-dataset.csv"
output_dir = "data/output"

# Read CSV
df = pd.read_csv(input_path)

# Drop the first column
df = df.drop(df.columns[0], axis=1)

# Replace nan with None
df = df.where(pd.notnull(df), None)

# Save to Parquet
df.to_parquet(os.path.join(output_dir, "parquet", "data.parquet"))

# Save to ORC
df.to_orc(os.path.join(output_dir, "orc", "data.orc"))

# Save to Avro
df.to_dict('records')
# For Avro, use fastavro
from fastavro import writer, parse_schema
import json

schema = {
    "type": "record",
    "name": "SpotifyTrack",
    "fields": []
}

for col in df.columns:
    if df[col].dtype == 'object':
        schema["fields"].append({"name": col, "type": ["string", "null"]})
    elif df[col].dtype == 'int64':
        schema["fields"].append({"name": col, "type": ["long", "null"]})
    elif df[col].dtype == 'float64':
        schema["fields"].append({"name": col, "type": ["double", "null"]})
    elif df[col].dtype == 'bool':
        schema["fields"].append({"name": col, "type": ["boolean", "null"]})

parsed_schema = parse_schema(schema)

records = df.to_dict('records')

with open(os.path.join(output_dir, "avro", "data.avro"), 'wb') as out:
    writer(out, parsed_schema, records)

print("Data prepared in Parquet, ORC, Avro formats")