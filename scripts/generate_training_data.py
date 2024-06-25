import sys
import os
import pandas as pd
import argparse
import argcomplete
import utils as utils
from tqdm import tqdm

# Dynamically add the project's root directory to PYTHONPATH
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from api.process.wrapper.Database import MongoDBWrapper  # MongoDB database wrapper

# Function to fetch dataset names from MongoDB
def fetch_db_datasets():
    mongoDBWrapper = MongoDBWrapper()
    client = mongoDBWrapper.get_client()
    dataset_c = mongoDBWrapper.get_collection("dataset")
    db_datasets = dataset_c.distinct("datasetName")
    return db_datasets

# Function to parse CEA ground truth
def parse_cea(cea_path):
    cea_gt = {}
    if os.path.exists(cea_path):
        total_lines = sum(1 for _ in open(cea_path))
        for chunk in tqdm(pd.read_csv(cea_path, header=None, chunksize=10000, dtype={0: str, 1: int, 2: int, 3: str}), total=total_lines//10000 + 1, desc="Parsing CEA"):
            for index, row in chunk.iterrows():
                key = f"{row[0]}-{row[1]}-{row[2]}"
                qids = row[3].split() # Clean QIDs
                qids = [qid.split('/')[-1] for qid in qids] # Extract QIDs
                if key not in cea_gt:
                    cea_gt[key] = []
                cea_gt[key].append(qids)
    return cea_gt

# Function to generate samples from candidates
def get_samples(candidates, cea_gt, table_name, group, key):
    samples = []
    for candidate in candidates:
        features = {feature: round(candidate["features"][feature], 3) for feature in candidate["features"]}
        if candidate["id"] in cea_gt:
            sample = dict(**{"tableName": table_name, "key": key}, **features, **{"group": group, "target": 1})
            samples.append(sample)
    
    for candidate in candidates:
        features = {feature: round(candidate["features"][feature], 3) for feature in candidate["features"]}
        if candidate["id"] not in cea_gt:
            sample = dict(**{"tableName": table_name, "key": key}, **features, **{"group": group, "target": 0})
            samples.append(sample)
        if len(samples) == 10:
            break
    
    return samples

# Function to generate training datasets
def generate_training_dataset(datasets, base_dir, export_path="data/training/", buffer_size=1000):
    if not os.path.exists(export_path):
        os.makedirs(export_path)
        
    mongoDBWrapper = MongoDBWrapper()
    client = mongoDBWrapper.get_client()

    with client.start_session() as session:
        cea_c = mongoDBWrapper.get_collection("candidateScored")
        for id_dataset in datasets:
            tables_path = os.path.join(base_dir, datasets[id_dataset]['tables'])
            cea_target_path = os.path.join(base_dir, datasets[id_dataset]['cea'])
            cea_gt = parse_cea(cea_target_path)
            path = os.path.join(export_path, f"{id_dataset}.csv")
            buffer = []
            group = 0

            db_datasets = fetch_db_datasets()
            if id_dataset not in db_datasets:
                print(f"Dataset name '{id_dataset}' not found in the database.")
                continue

            results = cea_c.find({"datasetName": id_dataset}, no_cursor_timeout=True, session=session)
            total = cea_c.count_documents({"datasetName": id_dataset})

            for result in tqdm(results, total=total, desc=f"Processing {id_dataset}"):
                id_table = result["tableName"]
                id_row = result["row"]
                for id_col, item in enumerate(result["candidates"]):
                    key = f"{id_table}-{id_row}-{id_col}"
                    if key not in cea_gt or cea_gt.get(key) is None:
                        continue    
                    temp = item
                    if len(temp) > 0:
                        samples = get_samples(temp, cea_gt.get(key, []), id_table, group, key)
                        buffer += samples
                        group += 1    

                    if len(buffer) >= buffer_size:
                        pd.DataFrame(buffer).to_csv(path, mode='a', index=False, header=not os.path.exists(path))
                        buffer = []

            if len(buffer) > 0:
                pd.DataFrame(buffer).to_csv(path, mode='a', index=False, header=not os.path.exists(path))
                buffer = []

            results.close()

def main():
    parser = argparse.ArgumentParser(description='Generate training datasets from specified datasets.')
    parser.add_argument('datasets', nargs='*', help='List of dataset names')
    parser.add_argument('--base_dir', type=str, help='Base directory to recover the datasets data')
    parser.add_argument('--export_path', type=str, default="data/training/", help='Path to export the training datasets')
    parser.add_argument('--buffer_size', type=int, default=1000, help='Buffer size for writing to CSV')
    parser.add_argument('--list_datasets', action='store_true', help='List available datasets in the database')
    
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.list_datasets:
        print("Available datasets in the database:")
        for dataset in fetch_db_datasets():
            print(f"- {dataset}")
        sys.exit(0)

    if not args.datasets or not args.base_dir:
        parser.print_help()
        sys.exit(1)

    datasets = {}
    for dataset_name in args.datasets:
        datasets[dataset_name] = {
            "tables": f"{dataset_name}/tables",
            "cea": f"{dataset_name}/gt/cea.csv"
        }

    generate_training_dataset(datasets, base_dir=args.base_dir, export_path=args.export_path, buffer_size=args.buffer_size)

if __name__ == "__main__":
    main()