import pandas as pd
from tqdm import tqdm

train_dataset = [
    "ALL-2T_2020", 
    "ALL-HTR2", 
    "ALL-HTR3", 
    "ALL-Round1_T2D", 
    "ALL-Round3", 
    "ALL-Round4"
]

for train_dataset in tqdm(train_dataset):
    path = f"../data/ml_with_type/10/{train_dataset}.csv"
    train = pd.read_csv(path)
    for label in ["cpa", "cpaMax", "cta", "ctaMax", "cea", "diff"]:
        train[label] = 0
    
    train.drop_duplicates(subset=list(set(train.columns)-set(["tableName"])), inplace=True)
    train.to_csv(f"data/ml_with_type/10/{train_dataset}_P.csv", index=False)
