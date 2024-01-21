from tqdm import tqdm
from process.wrapper.mongodb_conn import get_collection

#put id dataset as parameter

dataset = {
    "Round1_T2D": {
        "tables": "./data/Dataset/Round1_T2D/tables",
        "cea":"./data/Dataset/Round1_T2D/gt/CEA_Round1_gt.csv",
        "cpa":"./data/Dataset/Round1_T2D/gt/CPA_Round1_gt.csv",
        "cta": "./data/Dataset/Round1_T2D/gt/CTA_Round1_gt.csv"
    },
    "Round3": {
        "tables": "./data/Dataset/Round3_2019/tables/",
        "cea":"./data/Dataset/Round3_2019/gt/CEA_Round3_gt.csv",
        "cpa":"./data/Dataset/Round3_2019/gt/CPA_Round3_gt.csv",
        "cta": "./data/Dataset/Round3_2019/gt/CTA_Round3_gt.csv"
    },
    "Round4": {
        "tables": "./data/Dataset/Round4_2020/tables/",
        "cea":"./data/Dataset/Round4_2020/gt/cea.csv",
        "cpa":"./data/Dataset/Round4_2020/gt/cpa.csv",
        "cta": "./data/Dataset/Round4_2020/gt/cta.csv"
    },
    "2T-2020": {
        "tables":"./data/Dataset/2T_Round4/tables/", 
        "cea":"./data/Dataset/2T_Round4/gt/cea.csv",
        "cpa": None,
        "cta": "./data/Dataset/2T_Round4/gt/cta.csv"
    },
    "HardTableR2-2021": {
        "tables":"./data/Dataset/HardTablesR2/tables/", 
        "cea":"./data/Dataset/HardTablesR2/gt/cea.csv",
        "cpa":"./data/Dataset/HardTablesR2/gt/cpa.csv",
        "cta": "./data/Dataset/HardTablesR2/gt/cta.csv"
    },
    "HardTableR3-2021": {
        "tables":"./data/Dataset/HardTablesR3/tables/", 
        "cea":"./data/Dataset/HardTablesR3/gt/cea.csv",
        "cpa":"./data/Dataset/HardTablesR3/gt/cpa.csv",
        "cta": "./data/Dataset/HardTablesR3/gt/cta.csv"
    }
}


for id_dataset in tqdm(dataset):
    id_dataset = f"{id_dataset}"
    col = ['cea', 'cta', 'cpa', 'ceaInit', 'ceaPrelinking', 'candidateScored', 'headerCandidateScored', 'ceaHeader']
    for c in col:
        c = get_collection(c)
        c.delete_many({"datasetName": id_dataset})
    c = get_collection('row')
    c.delete_many({"datasetName": id_dataset})
    c = get_collection('dataset')
    c.delete_many({"datasetName": id_dataset})
    c = get_collection('table')
    c.delete_many({"datasetName": id_dataset})
    c = get_collection('job')
    c.delete_many({"name": id_dataset})