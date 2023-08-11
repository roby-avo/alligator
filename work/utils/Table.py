import pandas as pd
import math

class TableModel:
    
    MIN_ROWS = 25
    SPLIT_THRESHOLD = 50
    CHUNK_SIZE = 25
    TABLE_FOR_PAGE = 10

    def __init__(self, db):
        self._db = db
        self.data = []
        self.table_metadata = {}

    def parse_json(self, json_data):
        # Ensure it's a list of tables
        if not isinstance(json_data, list):
            raise ValueError("The provided JSON data is not a list of tables.")

        # Process data to split larger tables into chunks
        processed_data = []
        for entry in json_data:
            self.fill_table_metadata(entry)
            rows = entry['rows']
            column_metadata = entry.get('metadata', {}).get('column', {})
            column_types = entry.get('semanticAnnotations', {}).get('cta', {})
            entry['column'] = {str(c['idColumn']):c['tag'] for c in column_metadata}
            entry['target'] = {"SUBJ": None, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
            entry['types'] = {str(c['idColumn']):' '.join(sorted(c['types'], reverse=True)) for c in column_types}

            for id_col, column in enumerate(column_metadata):
                if column["tag"] == "SUBJ":
                    entry['target']['SUBJ'] = id_col
                    entry['target']['NE'].append(column["idColumn"]) 
                elif column["tag"] == "NE":
                    entry['target']['NE'].append(column["idColumn"]) 
                elif column["tag"] == "LIT":
                    entry['target']['LIT'].append(column["idColumn"]) 
                    entry['target']['LIT_DATATYPE'][str(column["idColumn"])] = column["datatype"]
            
            entry['status'] = 'TODO'
            entry['state'] = 'READY'
            if "candidateSize" not in entry:
                entry['candidateSize'] = 100

            # Split rows into chunks of CHUNK_SIZE and create new table entries for each chunk
            if len(rows) >= self.SPLIT_THRESHOLD:
                chunks = [rows[i: i + self.CHUNK_SIZE] for i in range(0, len(rows), self.CHUNK_SIZE)]
                
                # If the last chunk is smaller than MIN_ROWS, combine it with the previous chunk
                if len(chunks[-1]) < self.MIN_ROWS:
                    chunks[-2].extend(chunks[-1])
                    chunks.pop()
                
                # Create new table entries for each chunk
                for chunk in chunks:
                    new_entry = entry.copy()
                    new_entry['rows'] = chunk
                    processed_data.append(new_entry)
            else:
                processed_data.append(entry)

        self.data.extend(processed_data)

    def parse_csv(self, file_path, dataset_name, table_name, kg_reference):
        # Read the CSV file using pandas
        df = pd.read_csv(file_path)
        # Extract headers
        headers = df.columns.tolist()
        
        # Build the initial table object
        table_obj = {
            "datasetName": dataset_name,
            "tableName": table_name,
            "header": headers,
            "rows": [],
            "kgReference": kg_reference,
            "status": "TODO",
            "state": "READY",
            "candidateSize": 100
        }

        self.fill_table_metadata(table_obj)

        # Split DataFrame rows into chunks of CHUNK_SIZE and create new table entries for each chunk
        num_rows = len(df)
        if num_rows >= self.SPLIT_THRESHOLD:
            chunks = [df.iloc[i: i + self.CHUNK_SIZE] for i in range(0, num_rows, self.CHUNK_SIZE)]
            
            # If the last chunk is smaller than MIN_ROWS, combine it with the previous chunk
            if len(chunks[-1]) < self.MIN_ROWS:
                chunks[-2] = pd.concat([chunks[-2], chunks[-1]])
                chunks.pop()
            
            for chunk_df in chunks:
                new_entry = table_obj.copy()
                new_entry['rows'] = [{"idRow": idx + 1, "data": row_data} for idx, row_data in enumerate(chunk_df.values.tolist())]
                self.data.append(new_entry)
        else:
            table_obj['rows'] = [{"idRow": idx + 1, "data": row_data} for idx, row_data in enumerate(df.values.tolist())]
            self.data.append(table_obj)

    def fill_table_metadata(self, entry):
        dataset_name = entry['datasetName']
        table_name = entry['tableName']
        if dataset_name not in self.table_metadata:
            self.table_metadata[dataset_name] = {}
        if table_name not in self.table_metadata[dataset_name]:
            self.table_metadata[dataset_name][table_name] = {
                "datasetName": dataset_name,
                "tableName": table_name,
                "Nrows": 0, 
                "status": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "statusCopy": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "state": "TODO"
            }  
        self.table_metadata[dataset_name][table_name]["status"]["TODO"] += 1 
        self.table_metadata[dataset_name][table_name]["Nrows"] += len(entry["rows"])

    def get_data(self):
        return self.data

    def store_tables(self):
        for dataset_name in self.table_metadata:
            for table_name in self.table_metadata[dataset_name]:
                metadata = self.table_metadata[dataset_name][table_name]
                total_tables = self._db.get_collection("table").count_documents({"datasetName": dataset_name})
                page = math.floor(total_tables / self.TABLE_FOR_PAGE) + 1
                metadata["page"] = page
                self._db.get_collection("table").insert_one(metadata)