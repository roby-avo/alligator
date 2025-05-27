import pandas as pd
import math
import time
import sys
import os
import asyncio
import random

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TableModel:
    
    config_values = os.environ["CONFIG_VALUES"].split(",")  # CONFIG_VALUES
    DATASET_FOR_PAGE = int(config_values[0])
    TABLE_FOR_PAGE = int(config_values[1])
    CHUNCK_SIZE = int(config_values[2])
    SPLIT_THRESHOLD = 2 * CHUNCK_SIZE  # Set SPLIT_THRESHOLD based on CHUNCK_SIZE
    SAMPLE_SIZE_FOR_ANALYSIS = 50  # Maximum number of rows to sample for global column analysis


    def __init__(self, db, lamAPI):
        self._db = db
        self._lamAPI = lamAPI
        self.data = []
        self.table_metadata = {}
        self.global_column_metadata = {}  # Store global column metadata
    
    async def analyze_columns_globally(self, entry):
        """
        Analyze columns globally before splitting into batches.
        Samples rows from the table to ensure efficiency.
        
        Args:
            entry: Table entry containing rows data
        """
        if not entry.get('rows'):
            return None
        
        # Sample rows for analysis (up to SAMPLE_SIZE_FOR_ANALYSIS)
        rows_to_sample = entry['rows']
        if len(rows_to_sample) > self.SAMPLE_SIZE_FOR_ANALYSIS:
            rows_to_sample = random.sample(rows_to_sample, self.SAMPLE_SIZE_FOR_ANALYSIS)
        
        # Extract column data for analysis
        columns_data = [[] for _ in range(len(rows_to_sample[0]['data']))]
        for row in rows_to_sample:
            for id_col, cell in enumerate(row['data']):
                columns_data[id_col].append(str(cell))
        
        # Perform column analysis using LamAPI
        try:
            table_key = f"{entry['datasetName']}:{entry['tableName']}"
            metadata = await self._lamAPI.column_analysis(columns_data)
            self.global_column_metadata[table_key] = metadata
            return metadata
        except Exception as e:
            print(f"Error performing global column analysis: {e}")
            return None

    def parse_json(self, json_data):
        # Ensure it's a list of tables
        if not isinstance(json_data, list):
            raise ValueError("The provided JSON data is not a list of tables.")

        # Perform global column analysis for each table before processing
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for entry in json_data:
            loop.run_until_complete(self.analyze_columns_globally(entry))
        loop.close()

        # Process data to split larger tables into chunks
        processed_data = []
        for entry in json_data:
            self.fill_table_metadata(entry)
            rows = entry['rows']
            column_types = entry.get('semanticAnnotations', {}).get('cta', {})
            entry['types'] = {str(c['idColumn']):' '.join(sorted(c['types'], reverse=True)) for c in column_types}
            
            # Set default values for page, status and state
            entry['page'] = 1   
            entry['status'] = 'TODO'
            entry['state'] = 'READY'
            if "candidateSize" not in entry:
                entry['candidateSize'] = 20
                
            # Add global column metadata to each entry
            table_key = f"{entry['datasetName']}:{entry['tableName']}"
            if table_key in self.global_column_metadata:
                entry['globalColumnMetadata'] = self.global_column_metadata[table_key]

            # Split rows into chunks of CHUNCK_SIZE and create new table entries for each chunk
            if len(rows) >= TableModel.CHUNCK_SIZE * 2:
                chunks = [rows[i: i + TableModel.CHUNCK_SIZE] for i in range(0, len(rows), TableModel.CHUNCK_SIZE)]
                
                # If the last chunk is smaller than MIN_ROWS, combine it with the previous chunk
                if len(chunks[-1]) < TableModel.CHUNCK_SIZE:
                    chunks[-2].extend(chunks[-1])
                    chunks.pop()
                
                # Create new table entries for each chunk
                for page, chunk in enumerate(chunks):
                    new_entry = entry.copy()
                    new_entry['rows'] = chunk
                    new_entry['page'] = page + 1
                    processed_data.append(new_entry)
            else:
                processed_data.append(entry)
              
        self.data.extend(processed_data)

    async def _analyze_csv_columns(self, df, dataset_name, table_name):
        """
        Analyze columns for a CSV file
        """
        if len(df) == 0:
            return None
            
        # Sample rows for analysis
        sample_size = min(len(df), self.SAMPLE_SIZE_FOR_ANALYSIS)
        df_sample = df.sample(n=sample_size) if len(df) > sample_size else df
        
        # Extract column data for analysis
        columns_data = [[] for _ in range(len(df.columns))]
        for _, row in df_sample.iterrows():
            for id_col, cell in enumerate(row):
                columns_data[id_col].append(str(cell))
        
        # Perform column analysis
        try:
            table_key = f"{dataset_name}:{table_name}"
            metadata = await self._lamAPI.column_analysis(columns_data)
            self.global_column_metadata[table_key] = metadata
            return metadata
        except Exception as e:
            print(f"Error performing global column analysis: {e}")
            return None

    def parse_csv(self, file_path, dataset_name, table_name, kg_reference):
        # Read the CSV file using pandas
        df = pd.read_csv(file_path)
        # Extract headers
        headers = df.columns.tolist()
        
        # Perform global column analysis before splitting
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._analyze_csv_columns(df, dataset_name, table_name))
        loop.close()
        
        # Build the initial table object
        table_obj = {
            "datasetName": dataset_name,
            "tableName": table_name,
            "header": headers,
            "rows": [],
            "kgReference": kg_reference,
            "column": {},
            "target": {},
            "status": "TODO",
            "state": "READY",
            "candidateSize": 1000,
            "page": 1
        }
        
        # Add global column metadata if available
        table_key = f"{dataset_name}:{table_name}"
        if table_key in self.global_column_metadata:
            table_obj['globalColumnMetadata'] = self.global_column_metadata[table_key]
            
        table_obj['rows'] = [{"idRow": idx + 1, "data": row_data} for idx, row_data in enumerate(df.values.tolist())]
        self.fill_table_metadata(table_obj)    

        # Split DataFrame rows into chunks of CHUNK_SIZE and create new table entries for each chunk
        num_rows = len(df)
        if num_rows >= TableModel.SPLIT_THRESHOLD:
            offset = 1
            chunks = [df.iloc[i: i + TableModel.CHUNCK_SIZE] for i in range(0, num_rows, TableModel.CHUNCK_SIZE)]            
            # If the last chunk is smaller than MIN_ROWS, combine it with the previous chunk
            if len(chunks[-1]) < TableModel.CHUNCK_SIZE:
                chunks[-2] = pd.concat([chunks[-2], chunks[-1]])
                chunks.pop()
            
            for page, chunk_df in enumerate(chunks):
                new_entry = table_obj.copy()
                new_entry['rows'] = [{"idRow": idx + offset, "data": row_data} for idx, row_data in enumerate(chunk_df.values.tolist())]
                new_entry['page'] = page + 1
                self.data.append(new_entry)
                offset = new_entry['rows'][-1]["idRow"] + 1
        else:
            table_obj['rows'] = [{"idRow": idx + 1, "data": row_data} for idx, row_data in enumerate(df.values.tolist())]
            self.data.append(table_obj)
         
        return num_rows
    
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
                "taskStatus": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "status": "TODO"
            }  
        self.table_metadata[dataset_name][table_name]["taskStatus"]["TODO"] += 1 
        self.table_metadata[dataset_name][table_name]["Nrows"] += len(entry["rows"])

    def get_data(self):
        return self.data
    
    def update_data_with_id_job(self,  dataset_name, id_job):
        for table in self.data:
            if table['datasetName'] == dataset_name:
                table['idJob'] = id_job

    def store_tables(self, Nrows=None):
        for dataset_name in self.table_metadata:
            result = self._db.get_collection("job").insert_one({
                "name": dataset_name,
                "status": {
                    "TODO": len(self.table_metadata[dataset_name]), 
                    "DOING": 0, 
                    "DONE": 0
                },
                "startTime": time.time(),
                "startTimeComputation": None,
                "elapsedTime": 0,
                "elapsedTimeComputation": 0,
                "%": 0,
                "estimatedTime": None,
                "active": True
            })
            inserted_id = result.inserted_id
            self.update_data_with_id_job(dataset_name, inserted_id)

            for table_name in self.table_metadata[dataset_name]:
                metadata = self.table_metadata[dataset_name][table_name]
                total_tables = self._db.get_collection("table").count_documents({"datasetName": dataset_name})
                page = math.floor(total_tables / TableModel.TABLE_FOR_PAGE) + 1
                metadata["page"] = page
                if Nrows is not None:
                    metadata["Nrows"] = Nrows
                metadata["idJob"] = inserted_id    
                try:
                    self._db.get_collection("table").insert_one(metadata)
                except:
                    self._db.get_collection("job").delete_one({"_id": inserted_id})
                    raise
