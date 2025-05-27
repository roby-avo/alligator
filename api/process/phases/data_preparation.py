import utils.utils as utils
import asyncio

class DataPreparation:
    def __init__(self, header, rows, lamAPI):
        self._rows = rows
        self._column_to_datatype = self._parse_header(header)
        self._lamAPI = lamAPI

    def _parse_header(self, header):
        parsed_header = {}
        for id_col, column_label in enumerate(header):
            if 'NE' in column_label:
                kind = 'NE'
                datatype = None  # No datatype for NE columns
            elif 'LIT' in column_label:
                kind = 'LIT'
                # Extract datatype from column label
                datatype = column_label.split('(')[1].split(')')[0]
            else:
                kind = None
                datatype = None
            if kind is not None:    
                parsed_header[str(id_col)] = {'kind': kind, 'datatype': datatype}
        return parsed_header
            
    async def compute_datatype(self, current_column_metadata, current_target):
        column_metadata = {}
        #print("column_metadata", self._column_to_datatype, flush=True)
        #print("rows", self._rows, flush=True)
        target = {"SUBJ": None, "NE": [], "LIT": [], "NO_TAG": [], "LIT_DATATYPE": {}}
        columns_data = [[] for _ in range(0, len(self._rows[0]['data']))]
        for row in self._rows:
            for id_col, cell in enumerate(row["data"]):
                columns_data[id_col].append(str(cell))
        
        #print("columns_data", columns_data, flush=True)
        # Run the async function and wait for it to complete
        metadata = await self._lamAPI.column_analysis(columns_data)
        metadata = {
            "0": {
                "index_column": 0,
                "tag": "NE",
                "classification": "PERSON",
                "datatype": "PERSON",
                "probabilities": {
                "PERSON": 1.0
                }
            },
            "1": {
                "index_column": 1,
                "tag": "LIT",
                "classification": "NUMBER",
                "datatype": "NUMBER",
                "probabilities": {
                "NUMBER": 1.0,
                "DATE": 1.0
                }
            },
            "2": {
                "index_column": 2,
                "tag": "LIT",
                "classification": "NUMBER",
                "datatype": "NUMBER",
                "probabilities": {
                "NUMBER": 1.0
                }
            },
            "3": {
                "index_column": 3,
                "tag": "NE",
                "classification": "ORGANIZATION",
                "datatype": "ORGANIZATION",
                "probabilities": {
                "ORGANIZATION": 1.0
                }
            },
            "4": {
                "index_column": 4,
                "tag": "LIT",
                "classification": "NUMBER",
                "datatype": "NUMBER",
                "probabilities": {
                "NUMBER": 1.0
                }
            },
            "5": {
                "index_column": 5,
                "tag": "LIT",
                "classification": "STRING",
                "datatype": "STRING",
                "probabilities": {
                "PERSON": 1.0
                }
            },
            "6": {
                "index_column": 6,
                "tag": "LIT",
                "classification": "NUMBER",
                "datatype": "NUMBER",
                "probabilities": {
                "NUMBER": 1.0
                }
            },
            "7": {
                "index_column": 7,
                "tag": "NE",
                "classification": "PERSON",
                "datatype": "PERSON",
                "probabilities": {
                "PERSON": 1.0
                }
            },
            "8": {
                "index_column": 8,
                "tag": "NE",
                "classification": "PERSON",
                "datatype": "PERSON",
                "probabilities": {
                "PERSON": 1.0
                }
            },
            "9": {
                "index_column": 9,
                "tag": "LIT",
                "classification": "NUMBER",
                "datatype": "NUMBER",
                "probabilities": {
                "NUMBER": 1.0,
                "DATE": 1.0
                }
            },
            "10": {
                "index_column": 10,
                "tag": "LIT",
                "classification": "STRING",
                "datatype": "STRING",
                "probabilities": {
                "STRING": 1.0
                }
            }
        }
  

        #print("metadata", metadata, flush=True)
        first_NE_column = False  
        for id_col in metadata:
            lit_datatype = None
            if id_col in current_column_metadata:
                tag = current_column_metadata[id_col]
                if tag == "LIT":
                    lit_datatype = current_target["LIT_DATATYPE"][id_col]
            elif id_col not in self._column_to_datatype:
                tag = metadata[id_col]["tag"]
                if tag == "LIT":
                    lit_datatype = metadata[id_col]["datatype"]
            else:
                tag = self._column_to_datatype[id_col]['kind']
                lit_datatype = self._column_to_datatype[id_col]['datatype']
            
            if tag == "SUBJ": # you just need it for degugging if you re-run stuff!
                tag = "NE"

            column_metadata[id_col] = tag    
            target[tag].append(int(id_col)) 
            if tag == "NE":
                if not first_NE_column:
                    target["SUBJ"] = int(id_col)
                first_NE_column = True
            elif tag == "LIT":
                target['LIT_DATATYPE'][str(id_col)] = lit_datatype
        
        return column_metadata, target        

    def rows_normalization(self):
        for row in self._rows:
            for id_col, _ in enumerate(row["data"]):
                row["data"][id_col] = utils.clean_str(row["data"][id_col])
