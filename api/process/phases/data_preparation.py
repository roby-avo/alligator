

class DataPreparation:
    def __init__(self, rows, lamAPI):
        self._rows = rows
        self._lamAPI = lamAPI

  
    async def compute_datatype(self):
        column_metadata = {}
        target = {"SUBJ": None, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
        columns_data = [[] for _ in range(0, len(self._rows[0]['data']))]
        for row in self._rows:
            for id_col, cell in enumerate(row["data"]):
                columns_data[id_col].append(str(cell))
        
        metadata = await self._lamAPI.column_analysis(columns_data)
        first_NE_column = False  
        for id_col in metadata:
            tag = metadata[id_col]["tag"]
            column_metadata[id_col] = tag
            target[tag].append(int(id_col)) 
            if tag == "NE":
                if not first_NE_column:
                    target["SUBJ"] = int(id_col)
                first_NE_column = True
            else:
                target['LIT_DATATYPE'][str(id_col)] = metadata[id_col]["datatype"]
                
        return column_metadata, target        


    def rows_normalization(self):
        for row in self._rows:
            for id_col, _ in enumerate(row["data"]):
                row["data"][id_col] = self._clean_str(row["data"][id_col])


    def _clean_str(self, value):
        value = str(value)
        stop_charaters = ["_"]
        for char in stop_charaters:
            value = value.replace(char, " ")
        value = " ".join(value.split()).lower()
        return value
