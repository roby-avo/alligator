K = 0.6
THRESHOLD = 0.03
SIGMA = 0.3

class Decision:
    def __init__(self, metadata: dict, cea_prelinking_data, rows, cta, cpa, collections: dict):
        self._rows = rows
        self._cta = cta
        self._cpa = cpa
        self._dataset_name = metadata["datasetName"]
        self._table_name = metadata["tableName"]
        self._kg_reference = metadata["kgReference"]
        self._page = metadata["page"]
        self._cea_prelinking_data = cea_prelinking_data
        self._cea_prelinking_collection = collections["ceaPrelinking"] 
        self._cea_collection = collections["cea"] 
        self._cta_collection = collections["cta"] 
        self._cpa_collection = collections["cpa"] 
        self._cadidate_scored_collection = collections["candidateScored"] 


    def store_data(self):
        self.store_cea_prelinking()
        self.store_cea_and_candidates_scored_data()
        self.store_cta_data()
        self.store_cpa_data()


    def store_cea_prelinking(self):
        self._cea_prelinking_collection.insert_many(self._cea_prelinking_data)


    def store_cea_and_candidates_scored_data(self):
        cea_data = []
        candidates_scored_data = []
        for row in self._rows:
            winning_candidates =  []
            cea = {}
            rankend_candidates = []
            scores = []
            types = []
            column_type_set = {}
            for i, cell in enumerate(row.get_cells()):
                candidates = cell.candidates()
                wc = []
                rank = candidates[0:20] if len(candidates) > 0 else []
                score = None
                if len(candidates) > 0:
                    if len(candidates) > 1:
                        candidates[0]["delta"] = round(candidates[0]["rho'"] - candidates[1]["rho'"], 3)
                    else:
                        candidates[0]["delta"] = 1   
                    candidates[0]["score"] = round((1-K) * candidates[0]["rho'"] + K * candidates[0]["delta"], 3)
                    wc.append(candidates[0])
                    score = candidates[0]["rho'"]
                
                if len(wc) == 1:
                    cea[str(cell._id_col)] = wc[0]["id"]
                    if wc[0]["score"] > SIGMA:
                        wc[0]["match"] = True
                        wc.extend(candidates[1:3])
                    else:    
                        wc.extend(candidates[1:5])
                
                column_type_set[i] = set()
                for candidate in wc:
                    for t in candidate.get("types", []):
                        type_id = t["id"]
                        if type_id not in column_type_set[i]:
                            types.append({"column": i, "type": type_id})
                            column_type_set[i].add(type_id)

                winning_candidates.append(wc)
                rankend_candidates.append(rank)
                scores.append({"column": i, "score": score})

            cea_data.append({
                "datasetName": self._dataset_name,
                "tableName": self._table_name,
                "row": row._id_row,
                "data": row.get_row_text(),
                "winningCandidates": winning_candidates,
                "cea": cea,
                "kgReference": self._kg_reference,
                "page": self._page,
                "scores": scores,
                "types": types
            })

            candidates_scored_data.append({
                "datasetName": self._dataset_name,
                "tableName": self._table_name,
                "row": row._id_row,
                "data": row.get_row_text(),
                "candidates": rankend_candidates,
                "kgReference": self._kg_reference,
                "page": self._page
            })

        self._cea_collection.insert_many(cea_data)
        self._cadidate_scored_collection.insert_many(candidates_scored_data)


    def store_cta_data(self):
        cta = {}
        for id_col in self._cta:
            id_type = max(self._cta[id_col], key=self._cta[id_col].get, default=None)
            if id_type is not None:
                cta[id_col] = id_type
        
        cta_data = {
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "winningCandidates": self._cta,
            "cta": cta,
            "kgReference": self._kg_reference,
            "page": self._page
        }

        self._cta_collection.insert_one(cta_data)


    def store_cpa_data(self):
        cpa = {}
        for id_col in self._cpa:
            cpa[id_col] = {}
            for id_col_rel in self._cpa[id_col]:
                id_predicate = max(self._cpa[id_col][id_col_rel], key=self._cpa[id_col][id_col_rel].get, default=None)
                if id_predicate is not None:
                    cpa[id_col][id_col_rel] = id_predicate
                


        cpa_data = {
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "subjectCol": self._rows[0].get_subject_cell()._id_col if self._rows[0].get_subject_cell() is not None else 0,
            "winningCandidates": self._cpa,
            "cpa": cpa,
            "kgReference": self._kg_reference,
            "page": self._page
        }

        self._cpa_collection.insert_one(cpa_data)

        