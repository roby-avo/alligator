class FeaturesExtractionRevision:
    def __init__(self, rows):
        self._rows = rows
        self._cta = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        self._cpa = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        self._cpa_pair = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        self._compute_cta_and_cpa_freq()
        
        
    def compute_features(self):
        features = [[] for _ in range(len(self._rows[0]))]
        for row in self._rows:
            for cell in row.get_cells():
                id_col = str(cell._id_col)
                for candidate in cell.candidates():
                    (cta, ctaMax) = (0, 0)
                    total_types = 0
                    for t in candidate["types"]:
                        if t["id"] in self._cta[id_col]:
                            cta += self._cta[id_col][t["id"]]
                            total_types += 1
                            if self._cta[id_col][t["id"]] > ctaMax:
                                ctaMax = self._cta[id_col][t["id"]]
                    
                    (cpa, cpaMax) = (0, 0)
                    predicates = {}
                    for id_col_pred in candidate["predicates"]:
                        for id_predicate in candidate["predicates"][id_col_pred]:
                            if id_predicate not in predicates:
                                predicates[id_predicate] = 0
                            if candidate["predicates"][id_col_pred][id_predicate] > predicates[id_predicate]:
                                predicates[id_predicate] = candidate["predicates"][id_col_pred][id_predicate]
                                
                    for id_predicate in predicates:
                        if id_predicate in self._cpa[id_col]:
                            cpa += self._cpa[id_col][id_predicate] * predicates[id_predicate]
                            if self._cpa[id_col][id_predicate] * predicates[id_predicate] > cpaMax:
                                cpaMax = self._cpa[id_col][id_predicate] * predicates[id_predicate]
                                
                    cta /= len(candidate["types"]) if total_types > 0 else 1
                    candidate["features"]["cta"] = round(cta, 2)
                    candidate["features"]["ctaMax"] = round(ctaMax, 2)
                    
                    cpa /= len(predicates) if len(predicates) > 0 else 1
                    candidate["features"]["cpa"] = round(cpa, 2)
                    candidate["features"]["cpaMax"] = round(cpaMax, 2)
                    
                    candidate["features"]["diff"] = round(cell.candidates()[0]["features"]["rho"] - candidate["features"]["rho"], 3)
                    
                    features[int(id_col)].append(list(candidate["features"].values()))

        return features          
                    
            
    def _compute_cta_and_cpa_freq(self):
        for row in self._rows:
            for cell in row.get_cells():
                id_col = str(cell._id_col)
                history = set()
                for candidate in cell.candidates()[0:1]:
                    types = candidate["types"]
                    for t in types:
                        id_type = t["id"]
                        if id_type in history:
                            continue
                        if id_type not in self._cta[id_col]:
                            self._cta[id_col][id_type] = 0
                        self._cta[id_col][id_type] += 1
                        history.add(id_type)
                    
                    predicates = candidate["predicates"]
                    for id_col_rel in predicates:
                        if id_col_rel not in self._cpa_pair[id_col]:
                            self._cpa_pair[id_col][id_col_rel] = {}
                        for id_predicate in predicates[id_col_rel]:
                            if id_predicate in history:
                                continue
                            if id_predicate not in self._cpa[id_col]:
                                self._cpa[id_col][id_predicate] = 0
                            if id_predicate not in self._cpa_pair[id_col][id_col_rel]:
                                self._cpa_pair[id_col][id_col_rel][id_predicate] = 0    
                            self._cpa[id_col][id_predicate] += 1 * predicates[id_col_rel][id_predicate]  
                            self._cpa_pair[id_col][id_col_rel][id_predicate] += 1 * predicates[id_col_rel][id_predicate]
                            history.add(id_predicate)
        
        n_rows = len(self._rows)
        for id_col in self._cta:
            for id_type in self._cta[id_col]:
                self._cta[id_col][id_type] = round(self._cta[id_col][id_type]/n_rows, 2)
            for id_predicate in self._cpa[id_col]:
                self._cpa[id_col][id_predicate] = round(self._cpa[id_col][id_predicate]/n_rows, 2)   
            for id_col_rel in self._cpa_pair[id_col]:
                for id_predicate in self._cpa_pair[id_col][id_col_rel]:
                    self._cpa_pair[id_col][id_col_rel][id_predicate] = round(self._cpa_pair[id_col][id_col_rel][id_predicate]/n_rows, 2)