
class FeaturesExtractionRevision:
    def __init__(self, rows, entity_to_predicates_obj, entity_to_predicates_lit):
        self._rows = rows
        self._cta = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        self._cpa = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        self._cpa_pair = {str(id_col):{} for id_col in range(len(self._rows[0]))}
        for entity in entity_to_predicates_lit:
            if entity not in entity_to_predicates_obj:
                entity_to_predicates_obj[entity] = entity_to_predicates_lit[entity]
            else:
                entity_to_predicates_obj[entity] = entity_to_predicates_obj[entity].union(entity_to_predicates_lit[entity]) 
        self._entity_to_predicates = entity_to_predicates_obj  
        self._compute_cta_and_cpa_freq()
        
        
    def compute_features(self):
        features = [[] for _ in range(len(self._rows[0]))]
        for row in self._rows:
            for cell in row.get_cells():
                id_col = str(cell._id_col)
                for candidate in cell.candidates():
                    candidate_types_freq = {}
                    for t in candidate["types"]:
                        if t["id"] in self._cta[id_col]:
                            candidate_types_freq[t["id"]] = self._cta[id_col][t["id"]]

                    candidate_predicates_freq = {}
                    for id_predicate in self._entity_to_predicates.get(candidate["id"], {}):
                        if id_predicate in self._cpa[id_col]:
                            candidate_predicates_freq[id_predicate] = self._cpa[id_col][id_predicate]
                    
                    candidate_types_freq = sorted(candidate_types_freq.items(), key=lambda x: x[1], reverse=True)
                    candidate_predicates_freq = sorted(candidate_predicates_freq.items(), key=lambda x: x[1], reverse=True)

                    for i in range(0, 5):
                        freq = 0
                        if i < len(candidate_types_freq):
                            freq = candidate_types_freq[i][1]  
                        candidate["features"][f"cta_t{i+1}"] = round(freq, 3)

                    for i in range(0, 5):
                        freq = 0
                        if i < len(candidate_predicates_freq):
                            freq = candidate_predicates_freq[i][1]  
                        candidate["features"][f"cpa_t{i+1}"] = round(freq, 3)
                    
                    features[int(id_col)].append(list(candidate["features"].values()))

        return features          
                    
            
    def _compute_cta_and_cpa_freq(self):
        for row in self._rows:
            for cell in row.get_cells():
                id_col = str(cell._id_col)
                history = set()
                history_cpa_pair = set()
                for candidate in cell.candidates()[0:3]:
                    types = candidate["types"]
                    for t in types:
                        id_type = t["id"]
                        if id_type in history:
                            continue
                        if id_type not in self._cta[id_col]:
                            self._cta[id_col][id_type] = 0
                        self._cta[id_col][id_type] += 1
                        history.add(id_type)
                    
                    predicates = self._entity_to_predicates.get(candidate["id"], {})
                    for predicate in predicates:
                        if predicate in history:
                            continue
                        if predicate not in self._cpa[id_col]:
                            self._cpa[id_col][predicate] = 0
                        self._cpa[id_col][predicate] += 1
                        history.add(predicate)
                    
                    predicates = candidate["predicates"]
                    for id_col_rel in predicates:
                        if id_col_rel not in self._cpa_pair[id_col]:
                            self._cpa_pair[id_col][id_col_rel] = {}
                        for id_predicate in predicates[id_col_rel]:
                            if id_predicate in history_cpa_pair:
                                continue
                            if id_predicate not in self._cpa_pair[id_col][id_col_rel]:
                                self._cpa_pair[id_col][id_col_rel][id_predicate] = 0    
                            self._cpa_pair[id_col][id_col_rel][id_predicate] += 1 * predicates[id_col_rel][id_predicate]
                            history_cpa_pair.add(id_predicate)          

            
        n_rows = len(self._rows)
        for id_col in self._cta:
            for id_type in self._cta[id_col]:
                self._cta[id_col][id_type] = round(self._cta[id_col][id_type]/n_rows, 3)
            for id_predicate in self._cpa[id_col]:
                self._cpa[id_col][id_predicate] = round(self._cpa[id_col][id_predicate]/n_rows, 3)   
            for id_col_rel in self._cpa_pair[id_col]:
                for id_predicate in self._cpa_pair[id_col][id_col_rel]:
                    self._cpa_pair[id_col][id_col_rel][id_predicate] = round(self._cpa_pair[id_col][id_col_rel][id_predicate]/n_rows, 3)

