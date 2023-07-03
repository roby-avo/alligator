import utils.metrics as metrics


class FeauturesExtraction:
    def __init__(self, rows, lamAPI):
        self._rows = rows
        self._lamAPI = lamAPI
        
        
    def compute_feautures(self):
        for row in self._rows:
            ne_cells = row.get_ne_cells()
            cells = row.get_cells()
            for ne_cell in ne_cells:
                for cell in cells:
                    if cell == ne_cell:
                        continue
                    elif cell.is_lit_cell:
                        self._match_lit_cells(ne_cell, cell)
                    else:
                        self._compute_similarity_between_ne_cells(ne_cell, cell)
        
        return self._extract_features()


    def _extract_features(self):
        features = [[] for _ in range(len(self._rows[0]))]
        for row in self._rows:
            for cell in row.get_cells():
                for candidate in cell.candidates():
                    features[cell._id_col].append(list(candidate["features"].values()))
        return features    


    def _compute_similarity_between_ne_cells(self, subj_cell, obj_cell):
        subjects_objects = self._lamAPI.objects([candidate["id"] for candidate in subj_cell.candidates()])
        object_rel_score_buffer = {}
        for subj_candidate in subj_cell.candidates():
            id_subject = subj_candidate["id"]
            subj_candidates_objects = subjects_objects.get(id_subject, {}).get("objects", {})
            objects_set = set(subj_candidates_objects.keys())
            obj_score_max = 0
            objects_itersection = objects_set.intersection(set([candidate["id"] for candidate in obj_cell.candidates()]))
            for obj_candidate in obj_cell.candidates():
                id_object = obj_candidate["id"] 
                if id_object not in objects_itersection:
                    continue

                p_subj_ne = obj_candidate["features"]["ed"]
                if p_subj_ne > obj_score_max:
                    obj_score_max = p_subj_ne
                   
                if id_object not in object_rel_score_buffer:
                    object_rel_score_buffer[id_object] = 0
                score_rel = subj_candidate["features"]["ed"]
                if score_rel > object_rel_score_buffer[id_object]:
                    object_rel_score_buffer[id_object] = score_rel
                for predicate in subj_candidates_objects[id_object]:
                    subj_candidate["matches"][str(obj_cell._id_col)].append({
                        "p": predicate,
                        "o": id_object,
                        "s": round(p_subj_ne, 3)
                    })
                    subj_candidate["predicates"][str(obj_cell._id_col)][predicate] = p_subj_ne
            subj_candidate["features"]["p_subj_ne"] += obj_score_max
        
        for obj_candidate in obj_cell.candidates():
            id_object = obj_candidate["id"]  
            if id_object not in object_rel_score_buffer:
                continue
            obj_candidate["features"]["p_obj_ne"] += object_rel_score_buffer[id_object]  

        

    def _match_lit_cells(self, subj_cell, obj_cell):
    
        def get_score_based_on_datatype(valueInCell, valueFromKG, datatype):
            score = 0
            valueFromKG = str(valueFromKG)
            if datatype == "NUMBER":
                score = metrics.compute_similarty_between_numbers(valueInCell, valueFromKG.lower())
            elif datatype == "DATETIME":
                score = metrics.compute_similarity_between_dates(valueInCell, valueFromKG.lower())
            elif datatype == "STRING":
                score = metrics.compute_similarity_between_string(valueInCell, valueFromKg.lower())
            return score

        subjects_literals = self._lamAPI.literals([candidate["id"] for candidate in subj_cell.candidates()])
        datatype = obj_cell.datatype
        
        for subj_candidate in subj_cell.candidates():
            id_subject = subj_candidate["id"]
            subj_literals = subjects_literals.get(id_subject, {}).get("literals", {})

            new_datatype = datatype

            if datatype.lower() in subj_literals:
                new_datatype = datatype.lower()
            
            if new_datatype not in subj_literals or len(subj_literals[new_datatype]) == 0:
                continue

            max_score = 0
            for predicate in subj_literals[new_datatype]:
                for valueFromKg in subj_literals[new_datatype][predicate]:
                    p_subj_lit = get_score_based_on_datatype(obj_cell.content, valueFromKg, datatype)
                    p_subj_lit = round(p_subj_lit, 3)
                    if p_subj_lit > 0:
                        subj_candidate["matches"][str(obj_cell._id_col)].append({
                            "p": predicate,
                            "o": valueFromKg,
                            "s": p_subj_lit
                        })  
                        if p_subj_lit > max_score:
                            max_score = p_subj_lit
                        if predicate not in subj_candidate["predicates"][str(obj_cell._id_col)]:
                            subj_candidate["predicates"][str(obj_cell._id_col)][predicate] = 0
                        if p_subj_lit > subj_candidate["predicates"][str(obj_cell._id_col)][predicate]:
                            subj_candidate["predicates"][str(obj_cell._id_col)][predicate] = p_subj_lit    
                            
            subj_candidate["features"]["p_subj_lit"] += round(max_score, 3) 
