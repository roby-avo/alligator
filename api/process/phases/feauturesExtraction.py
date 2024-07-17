import asyncio
import utils.metrics as metrics
import utils.utils as utils

class FeauturesExtraction:
    def __init__(self, rows, lamAPI):
        self._rows = rows
        self._lamAPI = lamAPI
        self._cache_obj = {}
        self._cache_lit = {}
        self._lock_obj = asyncio.Lock()  # Lock for cache_obj
        self._lock_lit = asyncio.Lock()  # Lock for cache_lit
        
    async def compute_feautures(self):
        tasks = []
        for row in self._rows:
            ne_cells = row.get_ne_cells()
            lit_cells = row.get_lit_cells()
            cells = row.get_cells()
            tasks.append(asyncio.create_task(self._compute_rows(ne_cells, lit_cells, cells, row)))
            
        await asyncio.gather(*tasks)    
        
        return self._extract_features()

    async def _compute_rows(self, ne_cells, lit_cells, cells, row):
        tasks = []
        for ne_cell in ne_cells:
            for cell in cells:
                if cell == ne_cell:
                    continue
                elif cell.is_lit_cell:
                    await self._match_lit_cells(ne_cell, cell, row, len(lit_cells))
                else:
                    await self._compute_similarity_between_ne_cells(ne_cell, cell, len(ne_cells))
                            
    def _extract_features(self):
        features = [[] for _ in range(len(self._rows[0]))]
        for row in self._rows:
            for cell in row.get_cells():
                for candidate in cell.candidates():
                    features[cell._id_col].append(list(candidate["features"].values()))
        return features    

    async def _compute_similarity_between_ne_cells(self, subj_cell, obj_cell, nNE_cells):
        async with self._lamAPI.semaphore:

            subj_id_candidates = [candidate["id"] for candidate in subj_cell.candidates() if candidate["id"] not in self._cache_obj]

            if len(subj_id_candidates) > 0:
                subjects_objects = await self._lamAPI.objects(subj_id_candidates)

            object_rel_score_buffer = {}
            for subj_candidate in subj_cell.candidates():
                id_subject = subj_candidate["id"]

                async with self._lock_obj:
                    if id_subject not in self._cache_obj:
                        subj_candidates_objects = subjects_objects.get(id_subject, {}).get("objects", {})
                        self._cache_obj[id_subject] = subj_candidates_objects
                    else:    
                        subj_candidates_objects = self._cache_obj.get(id_subject, {})
                        
                objects_set = set(subj_candidates_objects.keys())
                obj_score_max = 0
                objects_itersection = objects_set.intersection(set([candidate["id"] for candidate in obj_cell.candidates()]))
                for obj_candidate in obj_cell.candidates():
                    id_object = obj_candidate["id"] 
                    if id_object not in objects_itersection:
                        continue
                    
                    string_similarity_features = [obj_candidate["features"][f] for f in ["ed_score", "jaccard_score", "jaccardNgram_score"]]
                    p_subj_ne = round(sum(string_similarity_features) / len(string_similarity_features), 3)

                    if p_subj_ne > obj_score_max:
                        obj_score_max = p_subj_ne
                    
                    if id_object not in object_rel_score_buffer:
                        object_rel_score_buffer[id_object] = 0
                    
                    string_similarity_features = [subj_candidate["features"][f] for f in ["ed_score", "jaccard_score", "jaccardNgram_score"]]
                    score_rel = round(sum(string_similarity_features) / len(string_similarity_features), 3)
                    if score_rel > object_rel_score_buffer[id_object]:
                        object_rel_score_buffer[id_object] = score_rel
                    for predicate in subj_candidates_objects[id_object]:
                        subj_candidate["matches"][str(obj_cell._id_col)].append({
                            "p": predicate,
                            "o": id_object,
                            "s": round(p_subj_ne, 3)
                        })
                        subj_candidate["predicates"][str(obj_cell._id_col)][predicate] = p_subj_ne
                subj_candidate["features"]["p_subj_ne"] += round(obj_score_max/ nNE_cells, 3)
            
            for obj_candidate in obj_cell.candidates():
                id_object = obj_candidate["id"]  
                if id_object not in object_rel_score_buffer:
                    continue
                obj_candidate["features"]["p_obj_ne"] += round(object_rel_score_buffer[id_object]/nNE_cells, 3)  

    
    def _get_literal_values_string(self, subj_literals):
        lit_strings = []
        for datatype in subj_literals:
            for predicate in subj_literals[datatype]:
                for value in subj_literals[datatype][predicate]:
                    if value.startswith('+') and value[1:].isdigit():
                        value = value[1:]
                    lit_strings.append(utils.clean_str(value))
        return " ".join(lit_strings)


    async def _match_lit_cells(self, subj_cell, obj_cell, row, nLIT_cells):
        async with self._lamAPI.semaphore:

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

        
            subj_id_candidates = [candidate["id"] for candidate in subj_cell.candidates() if candidate["id"] not in self._cache_lit]
            if len(subj_id_candidates) > 0:
                subjects_literals = await self._lamAPI.literals(subj_id_candidates)
                if len(subjects_literals) == 0:
                    return
                
            datatype = obj_cell.datatype
            
            for subj_candidate in subj_cell.candidates():
                id_subject = subj_candidate["id"]

                async with self._lock_lit:            
                    if id_subject not in self._cache_lit:
                        subj_literals = subjects_literals.get(id_subject, {}).get("literals", {})
                        self._cache_lit[id_subject] = subj_literals
                    else:   
                        subj_literals = self._cache_lit.get(id_subject, {})
                
                lit_string = self._get_literal_values_string(subj_literals)
                row_text_all = utils.clean_str(row.get_text())
                row_text_lit = utils.clean_str(row.get_text({"LIT"}))
                p_subj_lit_all_datatype = metrics.compute_similarity_between_string_token_based(lit_string, row_text_lit)
                p_subj_lit_row = metrics.compute_similarity_between_string_token_based(lit_string, row_text_all)
                subj_candidate["features"]["p_subj_lit_all_datatype"] = round(p_subj_lit_all_datatype, 3)
                subj_candidate["features"]["p_subj_lit_row"] = round(p_subj_lit_row, 3)

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
                                
                subj_candidate["features"]["p_subj_lit_datatype"] += round(max_score/nLIT_cells, 3)
