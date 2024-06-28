import utils.metrics as metrics
import utils.utils as utils


class Cell:
    def __init__(self, content: str, row_content:str, candidates: list, id_col: int, n_cols: int, is_lit_cell=False, is_notag_cell=False, datatype=None):
        self.content = content
        self._id_col = id_col
        self.is_lit_cell = is_lit_cell
        self.is_notag_cell = is_notag_cell
        self.datatype = datatype
        self._candidates = []
        candidates_dict = {}
        
        for candidate in candidates:
            id_candidate = candidate["id"]
            name_norm = utils.clean_str(candidate["name"]) 
            desc_norm = utils.clean_str(candidate["description"])
            row_content_norm = utils.clean_str(row_content)
            desc_score = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm), 3)
            desc_score_ngram = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm, 3), 3)
           
            features = {
                "ambiguity_mention": candidate["ambiguity_mention"],
                "ncorrects_tokens": candidate["corrects_tokens"],
                "ntoken_mention": candidate["ntoken_mention"],
                "ntoken_entity": candidate["ntoken_entity"],
                "length_mention": candidate["length_mention"],
                "length_entity": candidate["length_entity"],
                "popularity": candidate["popularity"],
                "pos_score": candidate["pos_score"],
                "es_score": candidate["es_score"],
                "ed_score": candidate["ed_score"],
                "jaccard_score": candidate["jaccard_score"],
                "jaccardNgram_score": candidate["jaccardNgram_score"],
                "p_subj_ne": 0,
                "p_subj_lit_datatype": 0,
                "p_subj_lit_all_datatype": 0,
                "p_subj_lit_row": 0,
                "p_obj_ne": 0,
                "desc": desc_score,
                "descNgram": desc_score_ngram,
                "cta_t1": 0,
                "cta_t2": 0,
                "cta_t3": 0,
                "cta_t4": 0,
                "cta_t5": 0,
                "cpa_t1": 0,
                "cpa_t2": 0,
                "cpa_t3": 0,
                "cpa_t4": 0,
                "cpa_t5": 0
            }

           
            replace = False
            if id_candidate in candidates_dict:
                score = candidates_dict[id_candidate]["features"]["ed"] + candidates_dict[id_candidate]["features"]["jaccard"]
                if (features["ed"] + features["jaccard"]) > score:
                    replace = True
            
            if id_candidate not in candidates_dict or replace:
                candidates_dict[id_candidate] = { 
                    "id": id_candidate,
                    "name": name_norm,
                    "description": desc_norm, 
                    "types": candidate["types"], 
                    "features": features,
                    "matches": {str(id_col):[] for id_col in range(n_cols)},
                    "predicates": {str(id_col):{} for id_col in range(n_cols)},
                    "match": False
                }

        self._candidates = list(candidates_dict.values())
           

    def candidates(self):
        return self._candidates


    def candidates_name(self, id_entity):
        return self._candidates.get(id_entity, {}).get("name")

    
    def candidates_description(self, id_entity):
        return self._candidates.get(id_entity, {}).get("description")

    
    def candidates_types(self, id_entity):
        return self._candidates.get(id_entity, {}).get("types")


    def candidates_ed(self, id_entity):
        return self._candidates.get(id_entity, {}).get("ed")    


    def get_id_candidates_entities(self):
        return list(self._candidates.keys())    


    def get_set_id_candidates_entities(self):
        return set(self._candidates.keys())    
        