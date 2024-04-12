import tensorflow as tf

all_features = [
    "ambiguity_mention",
    "ncorrects_tokens",
    "ntoken_mention",
    "ntoken_entity",
    "length_mention",
    "length_entity",
    "popularity",
    "pos_score",
    "es_score",
    "ed_score",
    "jaccard_score",
    "jaccardNgram_score",
    "p_subj_ne",
    "p_subj_lit_datatype",
    "p_subj_lit_all_datatype",
    "p_subj_lit_row",
    "p_obj_ne",
    "desc",
    "descNgram",
    "cta_t1",
    "cta_t2",
    "cta_t3",
    "cta_t4",
    "cta_t5",
    "cpa_t1",
    "cpa_t2",
    "cpa_t3",
    "cpa_t4",
    "cpa_t5",
    "cpa_r1",
    "cpa_r2",
    "cpa_r3",
    "cpa_r4",
    "cpa_r5"
]

features_to_use = {
    "es_score", "ed_score", 
    "jaccard_score", "jaccardNgram_score", 
    "p_subj_ne", "p_subj_lit_datatype",
    "p_subj_lit_all_datatype", "p_obj_ne", 
    "desc", "descNgram"
}

class Prediction:
    def __init__(self, rows, feautures, model):
        self._rows = rows
        self._model = model
        self._features = feautures
        
    def compute_prediction(self, feature_name, deteministic=False):
        prediction = []
        indexes = []
        for column_features in self._features:
            pred = [] 
            if len(column_features) > 0:
                if deteministic:
                    for features in column_features:
                        score = sum([v for i, v in enumerate(features) if all_features[i] in features_to_use]) / len(features_to_use)
                        pred.append([0, score])
                else:
                    pred = self._model.predict(tf.convert_to_tensor(column_features))
            prediction.append(pred)
            indexes.append(0)
        
        for row in self._rows:
            cells = row.get_cells()
            for cell in cells:
                candidates = cell.candidates()
                for candidate in candidates:
                    index = indexes[cell._id_col]
                    indexes[cell._id_col] += 1
                    feature = round(float(prediction[cell._id_col][index][1]), 3)
                    candidate[feature_name] = feature
                
                candidates.sort(key=lambda x:x[feature_name], reverse=True)
            
            