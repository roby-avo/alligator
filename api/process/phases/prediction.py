import tensorflow as tf

class Prediction:
    def __init__(self, rows, feautures, model):
        self._rows = rows
        self._model = model
        self._features = feautures
        
    def compute_prediction(self, feature_name):
        prediction = []
        indexes = []
        for column_features in self._features:
            pred = [] 
            if len(column_features) > 0:
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
            
            