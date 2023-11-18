from sklearn import preprocessing

class LogisticRegressor():
    def __init__(self, model: object, name: str) -> None:
        self._model = model
        self._name = name

    def predict(self, input):
        scaler = preprocessing.StandardScaler().fit(input)
        input = scaler.transform(input)
        return self._model.predict_proba(input)