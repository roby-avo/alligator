
class NeuralNetwork():
    def __init__(self, model: object, name: str) -> None:
        self._model = model
        self._name = name

    def predict(self, input):
        return self._model.predict(input, batch_size=1000)