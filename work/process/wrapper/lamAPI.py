import os
import requests
from wrapper.URLs import URLs

headers = {
    'accept': 'application/json'
}

LAMAPI_TOKEN =  os.environ["LAMAPI_TOKEN"]

class LamAPI():
    def __init__(self, LAMAPI_HOST, LAMAPI_HOST_PORT, client_key,  response_format="json", kg="wikidata") -> None:
        self.format = response_format
        base_url = f"http://{LAMAPI_HOST}:{LAMAPI_HOST_PORT}/"
        self._url = URLs(base_url, response_format=response_format)
        self.client_key = client_key
        self.kg = kg


    def _exec_post(sefl, params, json_data, url, kg):
        response = requests.post(url, 
                                params=params, 
                                headers=headers, 
                                json=json_data)
        result = response.json()
        if kg in result:
            result = result[kg]
        return result  


    def __to_format(self, response):
        if self.format == "json":
            result = response.json()
            for kg in ["wikidata", "dbpedia", "crunchbase"]:
                if kg in result:
                    result = result[kg]
                    break
            return result
        else:
            raise Exception("Sorry, Invalid format!") 


    def __submit_get(self, url, params):
        return self.__to_format(requests.get(url, headers=headers, params=params))


    def __submit_post(self, url, params, headers, json):
        return self.__to_format(requests.post(url, headers=headers, params=params, json=json))

    # TODO: deprecated will be removed!
    def literal_recognizer(self, column):
        json_data = {
            'json': column
        }
        params = {
            'token': self.client_key
        }
        result = self.__submit_post(self._url.literal_recognizer_url(), params, headers, json_data)
        freq_data = {}
        for cell in result:
            item = result[cell]
            if item["datatype"] == "STRING" and item["datatype"] == item["classification"]:
                datatype = "ENTITY"
            else:
                datatype = item["classification"]  
            if datatype not in freq_data:
                freq_data[datatype] = 0
            freq_data[datatype] += 1   

        return freq_data

    def column_analysis(self, columns):
        json_data = {
            'json': columns
        }
        params = {
            'token': self.client_key
        }
        result = self.__submit_post(self._url.column_analysis_url(), params, headers, json_data)
        return result

    def labels(self, entitites):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entitites
        }
        result = self.__submit_post(self._url.entities_labels(), params, headers, json_data)
        return result


    def objects(self, entitites):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entitites
        }
        result = self.__submit_post(self._url.entities_objects_url(), params, headers, json_data)
        return result


    def predicates(self, entitites):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entitites
        }
        result = self.__submit_post(self._url.entities_predicates_url(), params, headers, json_data)
        return result


    def types(self, entitites):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entitites
        }
        result = self.__submit_post(self._url.entities_types_url(), params, headers, json_data)
        return result


    def literals(self, entitites):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entitites
        }
        result = self.__submit_post(self._url.entities_literals_url(), params, headers, json_data)
        return result


    def lookup(self, string, ngrams=False, fuzzy=False, types=None, description=None, limit=100):
        params = {
            'token': LAMAPI_TOKEN,
            'name': string,
            'ngrams': ngrams,
            'fuzzy': fuzzy,
            'types': types,
            'kg': self.kg,
            'description': description,
            'limit': limit
        }
        result = self.__submit_get(self._url.lookup_url(), params)
        if len(result) > 1:
            result = {"wikidata": result}
        return result

