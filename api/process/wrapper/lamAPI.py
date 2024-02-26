import os
import aiohttp
from wrapper.URLs import URLs

headers = {
    'accept': 'application/json'
}

LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]

class LamAPI():
    def __init__(self, LAMAPI_HOST, client_key, response_format="json", kg="wikidata") -> None:
        self.format = response_format
        base_url = LAMAPI_HOST
        self._url = URLs(base_url, response_format=response_format)
        self.client_key = client_key
        self.kg = kg

    async def _exec_post(self, params, json_data, url, kg):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.post(url, params=params, headers=headers, json=json_data) as response:
                result = await response.json()
                if kg in result:
                    result = result[kg]
                return result

    async def __to_format(self, response):
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            if self.format == "json":
                result_json = await response.json()
                for kg in ["wikidata", "dbpedia", "crunchbase"]:
                    if kg in result_json:
                        return result_json[kg]
                return result_json  # If none of the keys are found, return the original JSON data
            else:
                raise Exception("Sorry, Invalid format!")
        else:
            # Handle non-JSON response here
            print(await response.text(), flush=True)
            return {}  # or raise an appropriate exception


    async def __submit_get(self, url, params):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.get(url, headers=headers, params=params) as response:
                return await self.__to_format(response)

    async def __submit_post(self, url, params, json_data):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            async with session.post(url, headers=headers, params=params, json=json_data) as response:
                return await self.__to_format(response)

    async def literal_recognizer(self, column):
        json_data = {
            'json': column
        }
        params = {
            'token': self.client_key
        }
        result = await self.__submit_post(self._url.literal_recognizer_url(), params, json_data)
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

    async def column_analysis(self, columns):
        json_data = {
            'json': columns
        }
        params = {
            'token': self.client_key
        }
        return await self.__submit_post(self._url.column_analysis_url(), params, json_data)

    async def labels(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        return await self.__submit_post(self._url.entities_labels(), params, json_data)

    async def objects(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        return await self.__submit_post(self._url.entities_objects_url(), params, json_data)

    async def predicates(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        return await self.__submit_post(self._url.entities_predicates_url(), params, json_data)

    async def types(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        return await self.__submit_post(self._url.entities_types_url(), params, json_data)

    async def literals(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        return await self.__submit_post(self._url.entities_literals_url(), params, json_data)

    async def lookup(self, string, ngrams=False, fuzzy=False, types=None, limit=100, ids=None):
        # Convert boolean values to strings
        ngrams_str = 'true' if ngrams else 'false'
        fuzzy_str = 'true' if fuzzy else 'false'
        types_str = ' '.join(types) if types else ''  # Provide default value if types is None
        ids_str = ' '.join(ids) if ids else ''  # Provide default value if ids is None
        
        params = {
            'token': LAMAPI_TOKEN,
            'name': string,
            'ngrams': ngrams_str,
            'fuzzy': fuzzy_str,
            'types': types_str,
            'kg': self.kg,
            'limit': limit
        }
        result = await self.__submit_get(self._url.lookup_url(), params)
        if len(result) > 1:
            result = {"wikidata": result}

        return result
