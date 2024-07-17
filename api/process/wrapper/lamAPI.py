import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import aiohttp
import asyncio
import traceback
from wrapper.URLs import URLs
from aiohttp_retry import RetryClient, ExponentialRetry


headers = {
    'accept': 'application/json'
}


class LamAPI():
    def __init__(self, host, client_key, database, response_format="json", kg="wikidata", max_concurrent_requests=50) -> None:
        self.format = response_format
        self.database = database
        self._url = URLs(host, response_format=response_format)
        self.client_key = client_key
        self.kg = kg
        # Initialize the semaphore with the max_concurrent_requests limit
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def __to_format(self, response):
        try:
            result = await response.json()
            return result
        except aiohttp.ContentTypeError:
            return {"error": "Invalid JSON response"}
        except Exception as e:
            return {"error": str(e)}

    async def __submit_get(self, url, params):
        try:
            retry_options = ExponentialRetry(attempts=3, start_timeout=3, max_timeout=10)
            timeout = aiohttp.ClientTimeout(total=1000)  # Adjusted timeout
            async with self.semaphore:
                async with RetryClient(connector=aiohttp.TCPConnector(ssl=False), retry_options=retry_options) as session:
                    async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
                        return await self.__to_format(response)
        except Exception as e:
            self.__log_error("GET", url, params, str(e))
            return {"error": str(e)}  # Return a structured error message.

    async def __submit_post(self, url, params, json_data):
        try:
            retry_options = ExponentialRetry(attempts=3, start_timeout=3, max_timeout=10)
            timeout = aiohttp.ClientTimeout(total=120)  # Adjusted timeout
            async with self.semaphore:
                async with RetryClient(connector=aiohttp.TCPConnector(ssl=False), retry_options=retry_options) as session:
                    async with session.post(url, headers=headers, params=params, json=json_data, timeout=timeout) as response:
                        return await self.__to_format(response)
        except Exception as e:
            self.__log_error("POST", url, params, str(e), json_data)
            return {"error": str(e)}  # Return a structured error message.

    def __log_error(self, method, url, params, error_message, json_data=None):
        # Use a generic or specific error type based on the exception.
        error_type = "timeout" if "TimeoutError" in error_message else "generic"
        traceback_info = traceback.format_exc()

        self.database.get_collection("log").insert_one({
            "type": error_type,
            "method": method,
            "url": url,
            "params": params,
            "json_data": json_data,
            "error_message": error_message,
            "stack_trace": traceback_info,
        })
                
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
        result =  await self.__submit_post(self._url.column_analysis_url(), params, json_data)
        result = result if result is not None else []
        return result

    async def labels(self, entities):
        params = {
            'token': self.client_key,
            'lang': 'en',
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_labels_url(), params, json_data)
        result = result if result is not None else {}
        return result

    async def objects(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_objects_url(), params, json_data)
        result = result if result is not None else {}
        return result
    
    async def predicates(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_predicates_url(), params, json_data)
        result = result if result is not None else {}
        return result

    async def types(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_types_url(), params, json_data)
        result = result if result is not None else {}

    async def literals(self, entities):
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_literals_url(), params, json_data)
        result = result if result is not None else {}
        return result

    async def lookup(self, string, fuzzy=False, types=None, limit=1000, ids=None, kind=None, NERtype=None, language=None, query=None):
        # Convert boolean values to strings
        fuzzy_str = 'true' if fuzzy else 'false'
        types_str = ' '.join(types) if types is not None else ''
        ids_str = ' '.join(ids) if ids is not None else ''

        params = {
            'token': self.client_key,
            'name': string,
            'fuzzy': fuzzy_str,
            'kg': self.kg,
            'limit': limit,
            'types': types_str,
            'ids': ids_str,
            'kind': kind,
            'NERtype': NERtype,
            'language': language,
            'query': query
        }

        # Remove any empty parameters
        params = {k: v for k, v in params.items() if v}

        result = await self.__submit_get(self._url.lookup_url(), params)
        result = result if result is not None else []
        return result
    