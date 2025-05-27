import aiohttp
import asyncio
import json
import os
from .lamAPICache import LamAPICache
from .URLs import URLs  # Using relative import instead of wrapper.URLs


class LamAPI:
    def __init__(self, host, token, mongoDBWrapper, kg="wikidata"):
        """
        Initialize the LamAPI with caching capabilities.
        
        Args:
            host: API host
            token: API token
            mongoDBWrapper: MongoDB wrapper instance
            kg: Knowledge graph to use
        """
        self.host = host
        self.token = token
        self.kg = kg
        self.semaphore = asyncio.Semaphore(30)  # Limit concurrent requests
        self.cache = LamAPICache(mongoDBWrapper)
        self._url = URLs(host, response_format="json")
        self.client_key = token
        self.format = "json"
        self.database = mongoDBWrapper
        
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
            headers = {"accept": "application/json"}
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params, timeout=timeout) as response:
                        return await self.__to_format(response)
        except Exception as e:
            self.__log_error("GET", url, params, str(e))
            return {"error": str(e)}

    async def __submit_post(self, url, params, json_data):
        try:
            headers = {"accept": "application/json", "Content-Type": "application/json"}
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, params=params, json=json_data, timeout=timeout) as response:
                        return await self.__to_format(response)
        except Exception as e:
            self.__log_error("POST", url, params, str(e), json_data)
            return {"error": str(e)}

    def __log_error(self, method, url, params, error_message, json_data=None):
        error_type = "timeout" if "TimeoutError" in error_message else "generic"
        traceback_info = ""
        
        self.database.get_collection("log").insert_one({
            "type": error_type,
            "method": method,
            "url": url,
            "params": params,
            "json_data": json_data,
            "error_message": error_message,
            "stack_trace": traceback_info,
        })

    # Adding the missing methods from original lamAPI.py
    
    async def column_analysis(self, columns):
        """
        Perform column analysis with caching
        """
        # Check cache first
        cache_key = "column_analysis"
        cached_data, found = self.cache.get(cache_key, columns)
        
        if found:
            return cached_data
        
        json_data = {
            'json': [columns]
        }
        params = {
            'model_type': 'fast',    
            'token': self.client_key
        }
        result = await self.__submit_post(self._url.column_analysis_url(), params, json_data)
        
        # Process the result to match the expected format
        processed_result = {}
        
        if result is not None and len(result) > 0:
            # Get the table data from the first result element
            table_data = result[0].get("table_1", {})
            
            # Process each column directly from the table_data
            for col_idx in table_data:
                processed_result[col_idx] = table_data[col_idx]
        
        self.cache.set(cache_key, columns, processed_result)
        return processed_result
    
    async def literal_recognizer(self, column):
        """
        Recognize literals with caching
        """
        cache_key = f"literal_recognizer"
        cached_data, found = self.cache.get(cache_key, column)
        
        if found:
            return cached_data
            
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
        
        self.cache.set(cache_key, column, freq_data)
        return freq_data
        
    async def labels(self, entities):
        """
        Get labels for entity IDs with caching
        """
        cache_key = f"labels"
        cached_data, found = self.cache.get(cache_key, entities)
        
        if found:
            return cached_data
            
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
        
        self.cache.set(cache_key, entities, result)
        return result
    
    async def objects(self, entities):
        """
        Get objects for entity IDs with caching
        """
        cache_key = f"objects"
        cached_data, found = self.cache.get(cache_key, entities)
        
        if found:
            return cached_data
            
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_objects_url(), params, json_data)
        result = result if result is not None else {}
        
        self.cache.set(cache_key, entities, result)
        return result
    
    async def predicates(self, entities):
        """
        Get predicates for entity IDs with caching
        """
        cache_key = f"predicates"
        cached_data, found = self.cache.get(cache_key, entities)
        
        if found:
            return cached_data
            
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_predicates_url(), params, json_data)
        result = result if result is not None else {}
        
        self.cache.set(cache_key, entities, result)
        return result

    async def types(self, entities):
        """
        Get types for entity IDs with caching
        """
        cache_key = f"types"
        cached_data, found = self.cache.get(cache_key, entities)
        
        if found:
            return cached_data
            
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_types_url(), params, json_data)
        result = result if result is not None else {}
        
        self.cache.set(cache_key, entities, result)
        return result
    
    async def literals(self, entities):
        """
        Get literals for entity IDs with caching
        """
        cache_key = f"literals"
        cached_data, found = self.cache.get(cache_key, entities)
        
        if found:
            return cached_data
            
        params = {
            'token': self.client_key,
            'kg': self.kg
        }
        json_data = {
            'json': entities
        }
        result = await self.__submit_post(self._url.entities_literals_url(), params, json_data)
        result = result if result is not None else {}
        
        self.cache.set(cache_key, entities, result)
        return result

    async def lookup(self, string, fuzzy=False, types=None, limit=1000, ids=None, kind="entity", NERtype=None, language=None, query=None):
        """
        Lookup entities with caching
        """
        # Create a cache key based on all parameters
        cache_params = {
            'string': string,
            'fuzzy': fuzzy,
            'types': types,
            'limit': limit,
            'ids': ids,
            'kind': kind,
            'NERtype': NERtype,
            'language': language,
            'query': query
        }
        
        cache_key = f"lookup"
        cached_data, found = self.cache.get(cache_key, cache_params)
        
        if found:
            return cached_data
        
        # Convert boolean values to strings
        fuzzy_str = 'true' if fuzzy else 'false'
        types_str = ' '.join(types) if types is not None else ''
        ids_str = ' '.join(ids) if ids is not None else ''

        params = {
            'token': self.client_key,
            'name': string,
            'fuzzy': False,
            'kg': self.kg,
            'limit': limit if limit is not None else 20,
            'types': types_str,
            'ids': ids_str,
            'kind': kind or "entity",
            'NERtype': NERtype,
            'language': language or "en",
            'cache': "False",
            'query': query
        }

        # Remove any empty parameters
        params = {k: v for k, v in params.items() if v}

        result = await self.__submit_get(self._url.lookup_url(), params)
        result = result if result is not None else []
        
        self.cache.set(cache_key, cache_params, result)
        return result
