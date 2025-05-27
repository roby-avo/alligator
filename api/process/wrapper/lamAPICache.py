import hashlib
import json
import time
from datetime import datetime, timedelta

class LamAPICache:
    def __init__(self, mongodb_wrapper, cache_expiry_days=30):
        """
        Initialize the cache with MongoDB wrapper and set expiry time for cached items.
        
        Args:
            mongodb_wrapper: Instance of MongoDBWrapper
            cache_expiry_days: Number of days after which cache entries expire
        """
        self.mongodb_wrapper = mongodb_wrapper
        self.cache_collection = mongodb_wrapper.get_collection('requestCache')
        self.cache_expiry_days = cache_expiry_days
        self._clean_expired_cache()
        
    def _generate_key(self, endpoint, params):
        """
        Generate a unique hash key based on the endpoint and parameters
        
        Args:
            endpoint: API endpoint
            params: Dictionary of parameters or list of IDs
        
        Returns:
            str: Unique hash key
        """
        # Convert params to a consistent string representation
        if isinstance(params, list):
            params_str = json.dumps(sorted(params))
        elif isinstance(params, dict):
            params_str = json.dumps(params, sort_keys=True)
        else:
            params_str = str(params)
            
        key_string = f"{endpoint}:{params_str}"
        return hashlib.md5(key_string.encode()).hexdigest()
        
    def get(self, endpoint, params):
        """
        Try to retrieve data from cache
        
        Args:
            endpoint: API endpoint
            params: Dictionary of parameters or list of IDs
        
        Returns:
            tuple: (data, found) where found is boolean indicating if data was in cache
        """
        key = self._generate_key(endpoint, params)
        cache_item = self.cache_collection.find_one({'request_hash': key})
        
        if cache_item:
            return cache_item['response_data'], True
        
        return None, False
        
    def set(self, endpoint, params, response_data):
        """
        Store API response in cache
        
        Args:
            endpoint: API endpoint
            params: Dictionary of parameters or list of IDs
            response_data: Data to cache
            
        Returns:
            bool: True if data was cached successfully
        """
        key = self._generate_key(endpoint, params)
        
        # Use upsert to handle both insert and update cases
        self.cache_collection.update_one(
            {'request_hash': key},
            {'$set': {
                'request_hash': key,
                'endpoint': endpoint,
                'params': params,
                'response_data': response_data,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }},
            upsert=True
        )
        return True
        
    def _clean_expired_cache(self):
        """Periodically clean expired cache entries"""
        try:
            expiry_date = datetime.now() - timedelta(days=self.cache_expiry_days)
            self.cache_collection.delete_many({'created_at': {'$lt': expiry_date}})
        except Exception as e:
            print(f"Error cleaning expired cache: {e}")
