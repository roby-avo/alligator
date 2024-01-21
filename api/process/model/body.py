body = {
    "token": {
        "match": {
            "name": {
                "query": "",
                "operator": ""
            }
        }
    },
    "token+filter": {
       "bool": {
            "should": [
                {"match": {"name": {"query": ""}}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}}
            ]
        }
    },
    "fuzzy": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "fuzziness": "auto"}}}
            ]
        }
    },
    "ngrams": {
        "bool": {
            "should": [
                {"match": {"name.ngrams": {"query": ""}}},
            ]
        }
    },
    "token+ngrams": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"name.ngrams": {"query": ""}}},
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}}
            ]
        }
    },
    "token+ngrams+rank": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"name.ngrams": {"query": ""}}},
                {"rank_feature": {"field": "popularities"}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}}
            ]
        }
    },
    "token+ngrams+fuzzy": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"name.ngrams": {"query": ""}}},
                {"match": {"name": {"query": "", "fuzziness": "auto"}}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}},
                {"range": {"length": {"gte": "","lte": ""}}}
            ]
        }
    },
    "token+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"type": {"query": ""}}},
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}},
                {"range": {"length": {"gte": "","lte": ""}}}
            ]
        }
    },
    "fuzzy+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "fuzziness": "auto"}}},
                {"match": {"type": ""}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "","lte": ""}}},
                {"range": {"length": {"gte": "","lte": ""}}}
            ]
        }
    },
    "ngrams+type": {
        "bool": {
            "should": [
                {"match": {"name.ngrams": {"query": ""}}},
                {"match": {"type": ""}}
            ]
        }
    },
    "token+fuzzy+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": ""}}},
                {"match": {"name": {"query": "", "fuzziness": "auto"}}},
                {"match": {"type": {"query": ""}}}
            ]
        }
    },
    "token+ngrams+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"name.ngrams": {"query": ""}}},
                {"match": {"type": {"query": ""}}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "", "lte": ""}}}
            ]
        }
    },
    "token+ngrams+rank+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": "", "boost": 2}}},
                {"match": {"name.ngrams": {"query": ""}}},
                {"match": {"type": {"query": ""}}},
                {"rank_feature": {"field": "popularities"}}
            ],
            "must": [
                {"range": {"ntoken": {"gte": "", "lte": ""}}}
            ]
        }
    },
    "token+fuzzy+ngrams+type": {
        "bool": {
            "should": [
                {"match": {"name": {"query": ""}}},
                {"match": {"name": {"query": "", "fuzziness": "auto"}}},
                {"match": {"name.ngrams": {"query": ""}}},
                {"match": {"type": ""}}
            ]
        }
    }
}