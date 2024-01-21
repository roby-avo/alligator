from urllib.parse import urljoin

class URLs:
    def __init__(self, base_url, response_format="json"):
        self.format = response_format

        self.base_url = base_url

        #lookup
        self.lookup = "lookup/entity-retrieval"

        # entity
        self.entities_labels = "entity/labels"
        self.entities_objects = "entity/objects"
        self.entities_predicates = "entity/predicates"
        self.entities_types = "entity/types"
        self.entities_literals = "entity/literals"

        #classify
        self._literal_recognizer = "classify/literal-recognizer"

        #sti
        self._column_analysis = "sti/column-analysis"
        

    def base_url(self):
        return self.base_url

    def lookup_url(self):
        return urljoin(self.base_url, self.lookup)
        
    def entities_labels_url(self):
        return urljoin(self.base_url, self.entities_labels)

    def entities_objects_url(self):
        return urljoin(self.base_url, self.entities_objects)

    def entities_predicates_url(self):
        return urljoin(self.base_url, self.entities_predicates)

    def entities_types_url(self):
        return urljoin(self.base_url, self.entities_types)
    
    def entities_literals_url(self):
        return urljoin(self.base_url, self.entities_literals)

    def literal_recognizer_url(self):
        return urljoin(self.base_url, self._literal_recognizer)

    def column_analysis_url(self):
        return urljoin(self.base_url, self._column_analysis)
        