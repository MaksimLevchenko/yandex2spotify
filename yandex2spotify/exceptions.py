class NotFoundException(Exception):
    def __init__(self, item_name, query):
        super().__init__(f'Item not found: {item_name}')
        self.item_name = item_name
        self.query = query


class SearchException(Exception):
    def __init__(self, item_name, query, original_exception=None):
        super().__init__(f'Search failed: {item_name}')
        self.item_name = item_name
        self.query = query
        self.original_exception = original_exception
