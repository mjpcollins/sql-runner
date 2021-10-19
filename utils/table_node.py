
class TableNode:

    def __init__(self, table_name):
        self.table_name = table_name
        self.dependencies = []
