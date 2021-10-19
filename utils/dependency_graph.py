from utils.table_node import TableNode


class DependencyGraph:

    def __init__(self):
        self._dependencies = {}
        self._table_nodes = {}
        self._root_nodes = []
        self._task_list = []

    def add_all(self, iterable_of_table_dependencies):
        for table_dependency in iterable_of_table_dependencies:
            self.add(table_dependency)

    def add(self, table_dependency):
        output_table = table_dependency['output_table']
        dep = self._dependencies.get(output_table, set())
        new_deps = dep.union(table_dependency.get('source_tables', set()))
        self._dependencies[output_table] = new_deps

    def get_task_list(self):
        if len(self._dependencies) != len(self._table_nodes):
            self._create_graph()
        return self._generate_task_list()

    def _generate_task_list(self):
        for root in self._root_nodes:
            self._task_list.insert(0, root)
        for root in self._root_nodes:
            self._unpack_dependencies_to_list(self._table_nodes[root])
        return self._task_list

    def _unpack_dependencies_to_list(self, table_node):
        for dependency_table_node in table_node.dependencies:
            try:
                self._task_list.remove(dependency_table_node.table_name)
            except ValueError:
                pass
            self._task_list.insert(0, dependency_table_node.table_name)
        for dependency_table_node in table_node.dependencies:
            self._unpack_dependencies_to_list(dependency_table_node)

    def _create_graph(self):
        self._create_table_nodes_dict()
        self._identify_root_nodes()

    def _create_table_nodes_dict(self):
        for table_name in self._dependencies:
            self._table_nodes[table_name] = TableNode(table_name=table_name)
        for table_name in self._table_nodes:
            node = self._table_nodes[table_name]
            for dep in self._dependencies[table_name]:
                if dep in self._table_nodes:
                    node.dependencies.append(self._table_nodes[dep])

    def _identify_root_nodes(self):
        possible_roots = {table_name for table_name in self._table_nodes}
        for table_name in self._table_nodes:
            deps = self._get_all_dependencies(self._table_nodes[table_name])
            possible_roots = possible_roots.difference(deps)
        self._root_nodes = possible_roots

    def _get_all_dependencies(self, table_node):
        deps = set()
        for dependency_node in table_node.dependencies:
            deps.add(dependency_node.table_name)
            deps = deps.union(self._get_all_dependencies(dependency_node))
        return deps


if __name__ == '__main__':
    g = DependencyGraph()
    dependencies = [
        {'output_table': '`a.c.200_d`',
         'source_tables': {'`a.d.100_e`',
                           '`a.c.100_f`'}},
        {'output_table': '`a.c.100_f`',
         'source_tables': {'`a.b.100_l`',
                           '`a.b.100_k`'}},
        {'output_table': '`a.c.300_d`',
         'source_tables': {'`a.c.200_d`',
                           '`a.c.100_f`'}},
        {'output_table': '`a.c.400_d`',
         'source_tables': {'`a.c.300_d`'}},
        {'output_table': '`a.c.500_d`',
         'source_tables': {'`a.g.100_k`'}}
    ]
    g.add_all(dependencies)
    g._create_graph()
    print(g._generate_task_list())
    print()
