from unittest import TestCase
from utils.dependency_graph import (
    DependencyGraph
)


class TestDependencyGraph(TestCase):

    def setUp(self):
        self.graph = DependencyGraph()
        self.table_dependencies = [
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
        self.dependencies = {'`a.c.200_d`': {'`a.d.100_e`', '`a.c.100_f`'},
                             '`a.c.100_f`': {'`a.b.100_l`', '`a.b.100_k`'},
                             '`a.c.300_d`': {'`a.c.200_d`', '`a.c.100_f`'},
                             '`a.c.400_d`': {'`a.c.300_d`'},
                             '`a.c.500_d`': {'`a.g.100_k`'}}

    def test_add(self):
        for table in self.table_dependencies:
            self.graph.add(table)
        self.assertEqual(self.dependencies,
                         self.graph._dependencies)

    def test_add_all(self):
        self.graph.add_all(self.table_dependencies)
        self.assertEqual(self.dependencies,
                         self.graph._dependencies)

    def test_yield_next_table(self):
        self.graph._dependencies = self.dependencies

    def test_identify_root_nodes(self):
        self.graph._dependencies = self.dependencies
        self.graph._create_graph()
        self.graph._identify_root_nodes()
        expected_root_nodes = {'`a.c.400_d`', '`a.c.500_d`'}
        self.assertEqual(expected_root_nodes,
                         self.graph._root_nodes)

    def test_get_all_dependencies(self):
        self.graph._dependencies = self.dependencies
        self.graph._create_graph()
        expected_deps = {'`a.c.200_d`', '`a.c.100_f`'}
        actual_deps = self.graph._get_all_dependencies(self.graph._table_nodes['`a.c.300_d`'])
        self.assertEqual(expected_deps, actual_deps)

    def test_get_task_list(self):
        self.dependencies = {'`a.c.200_d`': {'`a.d.100_e`', '`a.c.100_f`'},
                             '`a.c.100_f`': {'`a.b.100_l`', '`a.b.100_k`'},
                             '`a.c.300_d`': {'`a.c.200_d`', '`a.c.100_f`'},
                             '`a.c.400_d`': {'`a.c.300_d`'},
                             '`a.c.500_d`': {'`a.g.100_k`'}}
        self.graph._dependencies = self.dependencies
        expected_task_list = ['`a.c.100_f`',
                              '`a.c.200_d`',
                              '`a.c.300_d`',
                              '`a.c.500_d`',
                              '`a.c.400_d`']
        actual_task_list = self.graph.get_task_list()
        self.assertEqual(expected_task_list, actual_task_list)
