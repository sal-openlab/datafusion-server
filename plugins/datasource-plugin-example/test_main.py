# -*- coding: utf-8 -*-
""" Test code for plugin_main.py
"""

import unittest
import plugin_main


class QueryTests(unittest.TestCase):
    def simple_query(self):
        res = plugin_main.main("json", "api.publicapis.org", "/entries")
        self.assertTrue(res.startswith("[{"))


if __name__ == "__main__":
    unittest.main()
