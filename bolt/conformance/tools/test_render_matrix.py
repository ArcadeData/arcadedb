#!/usr/bin/env python3
import os
import unittest

from render_matrix import load_columns, Column

HERE = os.path.dirname(__file__)
DRIVER_VERSIONS_MD = os.path.join(HERE, "..", "driver-versions.md")


class LoadColumnsTest(unittest.TestCase):
    def test_parses_all_fifteen_columns_in_order(self):
        cols = load_columns(DRIVER_VERSIONS_MD)
        self.assertEqual(len(cols), 15)
        # File order: java first, go last; header/separator rows ignored.
        self.assertEqual(cols[0], Column("java", "oldest-supported-4.x", "4.4.20"))
        self.assertEqual(cols[-1], Column("go", "latest", "5.28.4"))
        self.assertTrue(all(c.language in
            {"java", "javascript", "python", "csharp", "go"} for c in cols))


if __name__ == "__main__":
    unittest.main()
