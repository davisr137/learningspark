import unittest2 as unittest

import python.quick_start as qs

class TestQuickStart(unittest.TestCase):
    """
    Test code from 'Quick Start' section exercises.
    """
    @classmethod
    def setUpClass(cls):
        """
        Read README.md file.
        """ 
        cls.textFile = qs.read_readme_file()
    
    def test_count(self):
        """
        Test row count.
        """
        self.assertEqual(self.textFile.count(), 105)


