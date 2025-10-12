import unittest

from fdwh_import.utils.get_subfolder import validate_subfolder_fmt, get_subfolder_from_path


class TestSubfolderMethods(unittest.TestCase):

    def test_validate_subfolder_fmt_valid(self):
        self.assertTrue(validate_subfolder_fmt("2023-06-10 description"))
        self.assertTrue(validate_subfolder_fmt("2020-01-01 New Year"))
        self.assertTrue(validate_subfolder_fmt("1999-12-31 End of the century"))

    def test_validate_subfolder_fmt_invalid_date(self):
        self.assertFalse(validate_subfolder_fmt("1999-13-31 End of the century"))
        self.assertFalse(validate_subfolder_fmt("2023-06-32 Invalid date"))
        self.assertFalse(validate_subfolder_fmt("2023-02-30 Invalid date"))
        self.assertFalse(validate_subfolder_fmt("2023-04-31 Invalid date"))

    def test_validate_subfolder_fmt_invalid_format(self):
        self.assertFalse(validate_subfolder_fmt("2023/06/10 description"))
        self.assertFalse(validate_subfolder_fmt("2023-06-10"))
        self.assertFalse(validate_subfolder_fmt("2023-13-01 Invalid month"))
        self.assertFalse(validate_subfolder_fmt("Invalid date format"))

    def test_get_subfolder_from_prefix_valid(self):
        self.assertEqual(get_subfolder_from_path("2023-06-10 description/other/path"), "2023-06-10 description")
        self.assertEqual(get_subfolder_from_path("2020-01-01 New Year/another/path"), "2020-01-01 New Year")

    def test_get_subfolder_from_prefix_invalid(self):
        self.assertIsNone(get_subfolder_from_path("2023-06-10/other/path"))
        self.assertIsNone(get_subfolder_from_path("Invalid format/other/path"))
        self.assertIsNone(get_subfolder_from_path("2023-06-10/"))

    def test_get_subfolder_from_prefix_invalid_future_date(self):
        self.assertIsNone(get_subfolder_from_path("2099-06-32 description/other/path"))
        self.assertIsNone(get_subfolder_from_path("2099-11-31 New Year/another/path"))
        self.assertIsNone(get_subfolder_from_path("2025-13-01 New Year/another/path"))
