# coding: utf-8
from util import convert_to_utf_8
import filecmp
import unittest
import codecs
import tempfile

class TestConvertToUTF8(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        reference_file_name = tempfile.mkstemp()[1]
        with codecs.open(reference_file_name, 'w', 'utf-8') as reference_file:
            reference_file.write(u'ÁÍÏÐÝè\n')

        cls.reference_file_name = reference_file_name

    def test_utf_8(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/utf-8.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )

    def test_utf_16le(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/utf-16le.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )

    def test_utf_16be(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/utf-16be.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )

    def test_utf_32le(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/utf-32le.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )

    def test_utf_32be(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/utf-32be.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )

    def test_windows_1252(self):
        self.assertListEqual(
            list(codecs.open(convert_to_utf_8('test/test_files/encoding/windows-1252.txt'), 'r', 'utf-8')),
            list(codecs.open(self.reference_file_name, 'r', 'utf-8'))
        )
