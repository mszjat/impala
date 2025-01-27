# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfFS
from tests.util.filesystem_utils import WAREHOUSE
from tests.common.test_result_verifier import verify_query_result_is_equal
import os
import random
import codecs


def get_table_loc(db, tbl_name):
  return '%s/%s.db/%s/' % (WAREHOUSE, db, tbl_name,)


class TestCharCodec(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCharCodec, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')

  def prepare_encoded_test_table(self, db, tbl_name, datafile, encoding):
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (name STRING)
        STORED AS TEXTFILE""".format(db, tbl_name))
    self.execute_query("""ALTER TABLE {}.{} SET
        SERDEPROPERTIES("serialization.encoding"="{}")"""
        .format(db, tbl_name, encoding))
    data_file_path = os.path.join(os.environ['IMPALA_HOME'], "testdata",
        "charcodec", datafile)
    self.filesystem_client.copy_from_local(data_file_path, get_table_loc(db, tbl_name))
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))

  @SkipIfFS.hive
  def test_dec(self, vector, unique_database):
    db = unique_database
    self.prepare_encoded_test_table(db, 'gbk_names', 'gbk_names.txt', 'gbk')
    self.prepare_encoded_test_table(db, 'latin1_names', 'latin1_names.txt', 'ISO-8859-1')
    self.prepare_encoded_test_table(
      db, 'shift_jis_names', 'shift_jis_names.txt', 'Shift_JIS')
    self.prepare_encoded_test_table(
      db, 'cp1251_names', 'cp1251_names.txt', 'windows-1251')
    self.prepare_encoded_test_table(db, 'koi8r_names', 'koi8r_names.txt', 'KOI8-R')
    self.run_test_case('QueryTest/dec-small', vector, db)

    self.run_test_case('QueryTest/dec-hive', vector, db)
    self.run_test_case('QueryTest/dec-alltypes', vector)

  @SkipIfFS.hive
  def test_enc_dec(self, vector, unique_database):
    db = unique_database
    self.run_test_case('QueryTest/enc-dec-small', vector, db)
    self.run_test_case('QueryTest/enc-dec-big', vector, db)
    self.run_test_case('QueryTest/enc-dec-alltypes', vector, db)
    self.run_test_case('QueryTest/enc-dec-huge', vector, db)


hiragana_range = list(range(0x3040, 0x309F))
problematic_symbols_hiragana = {0x3040, 0x3094, 0x3095, 0x3096, 0x3097, 0x3098, 0x3099,
                                0x309A, 0x309B, 0x309C}

cyrillic_range = list(range(0x0410, 0x045F))
problematic_symbols_koi8r = {
    0x0450, 0x0452, 0x0453, 0x0454, 0x0455, 0x0456, 0x0457, 0x0458,
    0x0459, 0x045A, 0x045B, 0x045C, 0x045D, 0x045E
}

charsets = {
  'gbk': u''.join(unichr(i) for i in range(0x4E00, 0x9FA6)),  # Common Chinese characters
  'latin1': u''.join(unichr(i) for i in range(0x20, 0x7F)),   # Basic Latin characters
  'shift_jis': u''.join(unichr(i) for i in hiragana_range     # Hiragana characters
                        if i not in problematic_symbols_hiragana),
  'cp1251': u''.join(unichr(i) for i in range(0x0410, 0x044F)),   # Cyrillic characters
  'koi8-r': u''.join(unichr(i) for i in cyrillic_range            # Cyrillic characters
                      if i not in problematic_symbols_koi8r)
}


class TestCharCodecGen(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCharCodecGen, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')

  def generate_random_word(self, charset, min_length=1, max_length=20):
    length = random.randint(min_length, max_length)
    return u''.join(random.choice(charset) for _ in range(length))

  def generate_text_file(self, encoding_name, charset, num_lines=10000, words_per_line=5):
    data_file_path = os.path.join(os.environ['IMPALA_HOME'], "testdata",
        "charcodec", "{}_gen_utf8.txt".format(encoding_name))
    with codecs.open(data_file_path, 'w', encoding='utf-8') as file:
        for _ in range(num_lines):
            words = [self.generate_random_word(charset) for _ in range(words_per_line)]
            line = u','.join(words)
            file.write(line + u'\n')
    return data_file_path

  def prepare_utf8_test_table(self, db, data_file_path, encoding_name):
    encoding_name_tbl = encoding_name.replace('-', '')
    tbl_name = "{}_gen_utf8".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"""
        .format(db, tbl_name))
    self.filesystem_client.copy_from_local(data_file_path, get_table_loc(db, tbl_name))
    self.execute_query("""REFRESH {}.{}""".format(db, tbl_name))
    return tbl_name

  def prepare_encoded_test_table(self, db, utf8_table, encoding_name):
    encoding_name_tbl = encoding_name.replace('-', '')
    encoded_table = "{}_gen".format(encoding_name_tbl)
    self.execute_query("""CREATE TABLE IF NOT EXISTS {}.{} (
        name1 STRING, name2 STRING, name3 STRING, name4 STRING, name5 STRING)
        STORED AS TEXTFILE""".format(db, encoded_table))
    self.execute_query("""ALTER TABLE {}.{}
                       SET SERDEPROPERTIES("serialization.encoding"="{}")"""
        .format(db, encoded_table, encoding_name))
    self.execute_query("""REFRESH {}.{}""".format(db, encoded_table))
    self.execute_query("""INSERT OVERWRITE TABLE {}.{} SELECT * FROM {}.{}"""
        .format(db, encoded_table, db, utf8_table))
    return encoded_table

  @SkipIfFS.hive
  def test_enc_dec_gen(self, vector, unique_database):
    db = unique_database
    for encoding_name, charset in charsets.items():
      data_file_path = self.generate_text_file(encoding_name, charset)
      utf8_table = self.prepare_utf8_test_table(db, data_file_path, encoding_name)
      encoded_table = self.prepare_encoded_test_table(db, utf8_table, encoding_name)

      # Compare count(*) of the encoded table with the utf8 table
      count_utf8 = self.client.execute("""select count(*) from {}.{}"""
          .format(db, utf8_table))
      count_encoded = self.client.execute("""select count(*) from {}.{}"""
          .format(db, encoded_table))
      assert int(count_utf8.get_data()) == int(count_encoded.get_data())

      # Compare * of the encoded table with the utf8 table
      result_utf8 = self.client.execute("""select * from {}.{}"""
          .format(db, utf8_table))
      result_encoded = self.client.execute("""select * from {}.{}"""
          .format(db, encoded_table))
      verify_query_result_is_equal(result_utf8.data, result_encoded.data)

      os.remove(data_file_path)
