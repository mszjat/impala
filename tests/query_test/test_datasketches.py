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

from tests.common.file_utils import create_table_from_parquet
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_single_exec_option_dimension


class TestDatasketches(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestDatasketches, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_hll(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'hll_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'hll_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-hll', vector, unique_database)

  def test_cpc(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'cpc_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'cpc_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-cpc', vector, unique_database)

  def test_theta(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'theta_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'theta_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-theta', vector, unique_database)

  def test_kll(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'kll_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'kll_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-kll', vector, unique_database)


class TestDatasketchesOrcHiveInterop(ImpalaTestSuite):
  """IMPALA-9821: Tests that ds_hll_sketch() returns BINARY and that consuming
  functions accept BINARY, enabling interop with Hive-written ORC sketch tables."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestDatasketchesOrcHiveInterop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'orc')

  @SkipIfFS.hive
  def test_hll_sketch_orc_string_binary_mismatch(self, vector, unique_database):
    """IMPALA-9821: Hive writes sketch as BINARY to ORC/HMS; after the fix Impala must
    accept BINARY sketch columns and pass them to ds_hll_estimate()."""
    self.run_test_case(
        'QueryTest/datasketches-hll-hive-orc', vector, unique_database)
