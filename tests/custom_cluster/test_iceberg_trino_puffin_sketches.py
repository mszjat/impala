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

# Impala <-> Trino interop over Iceberg Puffin theta-sketch statistics, at two levels
# (one class each):
#   - TestIcebergTrinoPuffinSketches compares the theta sketches themselves: both engines
#     build 'apache-datasketches-theta-v1' Puffin blobs over identical per-column data and
#     we compare the decoded 64-bit hash entries per column. Equal entry sets prove both
#     engines hash values identically (Iceberg Appendix D single-value serialization),
#     which is what makes cross-engine sketch UNION correct.
#   - TestIcebergTrinoPuffinNdv checks the weaker property that each engine can read the
#     NDV value the other derived from its Puffin blobs (Impala COMPUTE STATS <-> Trino
#     ANALYZE); it is driven by a .test file and needs none of the parsing helpers below.
#
# Trino runs in the 'impala-minicluster-trino' Docker container (see
# testdata/bin/TRINO-README.md) against the same HMS + HDFS as the Impala minicluster.
# The sketch-bytes test is skipped on non-HDFS filesystems and when the 'zstd' CLI
# (needed to decompress Trino's blobs) is unavailable.

from __future__ import absolute_import, division, print_function

import json
import os
import shutil
import subprocess

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf
from tests.common.trino_cluster import TrinoCluster

PUFFIN_MAGIC = b'PFA1'


def _zstd_binary():
  """Locate a 'zstd' CLI. Prefer the one bundled in the Impala toolchain (always
  present in CI) over a system package, which bootstrap_system.sh does not install."""
  packages_home = os.environ.get('IMPALA_TOOLCHAIN_PACKAGES_HOME')
  version = os.environ.get('IMPALA_ZSTD_VERSION')
  if packages_home and version:
    toolchain_zstd = os.path.join(
        packages_home, 'zstd-{0}'.format(version), 'bin', 'zstd')
    if os.path.isfile(toolchain_zstd):
      return toolchain_zstd
  return shutil.which('zstd')


def _hadoop_cat(hdfs_path):
  """Read an HDFS file's raw bytes via the hadoop CLI."""
  return subprocess.check_output(['hadoop', 'fs', '-cat', hdfs_path])


def _zstd_decompress(payload):
  """Decompress a zstd frame using the 'zstd' CLI (Trino compresses Puffin blobs
  with zstd; Impala writes them uncompressed)."""
  proc = subprocess.run([_zstd_binary(), '-d', '-c'], input=payload,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  if proc.returncode != 0:
    raise RuntimeError("zstd decompression failed: {0}".format(proc.stderr))
  return proc.stdout


def _read_puffin_theta_blobs(hdfs_path):
  """Parse an Iceberg Puffin (.stats) file and return {field_id: sketch_bytes} for
  every theta-sketch blob, decompressing zstd-compressed blobs. The Puffin footer
  layout is: trailing 4-byte magic, 4-byte flags, 4-byte footer-payload size, then
  the JSON footer payload (see the Iceberg Puffin spec)."""
  data = _hadoop_cat(hdfs_path)
  assert data[:4] == PUFFIN_MAGIC and data[-4:] == PUFFIN_MAGIC, \
      "Not a Puffin file: {0}".format(hdfs_path)
  footer_size = int.from_bytes(data[-12:-8], 'little')
  footer = json.loads(data[-12 - footer_size:-12].decode('utf-8'))
  blobs = {}
  for blob in footer['blobs']:
    payload = data[blob['offset']:blob['offset'] + blob['length']]
    if blob.get('compression-codec') == 'zstd':
      payload = _zstd_decompress(payload)
    blobs[blob['fields'][0]] = payload
  return blobs


def _theta_entries(sketch):
  """Return the sorted retained 64-bit hash entries of a serialized compact theta
  sketch (Apache DataSketches SerVer 3). Comparing entry sets is
  order/preamble-independent, so it isolates whether two engines hashed the same
  logical values to the same 64-bit hashes (i.e. agree on Iceberg Appendix D
  single-value serialization)."""
  pre_longs = sketch[0] & 0x3F
  if pre_longs == 1:
    cur_count = 0 if len(sketch) <= 8 else (len(sketch) - 8) // 8
    start = 8
  else:
    cur_count = int.from_bytes(sketch[8:12], 'little')
    start = pre_longs * 8
  entries = []
  for i in range(cur_count):
    off = start + i * 8
    entries.append(int.from_bytes(sketch[off:off + 8], 'little'))
  return sorted(entries)


# run_trino=True starts the Trino container before the Impala cluster (leaving an
# already-running container untouched) and stops it on teardown only if this class
# started it. Any failure to start Trino fails the tests rather than skipping silently.
@SkipIf.not_hdfs
@CustomClusterTestSuite.with_args(cluster_size=3, run_trino=True)
class TestIcebergTrinoPuffinSketches(CustomClusterTestSuite):
  """Compares the theta-sketch bytes Impala and Trino write for identical data by
  decoding and comparing the per-column 64-bit hash entries."""

  # Columns exercised by the test, in Iceberg field-id order (id, impala_name, expected
  # cross-engine sketch equality). COMPUTE STATS uses ds_theta_sketch_iceberg, which
  # encodes each value with Iceberg Appendix D single-value serialization, matching
  # Trino, so all types align.
  _SKETCH_COLS = [
      (1, 'i', True),     # INT
      (2, 'bi', True),    # BIGINT
      (3, 'fl', True),    # FLOAT
      (4, 'db', True),    # DOUBLE
      (5, 's', True),     # STRING
      (6, 'dt', True),    # DATE
      (7, 'ts', True),    # TIMESTAMP
      (8, 'dc', True),    # DECIMAL
      (9, 'bl', True),    # BOOLEAN
  ]

  def _current_statistics_file(self, fq_table):
    """Return the HDFS path of the Puffin statistics file attached to fq_table's
    current snapshot, read from the table's latest metadata.json."""
    describe = self.client.execute("describe extended %s" % fq_table)
    locations = [r for r in describe.data if r.startswith('\tmetadata_location')]
    assert len(locations) == 1, "metadata_location not found for %s" % fq_table
    metadata_path = locations[0].split('\t')[2]
    metadata = json.loads(_hadoop_cat(metadata_path))
    current_snapshot = metadata.get('current-snapshot-id')
    stats = metadata.get('statistics', [])
    assert stats, "No statistics files recorded for %s" % fq_table
    for entry in stats:
      if entry.get('snapshot-id') == current_snapshot:
        return entry['statistics-path']
    assert False, "No statistics file for current snapshot of %s" % fq_table

  @pytest.mark.execute_serially
  def test_puffin_sketch_bytes(self, unique_database):
    """Both engines build theta-sketch Puffin blobs over identical per-column data; we
    compare the decoded 64-bit hash entries per column. Equal entry sets prove both
    engines hash values identically (Iceberg Appendix D single-value serialization),
    which is stronger than agreeing on the derived NDV.

    _SKETCH_COLS pins the expected per-type alignment so any regression is caught."""
    if _zstd_binary() is None:
      pytest.skip("'zstd' CLI is required to decompress Trino's Puffin blobs")

    it_tbl = "{0}.it_sketch".format(unique_database)
    tt_tbl = "{0}.tt_sketch".format(unique_database)

    # Impala writes theta sketches for 5 distinct values per column.
    self.execute_query("""
        CREATE TABLE {tbl} (
            i INT, bi BIGINT, fl FLOAT, db DOUBLE, s STRING,
            dt DATE, ts TIMESTAMP, dc DECIMAL(10, 2), bl BOOLEAN)
        STORED BY ICEBERG""".format(tbl=it_tbl))
    self.execute_query("""
        INSERT INTO {tbl} VALUES
          (1, 100, CAST(1.5 AS FLOAT), 1.5, 'a',
           DATE '2020-01-01', '2020-01-01 00:00:00', CAST(1.00 AS DECIMAL(10,2)), true),
          (2, 200, CAST(2.5 AS FLOAT), 2.5, 'b',
           DATE '2020-01-02', '2020-01-02 00:00:00', CAST(2.00 AS DECIMAL(10,2)), false),
          (3, 300, CAST(3.5 AS FLOAT), 3.5, 'c',
           DATE '2020-01-03', '2020-01-03 00:00:00', CAST(3.00 AS DECIMAL(10,2)), true),
          (4, 400, CAST(4.5 AS FLOAT), 4.5, 'd',
           DATE '2020-01-04', '2020-01-04 00:00:00', CAST(4.00 AS DECIMAL(10,2)), false),
          (5, 500, CAST(5.5 AS FLOAT), 5.5, 'e',
           DATE '2020-01-05', '2020-01-05 00:00:00', CAST(5.00 AS DECIMAL(10,2)), true)
    """.format(tbl=it_tbl))
    self.execute_query("COMPUTE STATS {0}".format(it_tbl))

    # Trino writes theta sketches over the same logical values via ANALYZE.
    trino = TrinoCluster()
    trino_sql = """
        CREATE TABLE tt_sketch (
            i integer, bi bigint, fl real, db double, s varchar,
            dt date, ts timestamp(6), dc decimal(10, 2), bl boolean)
        WITH (format_version=2);
        INSERT INTO tt_sketch VALUES
          (1, BIGINT '100', REAL '1.5', DOUBLE '1.5', 'a', DATE '2020-01-01',
           TIMESTAMP '2020-01-01 00:00:00.000000', DECIMAL '1.00', true),
          (2, BIGINT '200', REAL '2.5', DOUBLE '2.5', 'b', DATE '2020-01-02',
           TIMESTAMP '2020-01-02 00:00:00.000000', DECIMAL '2.00', false),
          (3, BIGINT '300', REAL '3.5', DOUBLE '3.5', 'c', DATE '2020-01-03',
           TIMESTAMP '2020-01-03 00:00:00.000000', DECIMAL '3.00', true),
          (4, BIGINT '400', REAL '4.5', DOUBLE '4.5', 'd', DATE '2020-01-04',
           TIMESTAMP '2020-01-04 00:00:00.000000', DECIMAL '4.00', false),
          (5, BIGINT '500', REAL '5.5', DOUBLE '5.5', 'e', DATE '2020-01-05',
           TIMESTAMP '2020-01-05 00:00:00.000000', DECIMAL '5.00', true);
        ANALYZE tt_sketch"""
    _, stderr, rc = trino.run_query(trino_sql, schema=unique_database)
    assert rc == 0, "Trino setup failed:\n{0}".format(stderr)

    # Let Impala see the Trino-created table so its metadata.json is reachable.
    self.execute_query("INVALIDATE METADATA {0}".format(tt_tbl))

    impala_blobs = _read_puffin_theta_blobs(self._current_statistics_file(it_tbl))
    trino_blobs = _read_puffin_theta_blobs(self._current_statistics_file(tt_tbl))

    expected_ids = set(fid for fid, _, _ in self._SKETCH_COLS)
    assert set(impala_blobs) == expected_ids, \
        "Impala wrote blobs for {0}, expected {1}".format(
            sorted(impala_blobs), sorted(expected_ids))
    assert set(trino_blobs) == expected_ids, \
        "Trino wrote blobs for {0}, expected {1}".format(
            sorted(trino_blobs), sorted(expected_ids))

    mismatches = []
    for fid, name, should_match in self._SKETCH_COLS:
      equal = _theta_entries(impala_blobs[fid]) == _theta_entries(trino_blobs[fid])
      if equal != should_match:
        mismatches.append("{0} (field {1}): entries_equal={2}, expected={3}".format(
            name, fid, equal, should_match))
    assert not mismatches, (
        "Cross-engine theta-sketch alignment changed:\n  " + "\n  ".join(mismatches))


# Kept in a separate class from TestIcebergTrinoPuffinSketches: this .test-driven check
# needs none of the Puffin-parsing helpers above.
@SkipIf.not_hdfs
@CustomClusterTestSuite.with_args(cluster_size=3, run_trino=True)
class TestIcebergTrinoPuffinNdv(CustomClusterTestSuite):
  """Impala <-> Trino interop for the derived NDV value stored in Puffin blobs."""

  @pytest.mark.execute_serially
  def test_puffin_ndv_stats(self, vector, unique_database):
    """Column NDV statistics written as theta-sketch Puffin blobs by one engine are
    read by the other: Impala COMPUTE STATS -> Trino SHOW STATS, and Trino ANALYZE ->
    Impala SHOW COLUMN STATS."""
    self.run_test_case('QueryTest/iceberg-trino-interop-puffin-stats', vector,
                       use_db=unique_database)
