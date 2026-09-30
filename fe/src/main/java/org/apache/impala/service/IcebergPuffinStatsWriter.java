// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;

import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.ImpalaRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes NDV statistics as 'apache-datasketches-theta-v1' Puffin blobs for an Iceberg
 * table. Called from CatalogOpExecutor as part of COMPUTE STATS on Iceberg tables.
 *
 * Any previously committed statistics files are replaced by the new file, so this
 * implements a full replacement semantics. For partial COMPUTE STATS (specific columns),
 * only the columns included in thetaSketchBlobs are updated; blobs for other columns
 * are not preserved in Stage I.
 *
 * This matches the write pattern used by Trino's ANALYZE TABLE command.
 */
class IcebergPuffinStatsWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergPuffinStatsWriter.class);

  private static final String THETA_BLOB_TYPE = "apache-datasketches-theta-v1";

  /**
   * Writes compact theta sketch bytes as Puffin NDV statistics blobs for the given
   * Iceberg table. 'thetaSketchBlobs' maps column names to the raw bytes returned by
   * ds_theta_sketch(). 'snapshotId' is the snapshot for which stats were computed
   * (captured at query compile time).
   *
   * Any existing statistics files for the table are removed and replaced with a single
   * new file containing the provided blobs.
   */
  static void writePuffinNdvStats(IcebergTable impalaTable,
      Map<String, ByteBuffer> thetaSketchBlobs,
      long snapshotId) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(impalaTable);
    Preconditions.checkNotNull(thetaSketchBlobs);
    if (thetaSketchBlobs.isEmpty()) return;

    Table apiTable = impalaTable.getIcebergApiTable();
    Snapshot snapshot = apiTable.snapshot(snapshotId);
    if (snapshot == null) {
      throw new ImpalaRuntimeException(
          "Snapshot not found when writing Puffin NDV stats for table "
          + impalaTable.getFullName() + ": snapshotId=" + snapshotId);
    }
    long seqNum = snapshot.sequenceNumber();

    String puffinPath = newPuffinPath(apiTable);
    OutputFile outputFile = apiTable.io().newOutputFile(puffinPath);

    List<Blob> blobs = buildBlobs(apiTable, thetaSketchBlobs, snapshotId, seqNum,
        impalaTable.getFullName());
    if (blobs.isEmpty()) return;

    try (PuffinWriter writer = Puffin.write(outputFile).createdBy("Impala").build()) {
      for (Blob blob : blobs) writer.add(blob);
      writer.finish();

      // Build the StatisticsFile descriptor from writer metadata.
      List<org.apache.iceberg.BlobMetadata> blobMetadataList =
          GenericBlobMetadata.from(writer.writtenBlobsMetadata());
      StatisticsFile statsFile = new GenericStatisticsFile(
          snapshotId,
          writer.location(),
          writer.fileSize(),
          writer.footerSize(),
          blobMetadataList);

      // Replace all existing statistics files with the new one.
      UpdateStatistics updateStats = apiTable.updateStatistics();
      for (StatisticsFile existing : apiTable.statisticsFiles()) {
        updateStats.removeStatistics(existing.snapshotId());
      }
      updateStats.setStatistics(statsFile).commit();

      LOG.info("Wrote Puffin NDV stats for {} column(s) of table {} (snapshotId={})",
          blobs.size(), impalaTable.getFullName(), snapshotId);
    } catch (Exception e) {
      throw new ImpalaRuntimeException(
          "Failed to write Puffin NDV stats for table " + impalaTable.getFullName(), e);
    }
  }

  private static List<Blob> buildBlobs(Table apiTable,
      Map<String, ByteBuffer> thetaSketchBlobs,
      long snapshotId, long seqNum, String tableName) {
    List<Blob> blobs = new ArrayList<>();
    for (Map.Entry<String, ByteBuffer> e : thetaSketchBlobs.entrySet()) {
      String colName = e.getKey();
      ByteBuffer sketchBytes = e.getValue();
      if (sketchBytes == null || sketchBytes.remaining() == 0) continue;

      org.apache.iceberg.types.Types.NestedField field =
          apiTable.schema().findField(colName);
      if (field == null) {
        LOG.warn("Column {} not found in Iceberg schema when writing Puffin stats "
            + "for table {}", colName, tableName);
        continue;
      }
      int fieldId = field.fieldId();
      long ndv = estimateNdv(sketchBytes, colName);

      blobs.add(new Blob(
          THETA_BLOB_TYPE,
          Collections.singletonList(fieldId),
          snapshotId,
          seqNum,
          sketchBytes.duplicate(),
          null,   // no compression
          Collections.singletonMap("ndv", Long.toString(ndv))));
    }
    return blobs;
  }

  private static String newPuffinPath(Table apiTable) {
    String location = apiTable.location();
    if (location.endsWith("/")) location = location.substring(0, location.length() - 1);
    return location + "/metadata/" + UUID.randomUUID() + ".stats";
  }

  /**
   * Deserializes the compact theta sketch bytes and returns the NDV estimate.
   * Returns -1 on error.
   */
  private static long estimateNdv(ByteBuffer bytes, String colName) {
    try {
      byte[] arr;
      if (bytes.hasArray() && bytes.arrayOffset() == 0 && bytes.position() == 0) {
        arr = bytes.array();
      } else {
        arr = new byte[bytes.remaining()];
        bytes.duplicate().get(arr);
      }
      Sketch sketch = Sketches.heapifySketch(Memory.wrap(arr));
      return Math.max(0, (long) sketch.getEstimate());
    } catch (Exception e) {
      LOG.warn("Failed to estimate NDV from theta sketch for column {}: {}",
          colName, e.getMessage());
      return -1;
    }
  }
}
