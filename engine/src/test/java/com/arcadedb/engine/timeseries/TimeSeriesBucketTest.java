/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesBucketTest extends TestHelper {

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  private TimeSeriesBucket createAndRegisterBucket(final String name, final List<ColumnDefinition> cols) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TimeSeriesBucket bucket = new TimeSeriesBucket(db, name, db.getDatabasePath() + "/" + name, cols);
    ((LocalSchema) db.getSchema()).registerFile(bucket);
    bucket.initHeaderPage();
    return bucket;
  }

  @Test
  void testCreateBucketAndAppend() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_bucket", createTestColumns());

    bucket.appendSamples(
        new long[] { 1000L },
        new Object[] { "sensor_A" },
        new Object[] { 22.5 }
    );
    database.commit();

    database.begin();
    assertThat(bucket.getSampleCount()).isEqualTo(1);
    assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(1000L);
    database.commit();
  }

  @Test
  void testAppendMultipleSamples() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_multi", createTestColumns());

    final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
    final Object[] sensorIds = { "A", "B", "A", "C", "B" };
    final Object[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

    bucket.appendSamples(timestamps, sensorIds, temperatures);
    database.commit();

    database.begin();
    assertThat(bucket.getSampleCount()).isEqualTo(5);
    assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(5000L);
    database.commit();
  }

  @Test
  void testScanRange() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_scan", createTestColumns());

    final long[] timestamps = { 1000L, 2000L, 3000L, 4000L, 5000L };
    final Object[] sensorIds = { "A", "B", "A", "C", "B" };
    final Object[] temperatures = { 20.0, 21.5, 22.0, 19.5, 23.0 };

    bucket.appendSamples(timestamps, sensorIds, temperatures);
    database.commit();

    database.begin();
    final List<Object[]> results = bucket.scanRange(2000L, 4000L, null);
    assertThat(results).hasSize(3);

    assertThat((long) results.get(0)[0]).isEqualTo(2000L);
    assertThat((String) results.get(0)[1]).isEqualTo("B");
    assertThat((double) results.get(0)[2]).isEqualTo(21.5);

    assertThat((long) results.get(2)[0]).isEqualTo(4000L);
    assertThat((String) results.get(2)[1]).isEqualTo("C");
    assertThat((double) results.get(2)[2]).isEqualTo(19.5);
    database.commit();
  }

  @Test
  void testScanRangeEmpty() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_empty", createTestColumns());

    bucket.appendSamples(
        new long[] { 1000L, 2000L },
        new Object[] { "A", "B" },
        new Object[] { 20.0, 21.0 }
    );
    database.commit();

    database.begin();
    final List<Object[]> results = bucket.scanRange(5000L, 6000L, null);
    assertThat(results).isEmpty();
    database.commit();
  }

  @Test
  void testNumericOnlyColumns() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD),
        new ColumnDefinition("count", Type.INTEGER, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_numeric", cols);

    bucket.appendSamples(
        new long[] { 100L, 200L, 300L },
        new Object[] { 1.5, 2.5, 3.5 },
        new Object[] { 10, 20, 30 }
    );
    database.commit();

    database.begin();
    final List<Object[]> results = bucket.scanRange(100L, 300L, null);
    assertThat(results).hasSize(3);
    assertThat((double) results.get(0)[1]).isEqualTo(1.5);
    assertThat((int) results.get(0)[2]).isEqualTo(10);
    assertThat((double) results.get(2)[1]).isEqualTo(3.5);
    assertThat((int) results.get(2)[2]).isEqualTo(30);
    database.commit();
  }

  @Test
  void testCompactionFlag() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_compact", createTestColumns());

    assertThat(bucket.isCompactionInProgress()).isFalse();
    bucket.setCompactionInProgress(true);
    assertThat(bucket.isCompactionInProgress()).isTrue();
    bucket.setCompactionInProgress(false);
    assertThat(bucket.isCompactionInProgress()).isFalse();
    database.commit();
  }

  @Test
  void testReadAllForCompaction() throws Exception {
    database.begin();
    final TimeSeriesBucket bucket = createAndRegisterBucket("test_ts_readall", createTestColumns());

    bucket.appendSamples(
        new long[] { 3000L, 1000L, 2000L },
        new Object[] { "C", "A", "B" },
        new Object[] { 30.0, 10.0, 20.0 }
    );
    database.commit();

    database.begin();
    final Object[] allData = bucket.readAllForCompaction();
    assertThat(allData).isNotNull();
    assertThat(allData).hasSize(3); // 3 columns

    final long[] ts = (long[]) allData[0];
    assertThat(ts).hasSize(3);
    assertThat(ts[0]).isEqualTo(3000L);
    assertThat(ts[1]).isEqualTo(1000L);
    assertThat(ts[2]).isEqualTo(2000L);
    database.commit();
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
