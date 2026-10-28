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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10866: DROP TYPE on a TIMESERIES type whose engine failed to start (issue #6356) answered dropped:true and deleted none of
 * the engine's files - the shard buckets, the sealed files and the tag dictionary stayed on disk and registered, came back at every
 * open, and a type created later under the same name served the dropped type's samples.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10866DropEnginelessTimeSeriesTypeTest extends TestHelper {
  private static final int ROWS = 3_000;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void aTypeWhoseEngineDidNotStartIsDroppedWithEveryFileOfIt() throws Exception {
    createTypeWithSealedData();
    corruptSealedFileOfShard0();

    final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType("Cpu");
    assertThat(broken.isEngineAvailable()).as("the #6356 state: the type is registered without an engine").isFalse();

    database.command("sql", "DROP TYPE Cpu");
    assertThat(database.getSchema().existsType("Cpu")).isFalse();
    assertNothingOfTheTypeIsLeft();

    // nothing comes back at the next open either
    reopenDatabase();
    assertNothingOfTheTypeIsLeft();

    // the name is free: a type created under it starts empty, rather than failing on the damaged file or adopting the samples
    database.command("sql", "CREATE TIMESERIES TYPE Cpu TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 2");
    final LocalTimeSeriesType recreated = (LocalTimeSeriesType) database.getSchema().getType("Cpu");
    assertThat(recreated.isEngineAvailable()).isTrue();
    assertThat(countSamples()).isZero();
  }

  /** The control: a healthy type takes the same path through the engine, and the same guarantees hold. */
  @Test
  void aHealthyTypeIsDroppedWithEveryFileOfIt() throws Exception {
    createTypeWithSealedData();

    database.command("sql", "DROP TYPE Cpu");
    assertNothingOfTheTypeIsLeft();

    database.command("sql", "CREATE TIMESERIES TYPE Cpu TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 2");
    assertThat(countSamples()).isZero();
  }

  private void createTypeWithSealedData() throws IOException {
    database.command("sql", "CREATE TIMESERIES TYPE Cpu TIMESTAMP ts TAGS (hostname STRING) FIELDS (usage DOUBLE) SHARDS 2");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Cpu")).getEngine();
    final long[] timestamps = new long[ROWS];
    final Object[][] columns = new Object[2][ROWS];
    for (int i = 0; i < ROWS; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();
    assertThat(new File(getDatabasePath(), "Cpu_shard_0.ts.sealed")).exists();
    assertThat(new File(getDatabasePath(), "Cpu_shard_1.ts.sealed")).exists();
    assertThat(countSamples()).isEqualTo(ROWS);
  }

  /** Flips a byte of the magic of shard 0's sealed file, the recipe of Issue6340TimeSeriesCheckDatabaseTest, and reopens. */
  private void corruptSealedFileOfShard0() throws IOException {
    final File sealed = new File(getDatabasePath(), "Cpu_shard_0.ts.sealed");
    database.close();
    try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
      final int b = raf.read();
      raf.seek(0);
      raf.write(b ^ 0x01);
    } finally {
      database = factory.open();
    }
  }

  private void assertNothingOfTheTypeIsLeft() {
    final List<String> onDisk = new ArrayList<>();
    final File[] files = new File(getDatabasePath()).listFiles();
    if (files != null)
      for (final File file : files)
        if (file.getName().startsWith("Cpu_"))
          onDisk.add(file.getName());
    assertThat(onDisk).as("the files of the dropped type on disk").isEmpty();

    final List<String> registered = new ArrayList<>();
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null && file.getComponentName().startsWith("Cpu_"))
        registered.add(file.getFileName());
    assertThat(registered).as("the components of the dropped type in the file manager").isEmpty();

    for (final String component : List.of("Cpu_shard_0", "Cpu_shard_1", "Cpu" + TimeSeriesTagDictionary.NAME_SUFFIX))
      assertThat(((LocalSchema) database.getSchema()).getFileByName(component)).as(component).isNull();
  }

  private long countSamples() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Cpu")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
