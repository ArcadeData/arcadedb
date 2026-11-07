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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for DROP TIMESERIES TYPE SQL statement and data cleanup on drop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DropTimeSeriesTypeTest extends TestHelper {

  @Test
  void dropTimeSeriesTypeSyntax() {
    // Issue #3863: DROP TIMESERIES TYPE should be accepted as valid syntax
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    assertThat(database.getSchema().existsType("Ticker")).isTrue();

    // This should NOT throw a syntax error
    final ResultSet rs = database.command("sql", "DROP TIMESERIES TYPE Ticker");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((String) row.getProperty("operation")).isEqualTo("drop type");
    assertThat((String) row.getProperty("typeName")).isEqualTo("Ticker");

    assertThat(database.getSchema().existsType("Ticker")).isFalse();
  }

  @Test
  void dropTimeSeriesTypeIfExists() {
    // DROP TIMESERIES TYPE ... IF EXISTS should not throw for nonexistent types, and (issue #7871) must report a
    // dropped:false row rather than an empty result set, the same contract every other guarded DROP statement uses.
    final ResultSet rs = database.command("sql", "DROP TIMESERIES TYPE NonExistent IF EXISTS");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((String) row.getProperty("operation")).isEqualTo("drop type");
    assertThat((Boolean) row.getProperty("dropped")).isFalse();
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void dropTimeSeriesTypeWithPlainDropType() {
    // Plain DROP TYPE should still work for timeseries types
    database.command("sql",
        "CREATE TIMESERIES TYPE PlainDrop TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    assertThat(database.getSchema().existsType("PlainDrop")).isTrue();

    database.command("sql", "DROP TYPE PlainDrop");

    assertThat(database.getSchema().existsType("PlainDrop")).isFalse();
  }

  @Test
  void dropAndRecreateTimeSeriesTypeDataCleanup() {
    // Issue #3863: after dropping and recreating, old data must NOT reappear
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Ticker SET ts = 1000, name = 'AAPL', f1 = 150.0");
      database.command("sql", "INSERT INTO Ticker SET ts = 2000, name = 'GOOG', f1 = 2800.0");
      database.command("sql", "INSERT INTO Ticker SET ts = 3000, name = 'MSFT', f1 = 300.0");
    });

    // Verify data was inserted
    ResultSet rs = database.query("sql", "SELECT FROM Ticker");
    List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(3);

    // Drop the type
    database.command("sql", "DROP TYPE Ticker");
    assertThat(database.getSchema().existsType("Ticker")).isFalse();

    // Recreate the same type
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    // Verify the count is 0 - no old data should reappear
    rs = database.query("sql", "SELECT FROM Ticker");
    results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).isEmpty();
  }

  // #10749
  // Issue #10749: a rename of a TIMESERIES type that fails while the engine's files move puts every file back and the type back under
  // its old name, which it never gave up in between: a type created under the old name meanwhile would have found the files still
  // carrying it and taken them. The failure is a sealed file of the second shard that cannot be opened where it moved, so the first
  // shard, already moved, has to come back too.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final String OLD = "Old10749R";
  private static final String NEW = "New10749R";

  @AfterEach
  void disarm() {
    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = null;
  }

  // #10749
  @Test
  void aShardThatCannotMovePutsEveryFileAndTheNameBack() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    for (int i = 0; i < 8; i++)
      database.command("sql", "INSERT INTO " + OLD + " SET ts = ?, host = ?, v = ?", 1000L * (i + 1), "h" + (i % 2), (double) i);
    ((LocalTimeSeriesType) database.getSchema().getType(OLD)).getEngine().compactAll();

    // the sealed file of shard 0 is reopened under the new name (the 1st channel the rename opens), the one of shard 1 is not (2nd)
    final AtomicInteger opened = new AtomicInteger();
    final AtomicReference<String> nameHeldMeanwhile = new AtomicReference<>();
    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = channel -> {
      if (opened.incrementAndGet() == 2) {
        nameHeldMeanwhile.set(database.getSchema().existsType(OLD) ? database.getSchema().getType(OLD).getName() : null);
        throw new UncheckedIOException(new IOException("cannot open the sealed file (issue 10749 test)"));
      }
      return channel;
    };

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW)).isInstanceOf(SchemaException.class)
        .hasMessageContaining(OLD);
    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = null;

    assertThat(nameHeldMeanwhile.get()).as("the old name still resolved to the type while its files moved").isEqualTo(NEW);
    assertThat(database.getSchema().existsType(OLD)).isTrue();
    assertThat(database.getSchema().getType(OLD).getName()).isEqualTo(OLD);
    assertThat(database.getSchema().existsType(NEW)).isFalse();

    final File directory = new File(database.getDatabasePath());
    assertThat(directory.list()).noneMatch(n -> n.startsWith(NEW + "_"))
        .contains(OLD + "_shard_0.ts.sealed", OLD + "_shard_1.ts.sealed");
    assertThat(count(OLD)).isEqualTo(8);

    reopenDatabase();
    assertThat(count(OLD)).isEqualTo(8);

    // and the rename goes through once nothing stands in its way
    database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);
    reopenDatabase();
    assertThat(count(NEW)).isEqualTo(8);
  }

  private long count(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + typeName)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  // #10866
  // Issue #10866: DROP TYPE on a TIMESERIES type whose engine failed to start (issue #6356) answered dropped:true and deleted none of
  // the engine's files - the shard buckets, the sealed files and the tag dictionary stayed on disk and registered, came back at every
  // open, and a type created later under the same name served the dropped type's samples.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final int ENGINELESS_ROWS = 3_000;

  // #10866: the first engineless-drop test deliberately leaves a type with a damaged sealed file behind
  private boolean skipIntegrityCheck;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return !skipIntegrityCheck;
  }

  // #10866
  @Test
  void aTypeWhoseEngineDidNotStartIsDroppedWithEveryFileOfIt() throws Exception {
    skipIntegrityCheck = true;
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

  // #10866
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
    final long[] timestamps = new long[ENGINELESS_ROWS];
    final Object[][] columns = new Object[2][ENGINELESS_ROWS];
    for (int i = 0; i < ENGINELESS_ROWS; i++) {
      timestamps[i] = 1_700_000_000_000L + i * 1_000L;
      columns[0][i] = "host_" + (i % 7);
      columns[1][i] = (double) i;
    }
    engine.appendBatch(timestamps, columns);
    engine.compactAll();
    assertThat(new File(getDatabasePath(), "Cpu_shard_0.ts.sealed")).exists();
    assertThat(new File(getDatabasePath(), "Cpu_shard_1.ts.sealed")).exists();
    assertThat(countSamples()).isEqualTo(ENGINELESS_ROWS);
  }

  /** Flips a byte of the magic of shard 0's sealed file, the recipe of TimeSeriesCheckDatabaseTest, and reopens. */
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
