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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10749: a rename of a TIMESERIES type that fails while the engine's files move puts every file back and the type back under
 * its old name, which it never gave up in between: a type created under the old name meanwhile would have found the files still
 * carrying it and taken them. The failure is a sealed file of the second shard that cannot be opened where it moved, so the first
 * shard, already moved, has to come back too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10749RenameRollbackTest extends TestHelper {
  private static final String OLD = "Old10749R";
  private static final String NEW = "New10749R";

  @AfterEach
  void disarm() {
    TimeSeriesSealedStore.TEST_CHANNEL_DECORATOR = null;
  }

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
}
