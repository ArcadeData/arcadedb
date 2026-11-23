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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A row {@code vector.neighbors} hands out whose properties nothing asks for is made without reading its record (issue #10973), except where
 * reading does more than load the record: a read listener of the database or of the type, before or after the read, sees (and can veto) every
 * record the search hands out. The oracle is the listener itself: it has to see one read per row, in each of the four places it can be
 * registered, and the rows have to carry their record.
 */
class VectorNeighborsReadListenerOracleTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int ROWS       = 10;

  @Test
  void aReadListenerOfTheDatabaseOrOfTheTypeSeesEveryRowTheSearchHandsOut() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Doc.pid INTEGER");
    database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"EUCLIDEAN\" }");
    final Random random = new Random(42);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.newDocument("Doc").set("pid", i, "vector", vector(random)).save();
    });

    assertThat(neighbours()).as("no listener: the rows are made without reading their records").hasSize(ROWS)
        .allSatisfy(row -> assertThat(isUnread(row)).isTrue());

    final AtomicInteger reads = new AtomicInteger();
    final BeforeRecordReadListener before = record -> {
      reads.incrementAndGet();
      return true;
    };
    final AfterRecordReadListener after = record -> {
      reads.incrementAndGet();
      return record;
    };
    final RecordEvents[] registries = { database.getEvents(), database.getSchema().getType("Doc").getEvents() };
    for (final RecordEvents events : registries) {
      final String where = events == registries[0] ? "the database" : "the type";

      events.registerListener(before);
      reads.set(0);
      assertThat(neighbours()).as("a before-read listener of %s", where).hasSize(ROWS)
          .allSatisfy(row -> assertThat(isUnread(row)).isFalse());
      assertThat(reads.get()).as("reads the before-read listener of %s saw", where).isEqualTo(ROWS);
      events.unregisterListener(before);

      events.registerListener(after);
      reads.set(0);
      assertThat(neighbours()).as("an after-read listener of %s", where).hasSize(ROWS)
          .allSatisfy(row -> assertThat(isUnread(row)).isFalse());
      assertThat(reads.get()).as("reads the after-read listener of %s saw", where).isEqualTo(ROWS);
      events.unregisterListener(after);

      assertThat(neighbours()).as("the listener of %s is gone: the rows are made without reading", where)
          .allSatisfy(row -> assertThat(isUnread(row)).isTrue());
    }
  }

  private List<ScoredRecordRow> neighbours() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final Object result = new SQLFunctionVectorNeighbors().execute(null, null, null,
        new Object[] { "Doc[vector]", new float[DIMENSIONS], ROWS }, context);
    final List<ScoredRecordRow> rows = new ArrayList<>();
    for (final Object row : (List<?>) result)
      rows.add((ScoredRecordRow) row);
    return rows;
  }

  private static boolean isUnread(final ScoredRecordRow row) {
    return ((BaseRecord) row.getRecord()).getBuffer() == null;
  }

  private static float[] vector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = random.nextFloat();
    return vector;
  }
}
