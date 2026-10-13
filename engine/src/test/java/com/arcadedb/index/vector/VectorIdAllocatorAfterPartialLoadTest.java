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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A first-use materialisation of the location map that stops PARTWAY must not leave the vector id allocator behind
 * the ids the unread pages already hold. Issue #6762 moved the allocator past what the load had READ, but the pages
 * it never reached carry higher ids, so the next inserts minted ids that live records already own: two records on
 * one vector id, the persisted finding CHECK DATABASE reports as "a vector id that a load gives to another record".
 * An OutOfMemoryError is the realistic trigger: the first use after a restart materialises under full load, the
 * error is not an Exception so the swallow-and-carry-on branch never sees it, and the finally still latched the
 * half-filled map as materialised for the life of the process. Now an Error leaves the materialisation open for a
 * retry, and a load the lenient parser cut short refuses new vectors until CHECK DATABASE FIX rebuilds the index
 * (issue #9892).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIdAllocatorAfterPartialLoadTest {
  private static final String DB_PATH    = "target/databases/VectorIdAllocatorAfterPartialLoadTest";
  private static final int    DIMENSIONS = 64;
  private static final int    VECTORS    = 400;
  private static final int    READ_LIMIT = 30;
  private static final int    INSERTS    = 25;

  @AfterEach
  void cleanUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aLoadInterruptedByAnErrorIsRetriedInsteadOfLatchingTheHalfFilledMap() throws Exception {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.areLocationsMaterializedForTest()).as("reopened: the location map is deferred").isFalse();

        // The heap runs out after READ_LIMIT entries: the error reaches whichever caller triggered the first use.
        setField(index, "residentLocations", new FailingLocationIndex(READ_LIMIT, true));
        assertThatThrownBy(index::getVectorIndex).isInstanceOf(OutOfMemoryError.class);
        assertThat(index.areLocationsMaterializedForTest())
            .as("an Error must not latch a half-filled map: the next access loads again")
            .isFalse();

        insertVectors(db);

        assertThat(index.countEntries()).as("the retried load read every entry").isEqualTo(VECTORS + INSERTS);
        assertThat(index.checkIntegrity()).as("no record may share a vector id with another after the inserts").isEmpty();
      } finally {
        db.close();
      }
    }

    assertCleanAfterReopen(VECTORS + INSERTS);
  }

  @Test
  void aLoadThatCouldNotReadEveryEntryRefusesNewVectorsUntilRebuilt() throws Exception {
    createDatabaseWithVectorIndex();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);

        // A page that cannot be parsed to the end: the lenient load carries on, but the ids it did not read are unknown.
        setField(index, "residentLocations", new FailingLocationIndex(READ_LIMIT, false));
        index.getVectorIndex();
        assertThat(index.areLocationsMaterializedForTest()).as("one attempt, as before").isTrue();

        assertThatThrownBy(() -> db.transaction(
            () -> db.newDocument("Doc").set("id", VECTORS).set("embedding", randomVector(VECTORS)).save()))
            .as("an insert must not mint an id an unread entry may already hold")
            .hasRootCauseInstanceOf(IndexException.class)
            .hasStackTraceContaining("refuses new vectors");

        try (final ResultSet rs = db.command("sql", "CHECK DATABASE")) {
          assertThat(rs.next().<Collection<String>>getProperty("corruptedIndexes"))
              .as("CHECK DATABASE names the index so FIX rebuilds it")
              .contains(index.getName());
        }

        try (final ResultSet rs = db.command("sql", "CHECK DATABASE FIX")) {
          rs.next();
        }

        insertVectors(db);

        final LSMVectorIndex rebuilt = vectorIndexOf(db);
        assertThat(rebuilt.countEntries()).isEqualTo(VECTORS + INSERTS);
        assertThat(rebuilt.checkIntegrity()).as("after the rebuild, inserts take fresh ids").isEmpty();
      } finally {
        db.close();
      }
    }

    assertCleanAfterReopen(VECTORS + INSERTS);
  }

  private void insertVectors(final Database db) {
    for (int i = 0; i < INSERTS; i++) {
      final int id = VECTORS + i;
      db.transaction(() -> db.newDocument("Doc").set("id", id).set("embedding", randomVector(id)).save());
    }
  }

  /** Once the process is gone, a full load must see every record exactly once. */
  private void assertCleanAfterReopen(final int expectedEntries) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndexOf(db);
        assertThat(index.checkIntegrity()).as("the pages written after the failed load hold no shared id").isEmpty();
        assertThat(index.countEntries()).isEqualTo(expectedEntries);
      } finally {
        db.close();
      }
    }
  }

  /** A location map that gives up after a number of entries, like a load that ran out of heap or hit a bad page. */
  private static class FailingLocationIndex extends VectorLocationIndex {
    private final int     limit;
    private final boolean outOfMemory;
    private       int     added;
    private       boolean armed = true;

    FailingLocationIndex(final int limit, final boolean outOfMemory) {
      this.limit = limit;
      this.outOfMemory = outOfMemory;
    }

    @Override
    public void addOrUpdate(final int id, final boolean isCompacted, final long absoluteFileOffset, final RID rid,
        final boolean deleted) {
      if (armed && added++ >= limit) {
        armed = false;
        if (outOfMemory)
          throw new OutOfMemoryError("simulated heap exhaustion during the location load");
        throw new IllegalStateException("simulated unreadable page during the location load");
      }
      super.addOrUpdate(id, isCompacted, absoluteFileOffset, rid, deleted);
    }
  }

  private LSMVectorIndex vectorIndexOf(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final long seed) {
    final Random rnd = new Random(seed);
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) rnd.nextGaussian();
    return v;
  }

  private void createDatabaseWithVectorIndex() {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("Doc", 1);
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        });
        db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\" }");
        db.transaction(() -> {
          for (int i = 0; i < VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("embedding", randomVector(i)).save();
        });
      } finally {
        db.close();
      }
    }
  }

  private static void setField(final Object target, final String name, final Object value) throws Exception {
    final Field f = LSMVectorIndex.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(target, value);
  }
}
