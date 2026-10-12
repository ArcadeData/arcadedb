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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9811, engine side: {@link TypeIndex#get(Object[])} answers a key lookup over whatever sub-indexes the index has at
 * that instant, so a lookup during a concurrent CREATE, REBUILD or DROP INDEX came back short - empty, for a key that exists
 * - without any error. A protocol that answers a client from it (the Redis wrapper: "the key does not exist") had to remember
 * three separate guards. {@link TypeIndex#getForQuery} makes them in one place and raises the stale-index
 * {@link IndexException} instead of a short answer, and {@code Database.lookupByKey} reads through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9811GuardedKeyLookupTest extends TestHelper {
  private static final int RECORDS = 2_000;

  @Test
  void aLookupOnAnIndexWhoseSubIndexesAreGoneRaisesInsteadOfAnsweringEmpty() {
    final TypeIndex index = createIndex();
    assertThat(index.getForQuery(new Object[] { 7L }).hasNext()).isTrue();

    // the state a concurrent drop leaves for a moment: the wrapper still valid, its sub-indexes gone
    final IndexInternal[] snapshot = index.getIndexesOnBuckets();
    for (final IndexInternal subIndex : snapshot)
      index.removeIndexOnBucket(subIndex);
    try {
      // the index cannot answer, and says so rather than "no such key"
      assertThatThrownBy(() -> index.getForQuery(new Object[] { 7L })).isInstanceOf(IndexException.class);
    } finally {
      for (final IndexInternal subIndex : snapshot)
        index.addIndexOnBucket(subIndex);
    }
    assertThat(index.getForQuery(new Object[] { 7L }).hasNext()).isTrue();
  }

  /**
   * The invariant {@link TypeIndex#getForQuery} rests on: every arm of {@code get} has collected its entries by the time it
   * returns, so the change check after it covers the whole read. A lazy arm added later would make the check pass while the
   * cursor still reads, and must fail here instead.
   */
  @Test
  void everyArmOfGetReturnsACollectedCursor() {
    final DocumentType type = createType();
    type.createProperty("text", Type.STRING);
    database.transaction(() -> database.newDocument("Doc").set("k", 1L).set("text", "alpha beta").save());

    final TypeIndex notUnique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
    assertCollected(notUnique.get(new Object[] { 1L }));
    assertCollected(notUnique.get(new Object[] { 1L }, 1));
    database.getSchema().dropIndex(notUnique.getName());

    final TypeIndex unique = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "text");
    assertCollected(unique.get(new Object[] { "alpha beta" }));
    database.getSchema().dropIndex(unique.getName());

    final TypeIndex fullText = type.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "text");
    assertCollected(fullText.get(new Object[] { "alpha" }));
    database.getSchema().dropIndex(fullText.getName());

    database.command("sql", "CREATE INDEX ON Doc (text) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    assertCollected(((TypeIndex) database.getSchema().getIndexByName("Doc[text]")).get(new Object[] { "alpha" }));
  }

  private static void assertCollected(final IndexCursor cursor) {
    assertThat((Object) cursor).isInstanceOfAny(TempIndexCursor.class, IndexCursorCollection.class);
  }

  @Test
  void aLookupOnADroppedIndexIsAStaleRead() {
    final TypeIndex index = createIndex();
    database.getSchema().dropIndex(index.getName());

    final Throwable thrown = catchThrowable(() -> index.getForQuery(new Object[] { 7L }));
    assertThat(thrown).isInstanceOf(IndexException.class);
    assertThat(TypeIndex.isStaleRead(index, (RuntimeException) thrown)).isTrue();
  }

  /**
   * The issue's repro: one thread dropping and re-creating the index, readers looking up a key that exists. The raw read
   * answers "not found" now and then; the guarded reads either find the record or say they could not read the index.
   */
  @Test
  @Tag("slow")
  void underConcurrentDropAndCreateAGuardedLookupNeverAnswersAKeyThatExistsAsMissing() throws Exception {
    final DocumentType type = createType();
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicLong found = new AtomicLong();
    final AtomicLong silentlyEmpty = new AtomicLong();

    final Thread ddl = new Thread(() -> {
      try {
        while (!stop.get()) {
          database.command("sql", "DROP INDEX `Doc[k]` IF EXISTS");
          database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (k) UNIQUE");
          // a moment with the index whole, so the readers have something to find
          Thread.sleep(2);
        }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    });

    final List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      final long key = 100L + r * 300L;
      readers.add(new Thread(() -> {
        while (!stop.get()) {
          // through the schema, as a protocol resolving an index by name does
          final TypeIndex index;
          try {
            index = (TypeIndex) database.getSchema().getIndexByName("Doc[k]");
          } catch (final SchemaException e) {
            continue; // the index is not there at this instant: a visible answer
          }
          check(() -> index.getForQuery(new Object[] { key }), index, found, silentlyEmpty, failure);
          check(() -> ConsistentKeyLookup.getForQuery(database, index, new Object[] { key }), index, found, silentlyEmpty, failure);
          check(() -> database.lookupByKey("Doc", "k", key), index, found, silentlyEmpty, failure);
        }
      }));
    }

    ddl.start();
    readers.forEach(Thread::start);
    // at least 3 s of churn, and until the readers found the key a few times: a loaded runner may leave few windows open
    await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(60)).until(() -> found.get() >= 20 || failure.get() != null);
    stop.set(true);
    ddl.join(60_000);
    for (final Thread reader : readers)
      reader.join(60_000);

    assertThat(failure.get()).isNull();
    assertThat(found.get()).as("the readers did find the key between the DDLs").isPositive();
    assertThat(silentlyEmpty.get()).as("a key that exists answered as missing").isZero();
  }

  /**
   * {@code Database.lookupByKey} now raises instead of answering short, and the openCypher MERGE that seeks through it scans
   * the type then: a short answer made MERGE create a node that exists, an exception would fail a statement that can be
   * answered.
   */
  @Test
  @Tag("slow")
  void underConcurrentDropAndCreateMergeNeitherFailsNorDuplicates() throws Exception {
    database.getSchema().createVertexType("Node", 4).createProperty("k", Type.LONG);
    database.transaction(() -> {
      for (long i = 0; i < RECORDS; i++)
        database.newVertex("Node").set("k", i).save();
    });
    database.command("sql", "CREATE INDEX ON Node (k) NOTUNIQUE");

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicLong merges = new AtomicLong();

    final Thread ddl = new Thread(() -> {
      try {
        while (!stop.get()) {
          database.command("sql", "DROP INDEX `Node[k]` IF EXISTS");
          database.command("sql", "CREATE INDEX IF NOT EXISTS ON Node (k) NOTUNIQUE");
          Thread.sleep(2);
        }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    });
    final Thread merger = new Thread(() -> {
      try {
        while (!stop.get()) {
          database.transaction(() -> database.command("opencypher", "MERGE (n:Node {k: 777}) RETURN n").close());
          merges.incrementAndGet();
        }
      } catch (final Throwable t) {
        failure.compareAndSet(null, t);
      }
    });

    ddl.start();
    merger.start();
    await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(60)).until(() -> merges.get() >= 20 || failure.get() != null);
    stop.set(true);
    ddl.join(60_000);
    merger.join(60_000);

    assertThat(failure.get()).isNull();
    assertThat(merges.get()).isPositive();
    assertThat(database.query("sql", "SELECT count(@rid) AS c FROM Node WHERE k = 777").next().<Long>getProperty("c"))
        .as("MERGE matched the node that exists every time").isEqualTo(1L);
  }

  private interface Lookup {
    IndexCursor run();
  }

  private static void check(final Lookup lookup, final TypeIndex index, final AtomicLong found, final AtomicLong silentlyEmpty,
      final AtomicReference<Throwable> failure) {
    try (final IndexCursor cursor = lookup.run()) {
      if (cursor.hasNext())
        found.incrementAndGet();
      else
        silentlyEmpty.incrementAndGet();
    } catch (final IllegalArgumentException e) {
      // lookupByKey: "No index has been created", the index was not there or not ready
      if (!e.getMessage().startsWith("No index") && !TypeIndex.isStaleRead(index, e))
        failure.compareAndSet(null, e);
    } catch (final SchemaException e) {
      // the type's index is not registered at this instant: visible, the caller retries
    } catch (final RuntimeException e) {
      if (!TypeIndex.isStaleRead(index, e))
        failure.compareAndSet(null, e);
    }
  }

  private TypeIndex createIndex() {
    return createType().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
  }

  private DocumentType createType() {
    final DocumentType type = database.getSchema().createDocumentType("Doc", 4);
    type.createProperty("k", Type.LONG);
    database.transaction(() -> {
      for (long i = 0; i < RECORDS; i++)
        database.newDocument("Doc").set("k", i).save();
    });
    return type;
  }
}
