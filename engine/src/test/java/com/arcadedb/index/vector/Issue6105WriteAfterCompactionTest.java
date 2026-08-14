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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6105: a write issued in the same session right after {@code COMPACT INDEX} on an {@code LSM_VECTOR} index
 * was silently not indexed.
 * <p>
 * An {@link LSMVectorIndex} is named after the component file it holds, and a compaction swaps that file in - so
 * the compaction reassigns {@code indexName} to the new component's name. {@code LocalSchema.indexMap} is keyed by
 * the name the index was registered under, and nothing re-keyed it, so after a compaction the index was reachable
 * in the schema only under a name it no longer answers to. Every index operation of a transaction is queued on
 * {@link com.arcadedb.database.TransactionIndexContext} under {@code index.getName()} and
 * {@code TransactionIndexContext.commit()} opens by dropping the lanes of indexes that no longer exist in the
 * schema (the TYPE DROP case) - which matched the freshly compacted vector index exactly, so its queued entries
 * were discarded without a word. The record itself was written, and a reopen (which rebuilds {@code indexMap} from
 * the persisted names) made writes land again, which is what made this look like a phantom.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue6105WriteAfterCompactionTest extends TestHelper {

  private static final int DIMENSIONS = 16;
  private static final int VERTICES   = 100;

  /**
   * The name the index answers to must be the name the schema knows it by, at every moment of its life - a
   * compaction included. This is the structural invariant the two behavioural tests below depend on.
   */
  @Test
  void aCompactedIndexIsStillReachableUnderTheNameItAnswersTo() {
    createSchema();
    insertVertices();

    final LSMVectorIndex index = vectorIndex();
    final String nameBefore = index.getName();
    assertThat(database.getSchema().existsIndex(nameBefore)).as("registered before the compaction").isTrue();

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    final String nameAfter = index.getName();
    assertThat(nameAfter).as("a compaction renames the index after the file it swapped in").isNotEqualTo(nameBefore);
    assertThat(database.getSchema().existsIndex(nameAfter))
        .as("the schema must know the compacted index under its current name").isTrue();
    assertThat(database.getSchema().getIndexByName(nameAfter))
        .as("and must resolve that name to this very index instance").isSameAs(index);
    assertThat(database.getSchema().existsIndex(nameBefore))
        .as("the pre-compaction name must not linger in the schema").isFalse();
  }

  /**
   * An INSERT in the same session right after a compaction must reach the vector index: the document is created
   * either way, so a silent miss shows up only as a vector count that never moved and a vector search that cannot
   * find the new document.
   */
  @Test
  void anInsertRightAfterACompactionIsIndexed() {
    createSchema();
    insertVertices();

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");
    assertThat(countLive()).as("compaction must not lose or gain live vectors").isEqualTo(VERTICES);

    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES,
        embedding(VERTICES, 0)));

    assertThat(countLive()).as("the insert right after the compaction must reach the vector index")
        .isEqualTo(VERTICES + 1);

    final RID rid = ridOf("doc" + VERTICES);
    assertThat(vectorIndex().getVectorIndex().getVectorIdsForRid(rid))
        .as("exactly one vector id for the newly inserted document").hasSize(1);

    assertSearchWorks(VERTICES, 0);
  }

  /**
   * Same for an UPDATE: the re-embedded document must be found by its NEW vector, not only by the one the
   * compaction persisted. A dropped index operation leaves the live count unchanged AND the old vector in place,
   * so the count alone is not enough - the search below is what proves the new embedding actually landed.
   */
  @Test
  void anUpdateRightAfterACompactionIsIndexed() {
    createSchema();
    insertVertices();

    database.command("sql", "COMPACT INDEX `Doc[embedding]`");

    final RID rid = ridOf("doc0");
    final int[] idsBefore = vectorIndex().getVectorIndex().getVectorIdsForRid(rid);
    assertThat(idsBefore).as("doc0 holds one vector before the update").hasSize(1);

    // Move doc0 far away from where it was, into a region no other document occupies.
    final float[] relocated = embedding(VERTICES + 7, 0);
    database.transaction(() -> database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", relocated, "doc0"));

    assertThat(countLive()).as("an update replaces a vector, it does not add one").isEqualTo(VERTICES);

    final int[] idsAfter = vectorIndex().getVectorIndex().getVectorIdsForRid(rid);
    assertThat(idsAfter).as("doc0 still holds exactly one vector after the update").hasSize(1);
    assertThat(idsAfter[0]).as("the update must have minted a new vector id for doc0").isNotEqualTo(idsBefore[0]);

    assertThat(nearestId(relocated)).as("doc0 must be found at its new embedding").isEqualTo("doc0");
  }

  /**
   * The same loss, reached the way production reaches it: a compaction is scheduled automatically after a commit
   * ({@code LSMVectorIndex.onAfterCommit}) and runs on the async executor, so it can rename the index between an
   * entry being queued on an open transaction and that transaction committing. Nothing in the writer's own session
   * says a compaction happened - which is why the index reference the transaction captured, not the name it was
   * queued under, has to be what resolves the entry at commit time.
   */
  @Test
  void aWriteQueuedBeforeACompactionOnAnotherThreadIsStillIndexed() throws Exception {
    createSchema();
    insertVertices();

    final String nameWhenQueued = vectorIndex().getName();

    database.begin();
    database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES, embedding(VERTICES, 0));

    // The compaction refuses to run inside a transaction, so it has to come from another thread - exactly where the
    // async executor runs it.
    final Throwable[] failure = new Throwable[1];
    final Thread compactor = new Thread(() -> {
      try {
        database.command("sql", "COMPACT INDEX `Doc[embedding]`");
      } catch (final Throwable t) {
        failure[0] = t;
      }
    }, "issue6105-compactor");
    compactor.start();
    compactor.join(120_000);

    assertThat(compactor.isAlive()).as("the compaction must have finished").isFalse();
    assertThat(failure[0]).as("the compaction must not have failed").isNull();
    assertThat(vectorIndex().getName()).as("the compaction must have renamed the index").isNotEqualTo(nameWhenQueued);

    database.commit();

    assertThat(countLive()).as("a write queued before the compaction must still reach the vector index")
        .isEqualTo(VERTICES + 1);
    assertSearchWorks(VERTICES, 0);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

      // Automatic compaction and background rebuilds are disabled: only the explicit COMPACT INDEX below must
      // rewrite the data file, and the assertions must observe the write path, not a rebuild that papered over it.
      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR
          METADATA {
            "dimensions": %d,
            "similarity": "COSINE",
            "quantization": "INT8",
            "mutationsBeforeRebuild": 100000000,
            "inactivityRebuildTimeoutMs": 0
          }""".formatted(DIMENSIONS));
    });
  }

  private void insertVertices() {
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
    });
  }

  /** Well-separated vectors: the nearest neighbor of vertex i must be vertex i itself. */
  private static float[] embedding(final int vertex, final int cycle) {
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private long countLive() {
    return vectorIndex().countEntries();
  }

  private RID ridOf(final String id) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = database.query("sql", "SELECT FROM Doc WHERE id = ?", id).next()
        .getIdentity().get());
    return holder[0];
  }

  private String nearestId(final float[] target) {
    final String[] holder = new String[1];
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1", (Object) target);
      assertThat(rs.hasNext()).isTrue();
      final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors).as("neighbors must not be empty").isNotEmpty();
      holder[0] = (String) neighbors.getFirst().get("id");
    });
    return holder[0];
  }

  private void assertSearchWorks(final int vertex, final int cycle) {
    assertThat(nearestId(embedding(vertex, cycle))).as("closest vector to doc%d", vertex).isEqualTo("doc" + vertex);
  }
}
