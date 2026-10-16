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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10184 (#10130, #10131, #10133): the BM25 corpus counters drifted in ways the drift check could not see.
 * <ul>
 *   <li>#10131: a document whose only indexed property is null was counted with length 0 by the put and uncounted with length 1
 *   by the remove, so every insert+delete of such a document took one token off {@code sumDocLength} for good;</li>
 *   <li>#10130: the drift check compared the polymorphic counters with the type's own record count, so a full-text index on a
 *   type with subtypes rescanned the whole type and warned on the first search of every session;</li>
 *   <li>#10133: after a crash the persisted pair is the one of the last schema save, and the drift check only compares the
 *   document count, so a recovered workload that left the count unchanged kept the old {@code sumDocLength} forever.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10184Bm25CorpusCountersDriftTest extends TestHelper {

  private FullTextIndexMetadata metadata(final String indexName) {
    return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(indexName));
  }

  private void search(final String type, final String indexName) {
    database.query("sql", "SELECT FROM " + type + " WHERE SEARCH_INDEX('" + indexName + "', 'alpha') = true").close();
  }

  @Test
  void insertingAndDeletingANullValuedDocumentLeavesTheCountersWhereTheyWere() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', tag = 't'");
    });
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(5L, 15L);

    for (int i = 0; i < 4; i++) {
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET tag = 'nulltest'"));
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE tag = 'nulltest'"));
    }

    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(5L, 15L);
  }

  @Test
  void updatingTheIndexedPropertyFromAndToNullKeepsTheCountersExact() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET content = 'alpha beta gamma', tag = 'a'"));
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET tag = 'b'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 3L);

    database.transaction(() -> database.command("sql", "UPDATE Doc SET content = 'alpha beta' WHERE tag = 'b'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 5L);

    database.transaction(() -> database.command("sql", "UPDATE Doc REMOVE content WHERE tag = 'a'"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(2L, 2L);

    database.transaction(() -> database.command("sql", "DELETE FROM Doc"));
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(0L, 0L);
  }

  @Test
  void aMultiPropertyIndexWithANullPropertyBalancesPutAndRemove() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE INDEX ON Doc (title, body) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET title = 'alpha beta', body = 'gamma'"));
    assertThat(metadata("Doc[title,body]").getCountersPair()).containsExactly(1L, 3L);

    for (int i = 0; i < 3; i++) {
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET title = 'delta'"));
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET body = 'epsilon zeta'"));
      database.transaction(() -> database.command("sql", "DELETE FROM Doc WHERE title = 'delta' OR body = 'epsilon zeta'"));
    }

    assertThat(metadata("Doc[title,body]").getCountersPair()).containsExactly(1L, 3L);
  }

  @Test
  void anIndexOnATypeWithSubtypesFindsNothingToRepairOnAFreshSession() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma'");
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Child SET content = 'alpha beta gamma'");
    });

    for (int session = 0; session < 2; session++) {
      reopenDatabase();
      search("Parent", "Parent[content]");

      assertThat(metadata("Parent[content]").isStaleCheckDone()).isTrue();
      // The counters were exact: the drift check found nothing to repair and scanned nothing
      assertThat(metadata("Parent[content]").getScans()).isZero();
      assertThat(metadata("Parent[content]").getCountersPair()).containsExactly(10L, 30L);
    }
  }

  @Test
  void aRealDriftOnATypeWithSubtypesIsStillRepaired() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma'");
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Child SET content = 'alpha beta gamma'");
    });
    metadata("Parent[content]").applyCorpusDelta(3, 20);
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();

    reopenDatabase();
    search("Parent", "Parent[content]");

    assertThat(metadata("Parent[content]").getScans()).isEqualTo(1L);
    assertThat(metadata("Parent[content]").getCountersPair()).containsExactly(10L, 30L);
  }

  /** The invariant behind all of the above: whatever the workload, the incremental counters are what a fresh rescan finds. */
  @Test
  void afterAMixedWorkloadTheCountersAreWhatARescanFinds() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Parent BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Parent.content STRING");
      database.command("sql", "CREATE PROPERTY Parent.tag STRING");
      database.command("sql", "CREATE INDEX ON Parent (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent BUCKETS 2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 6; i++) {
        database.command("sql", "INSERT INTO Parent SET content = 'alpha beta gamma', tag = 'p" + i + "'");
        database.command("sql", "INSERT INTO Child SET content = 'alpha delta', tag = 'c" + i + "'");
        database.command("sql", "INSERT INTO Parent SET tag = 'pn" + i + "'");
        database.command("sql", "INSERT INTO Child SET tag = 'cn" + i + "'");
      }
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE Parent SET content = 'one two three four' WHERE tag = 'pn1'");
      database.command("sql", "UPDATE Child SET content = 'five' WHERE tag = 'cn2'");
      database.command("sql", "UPDATE Parent REMOVE content WHERE tag = 'p3'");
      database.command("sql", "UPDATE Child REMOVE content WHERE tag = 'c4'");
      database.command("sql", "DELETE FROM Parent WHERE tag IN ['p0', 'pn0', 'c0', 'cn0', 'pn5', 'cn5']");
    });

    final FullTextIndexMetadata metadata = metadata("Parent[content]");
    final long[] incremental = metadata.getCountersPair();
    assertThat(incremental[0]).isEqualTo(database.countType("Parent", true));

    ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName("Parent[content]")).getIndexesOnBuckets()[0]).recomputeBM25Counters();

    assertThat(metadata.getCountersPair()).containsExactly(incremental[0], incremental[1]);
  }

  @Test
  void afterACrashTheFirstSearchRepairsALengthDriftTheDocumentCountCannotShow() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE PROPERTY Doc.tag STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'aa bb cc', tag = 'old'");
    });
    // A clean close persists {10, 30}
    reopenDatabase();
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 30L);

    // Same number of documents, longer ones: only sumDocLength moves
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO Doc SET content = 'dd ee ff gg hh ii jj kk ll mm', tag = 'new'");
      database.command("sql", "DELETE FROM Doc WHERE tag = 'old' LIMIT 5");
    });
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 65L);

    // Crash: the schema still holds the pair of the last save, the WAL holds the transaction
    ((DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    search("Doc", "Doc[content]");

    assertThat(database.countType("Doc", true)).isEqualTo(10L);
    assertThat(metadata("Doc[content]").getCountersPair()).containsExactly(10L, 65L);
    assertThat(metadata("Doc[content]").getScans()).isEqualTo(1L);
  }
}
