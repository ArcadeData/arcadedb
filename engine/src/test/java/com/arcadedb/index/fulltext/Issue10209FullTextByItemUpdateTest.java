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
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10209: a FULL_TEXT index on a list property ({@code tags BY ITEM}) lost a record on update when the old list held the
 * same word twice in different case. The update diffed the list ITEMS, while the index stores the ANALYZED tokens: removing the
 * item {@code BETA} removed the posting {@code beta} the surviving item {@code beta} still needed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10209FullTextByItemUpdateTest extends TestHelper {

  private void createType(final String indexMetadata) {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("L");
      t.createProperty("id", Type.INTEGER);
      t.createProperty("tags", Type.LIST);
    });
    database.command("sql", "CREATE INDEX ON L (tags BY ITEM) FULL_TEXT" + indexMetadata);
  }

  private void insertTheThreeLists() {
    database.transaction(() -> {
      database.newDocument("L").set("id", 1).set("tags", List.of("beta", "BETA")).save();
      database.newDocument("L").set("id", 2).set("tags", List.of("beta", "beta")).save();
      database.newDocument("L").set("id", 3).set("tags", List.of("beta", "Beta", "beta")).save();
    });
  }

  private void setEveryListTo(final List<String> tags) {
    database.transaction(() -> {
      for (int id = 1; id <= 3; id++)
        try (final ResultSet rs = database.query("sql", "SELECT FROM L WHERE id = ?", id)) {
          rs.next().toElement().modify().set("tags", tags).save();
        }
    });
  }

  private List<Integer> found(final String word) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM L WHERE SEARCH_FIELDS(['tags'], ?) = true ORDER BY id", word)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }

  private TypeIndex index() {
    return (TypeIndex) database.getSchema().getType("L").getAllIndexes(false).iterator().next();
  }

  private FullTextIndexMetadata metadata() {
    return LSMTreeFullTextIndex.metadataOf(index());
  }

  @Test
  void anUpdateKeepsARecordWhoseOldListHeldTheWordInTwoCases() {
    createType("");
    insertTheThreeLists();
    assertThat(found("beta")).containsExactly(1, 2, 3);

    setEveryListTo(List.of("beta"));

    assertThat(found("beta")).containsExactly(1, 2, 3);
  }

  @Test
  void anUpdateKeepsTheRecordUnderBm25Too() {
    createType(" METADATA {\"similarity\": \"BM25\"}");
    insertTheThreeLists();

    setEveryListTo(List.of("beta"));

    assertThat(found("beta")).containsExactly(1, 2, 3);
  }

  @Test
  void anUpdateThatDropsTheWordRemovesItAndAddsTheNewOne() {
    createType("");
    insertTheThreeLists();

    setEveryListTo(List.of("gamma", "GAMMA"));

    assertThat(found("beta")).isEmpty();
    assertThat(found("gamma")).containsExactly(1, 2, 3);

    setEveryListTo(List.of("gamma"));
    assertThat(found("gamma")).containsExactly(1, 2, 3);
  }

  @Test
  void aDeleteRemovesEveryTokenOfTheRecord() {
    createType("");
    insertTheThreeLists();

    database.transaction(() -> database.command("sql", "DELETE FROM L WHERE id = 1"));

    assertThat(found("beta")).containsExactly(2, 3);
  }

  /**
   * A BM25 index counts one document per RECORD, of the length of all its items together: the list used to be put item by
   * item, so every item counted as a document of its own.
   */
  @Test
  void theBm25CountersCountOneDocumentPerRecord() {
    createType(" METADATA {\"similarity\": \"BM25\"}");
    insertTheThreeLists();
    database.transaction(() -> database.newDocument("L").set("id", 4).save()); // no list at all

    assertThat(metadata().getTotalDocs()).isEqualTo(4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(2L + 2L + 3L);

    setEveryListTo(List.of("beta"));
    assertThat(metadata().getTotalDocs()).isEqualTo(4L);
    assertThat(metadata().getSumDocLength()).isEqualTo(3L);

    database.transaction(() -> database.command("sql", "DELETE FROM L WHERE id = 1"));
    database.transaction(() -> database.command("sql", "DELETE FROM L WHERE id = 4"));
    assertThat(metadata().getTotalDocs()).isEqualTo(2L);
    assertThat(metadata().getSumDocLength()).isEqualTo(2L);

    // The recount reads the list too: it used to look for a property called "tags by item", found none, and counted 0 tokens
    database.command("sql", "REBUILD INDEX `" + index().getName() + "` WITH statsOnly = true");
    assertThat(metadata().getTotalDocs()).isEqualTo(2L);
    assertThat(metadata().getSumDocLength()).isEqualTo(2L);
  }

  /**
   * Postings written the way the index wrote them before the fix, one put per list ITEM: an update and a delete through the
   * per-record key remove them all, since a posting is keyed by (token, RID) whatever term frequency it carries.
   */
  @Test
  void postingsWrittenPerItemBeforeTheFixAreRemovedByTheRecordKey() {
    createType(" METADATA {\"similarity\": \"BM25\"}");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("L").set("id", 1).set("tags", List.of("gamma")).save().getIdentity());
    // The pre-fix layout for [beta, BETA, delta]: one put per item, each carrying the stats of that item alone
    database.transaction(() -> {
      for (final String item : List.of("beta", "BETA", "delta"))
        for (final Index bucketIndex : index().getIndexesOnBuckets())
          if (bucketIndex.getAssociatedBucketId() == rid[0].getBucketId())
            bucketIndex.put(new Object[] { item }, new RID[] { rid[0] });
    });
    assertThat(found("beta")).containsExactly(1);

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM L WHERE id = 1")) {
        rs.next().toElement().modify().set("tags", List.of("beta", "BETA", "delta")).save();
      }
    });
    database.transaction(() -> database.command("sql", "UPDATE L SET tags = ['epsilon'] WHERE id = 1"));
    assertThat(found("beta")).isEmpty();
    assertThat(found("delta")).isEmpty();
    assertThat(found("epsilon")).containsExactly(1);

    database.transaction(() -> database.command("sql", "DELETE FROM L WHERE id = 1"));
    assertThat(found("epsilon")).isEmpty();
  }

  /** A list emptied and filled again: no posting while empty, the record still counted once all along. */
  @Test
  void aListEmptiedAndFilledAgainKeepsPostingsAndCountersRight() {
    createType(" METADATA {\"similarity\": \"BM25\"}");
    insertTheThreeLists();

    setEveryListTo(List.of());
    assertThat(found("beta")).isEmpty();
    assertThat(metadata().getTotalDocs()).isEqualTo(3L);
    assertThat(metadata().getSumDocLength()).isZero();

    database.transaction(() -> database.command("sql", "UPDATE L SET tags = null"));
    assertThat(found("beta")).isEmpty();
    assertThat(metadata().getTotalDocs()).isEqualTo(3L);

    setEveryListTo(List.of("beta", "gamma"));
    assertThat(found("beta")).containsExactly(1, 2, 3);
    assertThat(found("gamma")).containsExactly(1, 2, 3);
    assertThat(metadata().getTotalDocs()).isEqualTo(3L);
    assertThat(metadata().getSumDocLength()).isEqualTo(6L);

    // Emptied again, then deleted: the delete of a record with an empty list still takes it out of the counters
    database.transaction(() -> database.command("sql", "UPDATE L SET tags = [] WHERE id = 1"));
    assertThat(metadata().getSumDocLength()).isEqualTo(4L);
    database.transaction(() -> database.command("sql", "DELETE FROM L WHERE id = 1"));
    assertThat(metadata().getTotalDocs()).isEqualTo(2L);
    assertThat(metadata().getSumDocLength()).isEqualTo(4L);
    assertThat(found("beta")).containsExactly(2, 3);
  }

  /**
   * An index built before the fix counted one document per list ITEM. The first search of a session finds the persisted count
   * disagrees with the record count and recounts, now reading the lists, so the counters come back exact by themselves.
   */
  @Test
  void countersLeftByThePerItemCountingAreRepairedByTheFirstSearch() {
    createType(" METADATA {\"similarity\": \"BM25\"}");
    insertTheThreeLists();
    // What the per-item counting left behind for these records: 7 documents (one per item) of length 1
    metadata().applyCorpusDelta(7 - 3, 7 - 7);
    ((DatabaseInternal) database).getSchema().getEmbedded().saveConfiguration();
    reopenDatabase();
    assertThat(metadata().getTotalDocs()).isEqualTo(7L);

    assertThat(found("beta")).containsExactly(1, 2, 3);

    assertThat(metadata().getTotalDocs()).isEqualTo(3L);
    assertThat(metadata().getSumDocLength()).isEqualTo(7L);
  }

  @Test
  void moreLikeThisReadsTheTermsOfTheList() {
    createType("");
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("L").set("id", 1).set("tags", List.of("database", "graph", "vector")).save().getIdentity();
      rids[1] = database.newDocument("L").set("id", 2).set("tags", List.of("graph", "database")).save().getIdentity();
      database.newDocument("L").set("id", 3).set("tags", List.of("cooking", "recipes")).save();
    });

    final List<RID> similar = new ArrayList<>();
    for (final Index bucketIndex : index().getIndexesOnBuckets()) {
      final IndexCursor cursor = ((LSMTreeFullTextIndex) bucketIndex).searchMoreLikeThis(Set.of(rids[0]),
          new MoreLikeThisConfig().setMinTermFreq(1).setMinDocFreq(1));
      while (cursor.hasNext())
        similar.add(cursor.next().getIdentity());
    }

    assertThat(similar).containsExactly(rids[1]);
  }

  @Test
  void aMapByValueIndexKeepsTheRecordToo() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("M");
      t.createProperty("id", Type.INTEGER);
      t.createProperty("labels", Type.MAP);
    });
    database.command("sql", "CREATE INDEX ON M (labels BY VALUE) FULL_TEXT");
    database.transaction(() -> database.newDocument("M").set("id", 1).set("labels", Map.of("a", "beta", "b", "BETA")).save());

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM M WHERE id = 1")) {
        rs.next().toElement().modify().set("labels", Map.of("a", "beta")).save();
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT id FROM M WHERE labels CONTAINSTEXT 'beta'")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void aMapByKeyIndexKeepsTheRecordToo() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType("K");
      t.createProperty("id", Type.INTEGER);
      t.createProperty("labels", Type.MAP);
    });
    database.command("sql", "CREATE INDEX ON K (labels BY KEY) FULL_TEXT");
    database.transaction(() -> database.newDocument("K").set("id", 1).set("labels", Map.of("beta", 1, "BETA", 2)).save());

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM K WHERE id = 1")) {
        rs.next().toElement().modify().set("labels", Map.of("beta", 1)).save();
      }
    });

    try (final ResultSet rs = database.query("sql", "SELECT id FROM K WHERE labels CONTAINSTEXT 'beta'")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }
}
