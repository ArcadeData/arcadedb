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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Compaction tests for BM25 full-text indexes. Uses a small (1024-byte) page size with enough documents sharing one
 * high-frequency token that its posting list provably spans multiple compacted pages - the exact case that used to silently drop
 * postings on read (a sparse-root-index bug, see {@code LSMTreeIndexCompactor}). The small page guarantees the multi-page split
 * is exercised rather than incidental. Verifies that after compaction the full result set, ranking and scores are unchanged.
 * <p>
 * Not tagged {@code slow}: although it inserts a few hundred documents, it completes in well under a second and pins a
 * correctness regression (postings silently dropped on compaction), so it must run in every CI build.
 */
class FullTextBM25CompactionTest extends TestHelper {

  private Map<String, Float> searchScores(final String query) {
    final Map<String, Float> scores = new HashMap<>();
    final ResultSet rs = database.query("sql",
        "SELECT name, $score FROM Doc WHERE SEARCH_INDEX('Doc[content]', '" + query + "') = true");
    while (rs.hasNext()) {
      final Result r = rs.next();
      scores.put(r.getProperty("name"), ((Number) r.getProperty("$score")).floatValue());
    }
    return scores;
  }

  private boolean forceCompaction() {
    boolean compacted = false;
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof LSMTreeFullTextIndex ftIndex) {
        try {
          ((IndexInternal) ftIndex).scheduleCompaction();
          compacted |= ftIndex.compact();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return compacted;
  }

  @Test
  void postingsAndTermFrequencySurviveCompaction() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.getSchema().getType("Doc").createProperty("name", String.class);
      database.getSchema().getType("Doc").createProperty("content", String.class);
      // Small pages (1024 bytes) so the multi-page split is GUARANTEED, not incidental: the "data" posting list below has 600
      // RIDs at ~6-8 bytes each (compressed RID + tf + docLength varints) ~= 3.6-4.8 KB, which provably overflows a 1024-byte
      // page several times over - exactly the spanning-multiple-compacted-pages condition the fix targets.
      database.getSchema().buildTypeIndex("Doc", new String[] { "content" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType().withPageSize(1024).create();
    });

    // 1 rare document with the discriminative term; many documents share the common token "data".
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'rare', content = 'quantum data analysis'"));
    for (int batch = 0; batch < 30; batch++) {
      final int base = batch;
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.command("sql",
              "INSERT INTO Doc SET name = 'common" + base + "_" + i + "', content = 'data record number " + base + " " + i + "'");
      });
    }

    final Map<String, Float> before = new HashMap<>();
    database.transaction(() -> before.putAll(searchScores("quantum data")));
    assertThat(before).containsKey("rare");
    assertThat(before.size()).isGreaterThanOrEqualTo(601); // 1 rare + 600 common all match "quantum data"

    assertThat(forceCompaction()).as("the full-text index should have compacted at least one bucket").isTrue();

    database.transaction(() -> {
      final Map<String, Float> after = searchScores("quantum data");
      // No postings dropped by compaction, and identical ranking + scores (tf + docLength preserved through the merge).
      assertThat(after.keySet()).isEqualTo(before.keySet());
      for (final Map.Entry<String, Float> e : after.entrySet()) {
        assertThat(e.getValue()).isCloseTo(before.get(e.getKey()), within(1e-4f));
        if (!"rare".equals(e.getKey()))
          assertThat(after.get("rare")).isGreaterThan(e.getValue());
      }
    });
  }

  @Test
  void classicPostingsSurviveCompaction() {
    // The compaction posting-drop bug was independent of BM25 - it affected CLASSIC too. This pins the fix for CLASSIC: a
    // single token's posting list spanning multiple compacted pages must not lose documents.
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.getSchema().getType("Doc").createProperty("name", String.class);
      database.getSchema().getType("Doc").createProperty("content", String.class);
      database.getSchema().buildTypeIndex("Doc", new String[] { "content" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType().withSimilarity("CLASSIC").withPageSize(1024).create();
    });

    for (int batch = 0; batch < 30; batch++) {
      final int base = batch;
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.command("sql",
              "INSERT INTO Doc SET name = 'd" + base + "_" + i + "', content = 'data record number " + base + " " + i + "'");
      });
    }

    final Map<String, Float> before = new HashMap<>();
    database.transaction(() -> before.putAll(searchScores("data")));
    assertThat(before.size()).isGreaterThanOrEqualTo(600); // every document contains the common token "data"

    assertThat(forceCompaction()).as("the full-text index should have compacted at least one bucket").isTrue();

    database.transaction(() -> assertThat(searchScores("data").keySet()).isEqualTo(before.keySet()));
  }
}
