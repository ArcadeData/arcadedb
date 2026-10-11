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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9717: compacting an LSM index created the new mutable and the new compacted component with the
 * field-default null strategy (SKIP). The schema reads the strategy back from the mutable component, so after the first
 * compaction an ERROR index accepted null keys and an INDEX index was planned as SKIP, in memory and in schema.json.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9717CompactionNullStrategyTest extends TestHelper {
  private static final String TYPE_NAME = "V9717";
  private static final int    PAGE_SIZE = 2048;
  private static final int    ROWS      = 5_000;

  @Test
  void compactionKeepsTheNullStrategyOfEveryComponent() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("k", Type.INTEGER);
      type.createProperty("f", Type.INTEGER);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(PAGE_SIZE).withNullStrategy(NULL_STRATEGY.ERROR).create();
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "f" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
          .withPageSize(PAGE_SIZE).withNullStrategy(NULL_STRATEGY.INDEX).create();
    });

    for (int batch = 0; batch < 5; batch++) {
      final int base = batch * (ROWS / 5);
      database.transaction(() -> {
        for (int i = base; i < base + ROWS / 5; i++) {
          final MutableDocument row = database.newDocument(TYPE_NAME).set("k", i);
          if (i % 10 != 0)
            row.set("f", i % 100);
          row.save();
        }
      });
    }

    compact(TYPE_NAME + "[k]");
    compact(TYPE_NAME + "[f]");

    assertStrategies();
    assertThat(bucketIndex(TYPE_NAME + "[f]").getMutableIndex().getSubIndex())
        .as("the compaction must have produced a compacted sub-index for the test to be meaningful").isNotNull();

    // A null key on the ERROR index is still refused after the compaction
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("f", 1).save()))
        .rootCause().hasMessageContaining("cannot be NULL");

    // A row moving from null to a value must lose its null entry: with the strategy read as SKIP, the removal of the null key
    // was skipped and the row was then counted twice by a range query
    final AtomicReference<RID> nullRow = new AtomicReference<>();
    database.transaction(() -> {
      final MutableDocument row = database.newDocument(TYPE_NAME).set("k", ROWS + 1);
      row.save();
      nullRow.set(row.getIdentity());
    });
    database.transaction(() -> nullRow.get().asDocument().modify().set("f", 3).save());

    assertThat(countWhere("f < 5")).isEqualTo(countBelowFiveByScan());
    assertThat(countWhere("f is null")).isEqualTo(ROWS / 10);

    reopenDatabase();

    assertStrategies();
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("f", 1).save()))
        .rootCause().hasMessageContaining("cannot be NULL");
    assertThat(countWhere("f < 5")).isEqualTo(countBelowFiveByScan());
    assertThat(countWhere("f is null")).isEqualTo(ROWS / 10);
  }

  private void assertStrategies() {
    assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[k]").getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
    assertThat(database.getSchema().getIndexByName(TYPE_NAME + "[f]").getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);

    final LSMTreeIndex k = bucketIndex(TYPE_NAME + "[k]");
    assertThat(k.getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
    if (k.getMutableIndex().getSubIndex() != null)
      assertThat(k.getMutableIndex().getSubIndex().getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);

    final LSMTreeIndex f = bucketIndex(TYPE_NAME + "[f]");
    assertThat(f.getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
    if (f.getMutableIndex().getSubIndex() != null)
      assertThat(f.getMutableIndex().getSubIndex().getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
  }

  private long countWhere(final String condition) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + TYPE_NAME + " WHERE " + condition)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countBelowFiveByScan() {
    long count = 0;
    try (final ResultSet rs = database.query("sql", "SELECT f FROM " + TYPE_NAME)) {
      while (rs.hasNext()) {
        final Integer f = rs.next().getProperty("f");
        if (f != null && f < 5)
          ++count;
      }
    }
    return count;
  }

  private LSMTreeIndex bucketIndex(final String indexName) {
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeIndex lsmIndex)
        return lsmIndex;
    throw new IllegalStateException("No LSM bucket index found for " + indexName);
  }

  private void compact(final String indexName) {
    database.async().waitCompletion();
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      try {
        for (int attempt = 0; attempt < 200 && !bucketIndex.scheduleCompaction(); ++attempt)
          Thread.sleep(25); // a background compaction owns the status; let it finish
        bucketIndex.compact();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
  }
}
