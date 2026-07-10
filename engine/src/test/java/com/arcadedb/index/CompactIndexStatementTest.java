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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.sparsevector.LSMSparseVectorIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5144: index compaction must be reachable from SQL (and therefore HTTP)
 * via {@code COMPACT INDEX}, not only from the Java {@code Index.compact()} API.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CompactIndexStatementTest extends TestHelper {

  private static final int    DIMENSIONS = 1000;
  private static final String TYPE_NAME  = "CompactDoc";
  private static final String IDX_NAME   = "CompactDoc[tokens,weights]";

  @Test
  void compactSparseVectorIndexViaSql() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });

    // Build 3 sealed segments by flushing the memtable between insert batches.
    for (int batch = 0; batch < 3; batch++) {
      insertDocs(20, batch * 1000);
      flushSubIndexes();
    }

    final LSMSparseVectorIndex sub = firstSubIndex();
    assertThat(sub.getEngine().segmentCount()).as("3 sealed segments before compaction").isEqualTo(3);

    final ResultSet rs = database.command("sql", "COMPACT INDEX `" + IDX_NAME + "`");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<String>getProperty("operation")).isEqualTo("compact index");
    assertThat(result.<Boolean>getProperty("compacted")).isTrue();
    assertThat(result.<java.util.List<String>>getProperty("indexes")).contains(IDX_NAME);

    assertThat(sub.getEngine().segmentCount()).as("segments merged into 1 after COMPACT INDEX").isEqualTo(1);

    // Data must still be queryable after compaction.
    final ResultSet count = database.query("sql", "SELECT count(*) AS c FROM " + TYPE_NAME);
    assertThat(count.next().<Long>getProperty("c")).isEqualTo(60L);
  }

  @Test
  void compactAllIndexesViaSql() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("name", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    });

    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("name", "name-" + i);
        doc.save();
      }
    });

    // COMPACT INDEX * must not fail on a regular LSM-tree index and must report the operation.
    final ResultSet rs = database.command("sql", "COMPACT INDEX *");
    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<String>getProperty("operation")).isEqualTo("compact index");

    // The type index remains fully queryable.
    final ResultSet q = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE name = 'name-42'");
    assertThat(q.hasNext()).isTrue();
  }

  @Test
  void compactLsmTreeIndexActuallyMergesViaSql() {
    // Disable background auto-compaction so it cannot race the SQL statement and steal the
    // COMPACTION_SCHEDULED status (which would silently turn the merge into a no-op).
    final Object previous = database.getConfiguration().getValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    try {
      final String typeName = "LsmCompactDoc";
      final String idxName = typeName + "[email]";
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
        type.createProperty("email", Type.STRING);
        // Small pages so a few hundred entries seal several immutable pages the compactor will merge
        // (kept modest to stay under the root-page pointer capacity without an intermediate compaction).
        database.getSchema().buildTypeIndex(typeName, new String[] { "email" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(2048).create();
      });

      // Insert across several transactions so the mutable index accumulates many sealed pages.
      for (int batch = 0; batch < 5; batch++) {
        final int from = batch * 200;
        database.transaction(() -> {
          for (int i = from; i < from + 200; i++)
            database.newDocument(typeName).set("email", String.format("e%06d", i)).save();
        });
      }

      final ResultSet rs = database.command("sql", "COMPACT INDEX `" + idxName + "`");
      final Result result = rs.next();
      assertThat(result.<String>getProperty("operation")).isEqualTo("compact index");
      // A real merge must have run on the LSM-tree index (not just a clean no-op).
      assertThat(result.<Boolean>getProperty("compacted")).as("LSM-tree index actually compacted").isTrue();

      // Every key must still resolve through the compacted index, and the total count must be intact.
      assertThat(database.query("sql", "SELECT FROM " + typeName + " WHERE email = 'e000000'").hasNext()).isTrue();
      assertThat(database.query("sql", "SELECT FROM " + typeName + " WHERE email = 'e000999'").hasNext()).isTrue();
      assertThat(database.query("sql", "SELECT count(*) AS c FROM " + typeName).next().<Long>getProperty("c")).isEqualTo(1000L);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, previous);
    }
  }

  private void insertDocs(final int n, final int seedOffset) {
    database.transaction(() -> {
      final Random rnd = new Random(0xCAFEL + seedOffset);
      for (int i = 0; i < n; i++) {
        final int[]   tokens  = new int[8];
        final float[] weights = new float[8];
        final HashSet<Integer> picked = new HashSet<>();
        for (int j = 0; j < 8; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMENSIONS);
          } while (!picked.add(dim));
          tokens[j] = dim;
          weights[j] = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", tokens);
        doc.set("weights", weights);
        doc.save();
      }
    });
  }

  private void flushSubIndexes() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    for (final IndexInternal sub : typeIndex.getIndexesOnBuckets())
      sub.flush();
  }

  private LSMSparseVectorIndex firstSubIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }
}
