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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where an entry of the index data file meets the end of a page. The entries are written back to back and a new page is started
 * when the next one does not fit; an entry counted one or two bytes short would be written over the end of the page. Entries are
 * a few bytes wide, so across consecutive page sizes the free space left at the end of a page takes every value from 0 up: for each
 * size the same sequence of inserts, rewrites and deletes is written, every page is parsed back and each entry is accounted for,
 * and after a reopen every live record finds itself by its own vector.
 */
class LSMVectorIndexPageFitOracleTest extends TestHelper {
  private static final int DIM        = 6;
  private static final int FIRST_PAGE = 256;
  private static final int SIZES      = 40;
  private static final int INSERTS    = 70;
  private static final int REWRITES   = 9;
  private static final int DELETES    = 8;

  @ParameterizedTest
  @EnumSource(value = VectorQuantizationType.class, names = { "NONE", "INT8" })
  void everyEntryIsWrittenWholeWhereverThePageEnds(final VectorQuantizationType quantization) {
    final Random rnd = new Random(41);
    final Map<String, Map<RID, float[]>> live = new HashMap<>();
    final Map<String, Integer> pagesSeen = new HashMap<>();

    for (int s = 0; s < SIZES; s++) {
      final String typeName = "Doc" + s;
      final int pageSize = FIRST_PAGE + s * 3;
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName).createProperty("vector", Type.ARRAY_OF_FLOATS);
        ((TypeLSMVectorIndexBuilder) database.getSchema().buildTypeIndex(typeName, new String[] { "vector" }).withLSMVectorType()
            .withPageSize(pageSize)).withDimensions(DIM).withQuantization(quantization).withSimilarity("EUCLIDEAN").create();
      });

      final Map<RID, float[]> rows = new HashMap<>();
      final List<RID> order = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < INSERTS; i++) {
          final float[] v = randomVector(rnd);
          final RID rid = database.newDocument(typeName).set("vector", v).save().getIdentity();
          rows.put(rid, v);
          order.add(rid);
        }
      });
      // a rewrite writes the new entry and a tombstone for the one it supersedes, a delete one tombstone
      database.transaction(() -> {
        for (int i = 0; i < REWRITES; i++) {
          final float[] v = randomVector(rnd);
          database.lookupByRID(order.get(i), true).asDocument(true).modify().set("vector", v).save();
          rows.put(order.get(i), v);
        }
        for (int i = 0; i < DELETES; i++) {
          database.lookupByRID(order.get(REWRITES + i), true).asDocument(true).delete();
          rows.remove(order.get(REWRITES + i));
        }
      });
      live.put(typeName, rows);

      final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(typeName + "[vector]"))
          .getIndexesOnBuckets()[0];
      final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
      final int[] cutShort = new int[1];
      LSMVectorIndexPageParser.parsePages((DatabaseInternal) database, index.getFileId(), index.getTotalPages(), index.getPageSize(), false,
          true, entries::add, cutShort);
      assertThat(cutShort[0]).as("no page of page size %d is cut short", pageSize).isZero();
      assertThat(entries).as("entries written at page size %d", pageSize).hasSize(INSERTS + 2 * REWRITES + DELETES);
      assertThat(entries.stream().filter(e -> e.deleted).count()).as("tombstones at page size %d", pageSize).isEqualTo(REWRITES + DELETES);
      pagesSeen.put(typeName, index.getTotalPages());
    }
    assertThat(pagesSeen.values().stream().distinct().count()).as("the sizes span more than one page count").isGreaterThan(1);

    reopenDatabase();

    for (final Map.Entry<String, Map<RID, float[]>> type : live.entrySet())
      for (final Map.Entry<RID, float[]> row : type.getValue().entrySet()) {
        final List<RID> answer = new ArrayList<>();
        try (final ResultSet rs = database.query("sql",
            "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('" + type.getKey() + "[vector]', ?, 1, {efSearch: 100})))", (Object) row.getValue())) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            answer.add(((Identifiable) r.getProperty("rid")).getIdentity());
          }
        }
        assertThat(answer).as("%s: %s finds itself by its own vector", type.getKey(), row.getKey()).containsExactly(row.getKey());
      }
  }

  private static float[] randomVector(final Random rnd) {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian() * 10f;
    return v;
  }
}
