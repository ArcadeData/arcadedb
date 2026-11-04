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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultInternal;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10879: {@code vector.fuse} read the RID and the score of a source row through private copies of the shared row
 * extractors. Every row shape the extractors accept must reach the fusion exactly as {@code vector.mmr} / {@code vector.rerank}
 * read it: same RID, same score direction (a {@code distance} is flipped, so the nearest row ranks first).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10879VectorRowExtractorTest extends TestHelper {

  @Test
  void everyRowShapeIsFusedAsTheSharedExtractorReadsIt() {
    final RID[] rids = new RID[6];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc10879");
      for (int i = 0; i < rids.length; i++) {
        final MutableDocument d = database.newDocument("Doc10879");
        d.set("n", i);
        if (i == 3)
          // read through the Result that carries this record as its element
          d.set("score", 0.7f);
        d.save();
        rids[i] = d.getIdentity();
      }
    });

    final List<Object> rows = new ArrayList<>();
    // Map with @rid and a distance (the shape of `vector.neighbors`): lower distance = better
    rows.add(mapRow("@rid", rids[0], "distance", 0.9f));
    // Map with rid and $score
    rows.add(mapRow("rid", rids[1], "$score", 0.5f));
    // Map with @rid as an Identifiable (a document) and a score
    rows.add(mapRow("@rid", database.lookupByRID(rids[2], true), "score", 0.8f));
    // Result carrying the element: the identity and the score both come from the record
    rows.add(new ResultInternal(database.lookupByRID(rids[3], true)));
    // Result with an @rid property and a distance
    final ResultInternal withRidProperty = new ResultInternal(database);
    withRidProperty.setProperty("@rid", rids[4]);
    withRidProperty.setProperty("distance", 0.1f);
    rows.add(withRidProperty);
    // Result with a rid property and $score
    final ResultInternal withPlainRid = new ResultInternal(database);
    withPlainRid.setProperty("rid", rids[5]);
    withPlainRid.setProperty("$score", 0.2f);
    rows.add(withPlainRid);

    // What the shared extractors read off every row: the expected fusion of one source with itself is that ranking
    final List<Object> byScore = new ArrayList<>(rows);
    byScore.sort(Comparator.comparingDouble(r -> -SQLFunctionVectorAbstract.extractScoreFromRow(r)));
    final List<RID> expected = new ArrayList<>();
    byScore.forEach(r -> expected.add(SQLFunctionVectorAbstract.extractRidFromRow(r)));
    // The flipped distances: 0.1 is the nearest (-0.1), 0.9 the farthest (-0.9), below every positive score
    assertThat(expected).containsExactly(rids[2], rids[3], rids[1], rids[5], rids[4], rids[0]);

    final BasicCommandContext ctx = new BasicCommandContext();
    ctx.setDatabase(database);
    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("fusion", "LINEAR");
    final Object fused = new SQLFunctionVectorFuse().execute(null, null, null, new Object[] { rows, rows, options }, ctx);

    final List<RID> actual = new ArrayList<>();
    for (final Object row : (List<?>) fused)
      actual.add(SQLFunctionVectorAbstract.extractRidFromRow(row));
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  private static Map<String, Object> mapRow(final String ridKey, final Object rid, final String scoreKey, final float score) {
    final Map<String, Object> row = new LinkedHashMap<>();
    row.put(ridKey, rid);
    row.put(scoreKey, score);
    return row;
  }
}
