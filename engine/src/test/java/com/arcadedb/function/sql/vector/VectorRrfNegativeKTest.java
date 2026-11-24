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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The RRF constant {@code k} of {@code vector.fuse} and {@code vector.rrfScore} is a non-negative number: a negative one makes
 * {@code k + rank} zero for some rank (a score of Infinity) or negative (scores that rank the worst result first), so it is
 * refused as a negative count is elsewhere.
 */
class VectorRrfNegativeKTest extends TestHelper {
  private final List<RID> rids = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("RrfDoc");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++) {
        final MutableDocument d = database.newDocument("RrfDoc");
        d.set("n", i);
        d.save();
        rids.add(d.getIdentity());
      }
    });
  }

  private List<Map<String, Object>> rows(final int... ids) {
    final List<Map<String, Object>> list = new ArrayList<>();
    for (final int id : ids)
      list.add(Map.of("@rid", rids.get(id)));
    return list;
  }

  private List<Float> fuseScores(final long k) {
    final List<Float> scores = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.fuse`(?, ?, { k: " + k + " }))", rows(0, 1), rows(1, 2))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        scores.add(((Number) r.getProperty("score")).floatValue());
      }
    }
    return scores;
  }

  @Test
  void fuseAcceptsZeroAndPositiveK() {
    // k = 0: doc 1 is rank 2 and rank 1 -> 1/2 + 1/1; doc 0 is rank 1 -> 1; doc 2 is rank 2 -> 1/2
    assertThat(fuseScores(0)).containsExactly(1.5f, 1.0f, 0.5f);
    assertThat(fuseScores(1)).hasSize(3);
  }

  @Test
  void fuseRefusesANegativeK() {
    assertThatThrownBy(() -> fuseScores(-1)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("'k'").hasMessageContaining("negative");
  }

  @Test
  void rrfScoreRefusesANegativeK() {
    assertThat(database.query("sql", "SELECT `vector.rrfScore`(1, 2, { k: 0 }) AS s").next().<Number>getProperty("s").floatValue())
        .isEqualTo(1.5f);
    assertThatThrownBy(() -> database.query("sql", "SELECT `vector.rrfScore`(1, 2, { k: -1 }) AS s").next())
        .hasMessageContaining("negative");
    assertThatThrownBy(() -> database.query("sql", "SELECT `vector.rrfScore`([1, 2], { k: -1 }) AS s").next())
        .hasMessageContaining("negative");
  }
}
