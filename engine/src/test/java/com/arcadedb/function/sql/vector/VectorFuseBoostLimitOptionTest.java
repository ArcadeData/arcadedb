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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code limit} option of {@code vector.fuse} and {@code vector.boost} is "max rows returned, default unbounded": a limit of
 * 0 returns no row and a negative limit is refused, as the count of {@code vector.neighbors} is. It used to read both as "no
 * limit", so a limit computed to 0 (or -1) by a caller returned every candidate.
 */
class VectorFuseBoostLimitOptionTest extends TestHelper {
  private final List<RID> rids = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("LimitDoc");
    database.transaction(() -> {
      for (int i = 0; i < 4; i++) {
        final MutableDocument d = database.newDocument("LimitDoc");
        d.set("n", i);
        d.save();
        rids.add(d.getIdentity());
      }
    });
  }

  private List<Map<String, Object>> rows(final int... ids) {
    final List<Map<String, Object>> list = new ArrayList<>();
    for (final int id : ids)
      list.add(Map.of("@rid", rids.get(id), "score", 1.0f - id * 0.1f));
    return list;
  }

  private int fuseCount(final String options) {
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.fuse`(?, ?, " + options + "))", rows(0, 1, 2), rows(2, 3))) {
      int n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      return n;
    }
  }

  private int boostCount(final String limitOption) {
    try (final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.boost`(?, { boosts: [{ field: 'n', weight: 0.5 }]" + limitOption + " }))", rows(0, 1, 2, 3))) {
      int n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      return n;
    }
  }

  @Test
  void fuseLimitCapsTheRows() {
    assertThat(fuseCount("{ limit: 2 }")).isEqualTo(2);
    assertThat(fuseCount("{ limit: 1000 }")).isEqualTo(4);
    assertThat(fuseCount("{ fusion: 'RRF' }")).isEqualTo(4);
    assertThat(fuseCount("{ limit: null }")).isEqualTo(4);
  }

  @Test
  void fuseLimitZeroReturnsNoRow() {
    assertThat(fuseCount("{ limit: 0 }")).isZero();
  }

  @Test
  void fuseNegativeLimitIsRefused() {
    assertThatThrownBy(() -> fuseCount("{ limit: -1 }")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("limit").hasMessageContaining("negative");
  }

  @Test
  void boostLimitCapsTheRows() {
    assertThat(boostCount(", limit: 2")).isEqualTo(2);
    assertThat(boostCount(", limit: 1000")).isEqualTo(4);
    assertThat(boostCount("")).isEqualTo(4);
  }

  @Test
  void boostLimitZeroReturnsNoRow() {
    assertThat(boostCount(", limit: 0")).isZero();
  }

  @Test
  void boostNegativeLimitIsRefused() {
    assertThatThrownBy(() -> boostCount(", limit: -3")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("limit").hasMessageContaining("negative");
  }
}
