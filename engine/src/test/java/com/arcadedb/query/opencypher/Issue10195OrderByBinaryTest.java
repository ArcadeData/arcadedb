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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10195: Cypher {@code ORDER BY} over a BINARY property compared two {@code byte[]} by their identity
 * {@code toString()}, which changes with every load of the record, so the order changed from run to run. Two byte
 * arrays are now compared by their bytes, unsigned and lexicographically, which is the order SQL gives them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10195OrderByBinaryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("PV").createProperty("c_bin", Type.BINARY);
      database.getSchema().getType("PV").createProperty("id", Type.INTEGER);
    });
    database.transaction(() -> {
      database.newVertex("PV").set("id", 1).set("c_bin", new byte[] { 0, 1, (byte) 0xfe, (byte) 0xff }).save();
      database.newVertex("PV").set("id", 2).set("c_bin", new byte[] {}).save();
      database.newVertex("PV").set("id", 3).set("c_bin", new byte[] { 0x7f }).save();
      database.newVertex("PV").set("id", 4).set("c_bin", new byte[] { (byte) 0x80 }).save();
      database.newVertex("PV").set("id", 5).set("c_bin", new byte[] { 0, 1 }).save();
    });
  }

  @Test
  void orderByBinaryIsStableAndUnsigned() {
    // EMPTY FIRST, A PREFIX BEFORE ITS EXTENSIONS, 0x80 AFTER 0x7f (UNSIGNED)
    for (int i = 0; i < 20; i++) {
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin")).containsExactly(2, 5, 1, 3, 4);
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin DESC")).containsExactly(4, 3, 1, 5, 2);
    }
  }

  @Test
  void orderByBinaryMatchesSql() {
    assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin"))
        .containsExactlyElementsOf(ids("sql", "SELECT id FROM PV ORDER BY c_bin"));
    assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin DESC"))
        .containsExactlyElementsOf(ids("sql", "SELECT id FROM PV ORDER BY c_bin DESC"));
  }

  @Test
  void orderByBinaryWithLimitIsStable() {
    // A LIMIT AFTER ORDER BY TAKES THE TOP-K PATH
    for (int i = 0; i < 20; i++)
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin LIMIT 2")).containsExactly(2, 5);
  }

  @Test
  void orderByBinaryInWithFeedsCollectInOrder() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:PV) WITH n.c_bin AS b ORDER BY b RETURN collect(b)[1] AS second")) {
      assertThat(rs.next().<byte[]>getProperty("second")).containsExactly(0, 1);
    }
  }

  private List<Integer> ids(final String language, final String query) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("id").intValue());
    }
    return out;
  }
}
