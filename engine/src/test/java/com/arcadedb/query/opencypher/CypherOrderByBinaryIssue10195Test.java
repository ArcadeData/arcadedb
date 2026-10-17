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
 * Regression tests for #10195: ORDER BY on a BINARY property compared two byte arrays through their identity
 * {@code toString()}, so the order changed from one execution to the next. The rows must come back in unsigned byte
 * order, every time, and in the same order SQL gives.
 */
class CypherOrderByBinaryIssue10195Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("PV").createProperty("c_bin", Type.BINARY);
    database.getSchema().getType("PV").createProperty("id", Type.INTEGER);
    database.transaction(() -> {
      database.newVertex("PV").set("id", 1).set("c_bin", new byte[] { 0, 1, (byte) 0xfe, (byte) 0xff }).save();
      database.newVertex("PV").set("id", 2).set("c_bin", new byte[] {}).save();
      database.newVertex("PV").set("id", 3).set("c_bin", new byte[] { 0x7f }).save();
      database.newVertex("PV").set("id", 4).set("c_bin", new byte[] { (byte) 0x80 }).save();
      database.newVertex("PV").set("id", 5).set("c_bin", new byte[] { 0, 1 }).save();
    });
  }

  private List<Integer> ids(final String language, final String query) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        ids.add(rs.next().<Integer>getProperty("id"));
    }
    return ids;
  }

  @Test
  void ascendingOrderIsUnsignedAndStable() {
    // empty < 0x0001 < 0x0001feff < 0x7f < 0x80 (0x80 is -128 as a signed byte, so a signed order would put it first)
    for (int i = 0; i < 20; i++)
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin")).containsExactly(2, 5, 1, 3, 4);
  }

  @Test
  void descendingOrderIsTheReverse() {
    for (int i = 0; i < 20; i++)
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin DESC")).containsExactly(4, 3, 1, 5, 2);
  }

  @Test
  void cypherAndSqlAgree() {
    assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin"))
        .isEqualTo(ids("sql", "SELECT id FROM PV ORDER BY c_bin"));
  }
}
