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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11578: an index openCypher creates on a property nothing declares has the key type STRING, and a
 * primitive array value (what a numeric list nested in an HTTP parameter row becomes, or a BINARY) was keyed by its Java
 * identity text, {@code [J@10eb4f88}: a seek never found the record, and since the text differs on every read, an update or
 * a delete of the value left its old entry in the index for good. The key is now the text of the array's content, the
 * one a list of the same values has.
 */
class UndeclaredIndexArrayKeyIssue11578Test extends TestHelper {

  @Test
  void arrayKeysAreFoundUpdatedAndDeleted() {
    database.command("opencypher", "CREATE INDEX FOR (n:N) ON (n.l)");
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 50; i++)
      rows.add(Map.of("id", i, "l", new long[] { i, i + 1 }));
    database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row CREATE (:N {id: row.id, l: row.l})",
        Map.of("rows", rows)));
    final Index index = database.getSchema().getIndexByName("N[l]");
    assertThat(index.countEntries()).isEqualTo(50L);

    assertThat(ids("MATCH (n:N) WHERE n.l = $p RETURN n.id AS id", Map.of("p", List.of(7, 8)))).containsExactly(7);
    assertThat(ids("MATCH (n:N) WHERE n.l = $p RETURN n.id AS id", Map.of("p", new long[] { 7, 8 }))).containsExactly(7);

    database.transaction(() -> database.command("opencypher", "MATCH (n:N) WHERE n.id < 25 SET n.l = $l",
        Map.of("l", new long[] { 1000 })));
    database.transaction(() -> database.command("opencypher", "MATCH (n:N) WHERE n.id >= 40 DELETE n"));
    assertThat(index.countEntries()).isEqualTo(40L);
    assertThat(ids("MATCH (n:N) WHERE n.l = $p RETURN n.id AS id ORDER BY id", Map.of("p", new long[] { 1000 }))).hasSize(25);
    assertThat(ids("MATCH (n:N) WHERE n.l = $p RETURN n.id AS id", Map.of("p", new long[] { 30, 31 }))).containsExactly(30);
  }

  @Test
  void doubleAndBinaryKeys() {
    database.command("opencypher", "CREATE INDEX FOR (n:D) ON (n.v)");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:D {id: 1, v: $v})", Map.of("v", new double[] { 1.5, 2.5 }));
      database.command("opencypher", "CREATE (:D {id: 2, v: $v})", Map.of("v", new byte[] { 0, 1, 2 }));
    });
    assertThat(ids("MATCH (n:D) WHERE n.v = $p RETURN n.id AS id", Map.of("p", new double[] { 1.5, 2.5 }))).containsExactly(1);
    assertThat(database.getSchema().getIndexByName("D[v]").countEntries()).isEqualTo(2L);

    // the BINARY value is rewritten: its old entry has to go with it
    database.transaction(() -> database.command("opencypher", "MATCH (n:D {id: 2}) SET n.v = $v", Map.of("v", new byte[] { 3 })));
    assertThat(database.getSchema().getIndexByName("D[v]").countEntries()).isEqualTo(2L);

    database.transaction(() -> database.command("opencypher", "MATCH (n:D) DELETE n"));
    assertThat(database.getSchema().getIndexByName("D[v]").countEntries()).isZero();
  }

  private List<Integer> ids(final String query, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("id").intValue());
    }
    return out;
  }
}
