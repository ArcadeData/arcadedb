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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  /**
   * An Object[] holding a long[] is stored as a list, so the reloaded record holds a List with a long[] inside. Its key has to
   * be the one the in-memory value was indexed under, or a delete leaves the entry behind.
   */
  @Test
  void nestedArrayKeyIsRemovedFromTheReloadedRecord() {
    database.command("opencypher", "CREATE INDEX FOR (n:Nest) ON (n.v)");
    database.transaction(() -> database.command("opencypher", "CREATE (:Nest {id: 1, v: $v})",
        Map.of("v", new Object[] { new long[] { 1, 2 }, "a" })));
    final Index index = database.getSchema().getIndexByName("Nest[v]");
    assertThat(index.countEntries()).isEqualTo(1L);
    assertThat(ids("MATCH (n:Nest) WHERE n.v = $p RETURN n.id AS id", Map.of("p", List.of(List.of(1, 2), "a"))))
        .containsExactly(1);

    database.transaction(() -> database.command("opencypher", "MATCH (n:Nest) SET n.v = 5"));
    assertThat(ids("MATCH (n:Nest) WHERE n.v = 5 RETURN n.id AS id", Map.of())).containsExactly(1);
    database.transaction(() -> database.command("opencypher", "MATCH (n:Nest) DELETE n"));
    assertThat(index.countEntries()).isZero();
  }

  /** [1.0] and [1] are equal for Cypher, whether the value is a double[], a long[] or a list: one key, so a seek finds all. */
  @Test
  void equalNumericListsShareOneKey() {
    database.command("opencypher", "CREATE INDEX FOR (n:Num) ON (n.v)");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Num {id: 1, v: $v})", Map.of("v", new double[] { 1.0 }));
      database.command("opencypher", "CREATE (:Num {id: 2, v: $v})", Map.of("v", new long[] { 1 }));
      database.command("opencypher", "CREATE (:Num {id: 3, v: $v})", Map.of("v", List.of(1)));
      database.command("opencypher", "CREATE (:Num {id: 4, v: $v})", Map.of("v", List.of(1.0f)));
      database.command("opencypher", "CREATE (:Num {id: 5, v: $v})", Map.of("v", List.of(List.of(2.50), new double[] { 3 })));
    });
    // the scan: a list comprehension is not an indexed property, so this compares every record's value
    assertThat(ids("MATCH (n:Num) WITH n, [x IN n.v | x] AS v WHERE v = [1] RETURN n.id AS id ORDER BY id", Map.of()))
        .containsExactly(1, 2, 3, 4);
    final String seek = "MATCH (n:Num) WHERE n.v = $p RETURN n.id AS id ORDER BY id";
    for (final Object p : List.of(List.of(1), List.of(1.0), new double[] { 1.0 }, new long[] { 1 }))
      assertThat(ids(seek, Map.of("p", p))).as("seek %s", p).containsExactly(1, 2, 3, 4);
    assertThat(ids(seek, Map.of("p", List.of(List.of(2.5), List.of(3))))).containsExactly(5);

    database.transaction(() -> database.command("opencypher", "MATCH (n:Num) DELETE n"));
    assertThat(database.getSchema().getIndexByName("Num[v]").countEntries()).isZero();
  }

  /** Type.convert to STRING without a property: the text of the content; with a declared property an array is refused. */
  @Test
  void convertToStringWritesTheContent() {
    assertThat(Type.convert(database, new int[0], String.class)).isEqualTo("[]");
    assertThat(Type.convert(database, new int[] { 1, 2 }, String.class)).isEqualTo("[1, 2]");
    assertThat(Type.convert(database, new String[] { "a", null }, String.class)).isEqualTo("[a, null]");
    assertThat(Type.convert(database, new Object[] { new long[] { 1 }, new Object[0], null }, String.class))
        .isEqualTo("[[1], [], null]");
    assertThat(Type.convert(database, List.of(new long[] { 1, 2 }, List.of(new byte[] { 3 })), String.class))
        .isEqualTo("[[1, 2], [[3]]]");
    assertThat(Type.convert(database, List.of(1.5, "a"), String.class)).isEqualTo("[1.5, a]");

    final DocumentType type = database.getSchema().createDocumentType("Conv");
    final Property property = type.createProperty("s", Type.STRING);
    // a declared STRING property holds the same text, as it does for the same list (issue #12896); a byte[] is still refused
    assertThat(Type.convert(database, new long[] { 1 }, String.class, property)).isEqualTo("[1]");
    assertThatThrownBy(() -> Type.convert(database, new byte[] { 1 }, String.class, property))
        .isInstanceOf(IllegalArgumentException.class);
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
