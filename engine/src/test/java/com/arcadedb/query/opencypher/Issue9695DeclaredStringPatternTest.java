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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9695: a value written into a property declared {@code STRING} is stored as its text, so the
 * same pattern has to find it again. Before, {@code MERGE (p:S {d: 1})} stored {@code '1'}, and neither the next
 * {@code MERGE} nor any {@code MATCH} ({@code {d: 1}} or {@code WHERE p.d = 1}) ever equalled it: every repeated MERGE
 * created a duplicate, or died on the UNIQUE index. A pattern operand is now compared with the text a write of it would
 * store, in MATCH and MERGE alike, and only against a declared STRING property: a schemaless {@code '1'} still differs
 * from {@code 1}, as the Cypher TCK requires.
 */
class Issue9695DeclaredStringPatternTest extends TestHelper {

  private static final String[] OPERANDS = { "1", "-7", "1.5", "true", "datetime('2021-06-15T12:30:00Z')",
      "localdatetime('2021-06-15T12:30:00')", "date('2021-06-15')", "time('12:30:00Z')", "localtime('12:30:00')",
      "duration('P1DT2H')", "[1, 2]" };

  private long count(final String query) {
    return count(query, Map.of());
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  private String merge(final String query) {
    final String[] tag = new String[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        final Result row = rs.next();
        tag[0] = row.getProperty("tag");
      }
    });
    return tag[0];
  }

  private void declare(final String type, final String index) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".d STRING");
    if (index != null)
      database.command("sql", "CREATE INDEX ON " + type + " (d) " + index);
  }

  @ParameterizedTest
  @ValueSource(strings = { "", "NOTUNIQUE", "UNIQUE" })
  void aRepeatedMergeIsIdempotentOnADeclaredStringProperty(final String index) {
    final String type = "M" + index;
    declare(type, index.isEmpty() ? null : index);
    for (final String operand : OPERANDS) {
      database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
      final String described = type + " " + operand;

      assertThat(merge("MERGE (p:" + type + " {d: " + operand + "}) ON CREATE SET p.tag = 'CREATED' ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag"))
          .as(described + " first MERGE").isEqualTo("CREATED");
      assertThat(merge("MERGE (p:" + type + " {d: " + operand + "}) ON CREATE SET p.tag = 'CREATED' ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag"))
          .as(described + " second MERGE finds what the first stored").isEqualTo("MATCHED");
      assertThat(count("MATCH (p:" + type + ") RETURN count(p) AS c")).as(described + " total").isEqualTo(1L);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "", "NOTUNIQUE", "UNIQUE" })
  void matchFindsWhatTheSamePatternStored(final String index) {
    final String type = "F" + index;
    declare(type, index.isEmpty() ? null : index);
    for (final String operand : OPERANDS) {
      database.transaction(() -> database.command("opencypher", "MATCH (n:" + type + ") DELETE n").close());
      database.transaction(() -> database.command("opencypher", "CREATE (:" + type + " {d: " + operand + "})").close());
      final String described = type + " " + operand;

      assertThat(count("MATCH (p:" + type + " {d: " + operand + "}) RETURN count(p) AS c")).as(described + " inline").isEqualTo(1L);
      assertThat(count("MATCH (p:" + type + ") WHERE p.d = " + operand + " RETURN count(p) AS c")).as(described + " WHERE =")
          .isEqualTo(1L);
      assertThat(count("MATCH (p:" + type + ") WHERE " + operand + " = p.d RETURN count(p) AS c")).as(described + " WHERE = reversed")
          .isEqualTo(1L);
      assertThat(count("MATCH (p:" + type + ") WHERE p.d <> " + operand + " RETURN count(p) AS c")).as(described + " WHERE <>")
          .isEqualTo(0L);
      // The same comparison inside a projection, which the expression evaluator answers
      assertThat(count("MATCH (p:" + type + ") RETURN sum(CASE WHEN p.d = " + operand + " THEN 1 ELSE 0 END) AS c"))
          .as(described + " projected =").isEqualTo(1L);
      assertThat(count("MATCH (p:" + type + ") WITH p.d = " + operand + " AS eq, p.d <> " + operand + " AS ne WHERE eq AND NOT ne RETURN count(*) AS c"))
          .as(described + " projected comparison").isEqualTo(1L);
    }
  }

  @Test
  void aDifferentValueStillDoesNotMatch() {
    declare("D", "NOTUNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:D {d: 1})").close());
    assertThat(count("MATCH (p:D {d: 2}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:D) WHERE p.d = 2 RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:D) WHERE p.d <> 2 RETURN count(p) AS c")).isEqualTo(1L);
    // A map is never stored, so it has no text to be found by
    assertThat(count("MATCH (p:D) WHERE p.d = {a: 1} RETURN count(p) AS c")).isEqualTo(0L);
    // 1.0 is stored as '1.0': a different text from the '1' the integer stored
    assertThat(count("MATCH (p:D {d: 1.0}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(merge("MERGE (p:D {d: 2}) ON CREATE SET p.tag = 'CREATED' RETURN p.tag AS tag")).isEqualTo("CREATED");
    assertThat(count("MATCH (p:D) RETURN count(p) AS c")).isEqualTo(2L);
  }

  @Test
  void aTextOperandStillMatchesTheText() {
    declare("T", "UNIQUE");
    database.transaction(() -> database.command("opencypher", "CREATE (:T {d: 1})").close());
    assertThat(count("MATCH (p:T {d: '1'}) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(merge("MERGE (p:T {d: '1'}) ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag")).isEqualTo("MATCHED");
  }

  /** The Cypher typing of {@code =} is unchanged where nothing is declared: {@code '1' = 1} is false (TCK). */
  @Test
  void aSchemalessPropertyKeepsCypherTyping() {
    database.command("sql", "CREATE VERTEX TYPE U");
    database.transaction(() -> database.command("opencypher", "CREATE (:U {d: '1'})").close());
    assertThat(count("MATCH (p:U {d: 1}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:U) WHERE p.d = 1 RETURN count(p) AS c")).isEqualTo(0L);
    database.transaction(() -> database.command("opencypher", "MERGE (p:U {d: 1})").close());
    assertThat(count("MATCH (p:U) RETURN count(p) AS c")).isEqualTo(2L);
  }

  @Test
  void anInheritedDeclarationCoercesToo() {
    declare("Base", null);
    database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Base");
    database.transaction(() -> database.command("opencypher", "MERGE (:Child {d: 42})").close());
    database.transaction(() -> database.command("opencypher", "MERGE (:Child {d: 42})").close());
    assertThat(count("MATCH (p:Child) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:Base {d: 42}) RETURN count(p) AS c")).isEqualTo(1L);
  }

  @Test
  void parametersAreCoercedLikeLiterals() {
    declare("P", "UNIQUE");
    final Map<String, Object> number = Map.of("d", 7L);
    final Map<String, Object> zoned = Map.of("d", ZonedDateTime.parse("2021-06-15T12:30:00Z"));
    for (final Map<String, Object> params : new Map[] { number, zoned }) {
      database.transaction(() -> database.command("opencypher", "MERGE (p:P {d: $d})", params).close());
      database.transaction(() -> database.command("opencypher", "MERGE (p:P {d: $d})", params).close());
      assertThat(count("MATCH (p:P {d: $d}) RETURN count(p) AS c", params)).as(params.toString()).isEqualTo(1L);
      assertThat(count("MATCH (p:P) WHERE p.d = $d RETURN count(p) AS c", params)).as(params.toString()).isEqualTo(1L);
    }
    assertThat(count("MATCH (p:P) RETURN count(p) AS c")).isEqualTo(2L);
  }

  @Test
  void aCompositeIndexCoercesItsStringKey() {
    database.command("sql", "CREATE VERTEX TYPE X");
    database.command("sql", "CREATE PROPERTY X.k INTEGER");
    database.command("sql", "CREATE PROPERTY X.d STRING");
    database.command("sql", "CREATE INDEX ON X (k, d) UNIQUE");
    database.transaction(() -> database.command("opencypher", "MERGE (p:X {k: 1, d: date('2021-06-15')})").close());
    assertThat(merge("MERGE (p:X {k: 1, d: date('2021-06-15')}) ON MATCH SET p.tag = 'MATCHED' RETURN p.tag AS tag")).isEqualTo("MATCHED");
    assertThat(count("MATCH (p:X {k: 1, d: date('2021-06-15')}) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:X) RETURN count(p) AS c")).isEqualTo(1L);
  }

  @Test
  void aRelationshipPropertyDeclaredStringIsCoercedInEveryClause() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE PROPERTY R.d STRING");
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:A {id: 2})").close());
    final String anchors = "MATCH (a:A {id: 1}), (b:A {id: 2}) ";
    database.transaction(() -> database.command("opencypher", anchors + "MERGE (a)-[:R {d: 5}]->(b)").close());
    database.transaction(() -> database.command("opencypher", anchors + "MERGE (a)-[:R {d: 5}]->(b)").close());
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).as("MERGE").isEqualTo(1L);

    assertThat(count("MATCH ()-[r:R {d: 5}]->() RETURN count(r) AS c")).as("inline").isEqualTo(1L);
    assertThat(count("MATCH (:A {id: 1})-[r:R {d: 5}]->(:A {id: 2}) RETURN count(r) AS c")).as("anchored inline").isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R]->() WHERE r.d = 5 RETURN count(r) AS c")).as("WHERE").isEqualTo(1L);
    assertThat(count("MATCH (a:A {id: 1}) RETURN size([(a)-[:R {d: 5}]->(b) | b]) AS c")).as("pattern comprehension").isEqualTo(1L);
    assertThat(count("MATCH (a:A {id: 1})-[:R*1..2 {d: 5}]->(b) RETURN count(b) AS c")).as("variable length").isEqualTo(1L);

    database.transaction(() -> database.command("opencypher",
        anchors + "CALL merge.relationship(a, 'R', {d: 5}, {}, b) YIELD rel RETURN rel").close());
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).as("merge.relationship").isEqualTo(1L);
  }

  @Test
  void aNodePatternInsideARelationshipPatternIsCoerced() {
    declare("N", null);
    database.command("sql", "CREATE EDGE TYPE L");
    database.transaction(() -> database.command("opencypher", "CREATE (:N {d: 3})-[:L]->(:N {d: true})").close());
    assertThat(count("MATCH (:N {d: 3})-[:L]->(b:N {d: true}) RETURN count(b) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:N) WHERE (a)-[:L]->(:N {d: true}) RETURN count(a) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (a:N) WHERE EXISTS { (a)-[:L]->(:N {d: true}) } RETURN count(a) AS c")).isEqualTo(1L);
  }

  /** Membership is equality against each element, so IN agrees with =, on a list and on a lazy range alike. */
  @Test
  void inAgreesWithEquals() {
    declare("I", null);
    database.command("sql", "CREATE VERTEX TYPE IU");
    database.transaction(() -> database.command("opencypher", "CREATE (:I {d: 5}), (:I {d: date('2021-06-15')}), (:I {d: 'x'}), (:IU {d: '5'})").close());
    assertThat(count("MATCH (p:I) WHERE p.d IN [5, 6] RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:I) WHERE p.d IN [date('2021-06-15')] RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:I) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:I) WHERE p.d IN range(6, 9) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:I) WHERE p.d IN ['x', 7] RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:I) WHERE NOT p.d IN [5] RETURN count(p) AS c")).isEqualTo(2L);
    // Only the canonical spelling of a long is a member of a range: '-0' and '05' were never written by an integer
    database.transaction(() -> database.command("opencypher", "CREATE (:I {d: '-0'}), (:I {d: '05'}), (:I {d: 0})").close());
    assertThat(count("MATCH (p:I) WHERE p.d IN range(-1, 1) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:I) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(2L);
    // schemaless: '5' IN [5] stays false
    assertThat(count("MATCH (p:IU) WHERE p.d IN [5] RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:IU) WHERE p.d IN range(0, 9) RETURN count(p) AS c")).isEqualTo(0L);
  }

  /**
   * A correlated inline match ({@code UNWIND ... MATCH (n {d: x})}) over an unindexed type is answered from a hash of the
   * scanned values: an integer has to find the digits it was stored as there too.
   */
  @Test
  void aCorrelatedInlineMatchOverAnUnindexedTypeFindsTheStoredDigits() {
    declare("H", null);
    database.command("sql", "CREATE VERTEX TYPE HU");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.command("opencypher", "CREATE (:H {d: " + i + "})").close();
        database.command("opencypher", "CREATE (:HU {d: '" + i + "'})").close();
      }
    });
    assertThat(count("UNWIND range(0, 19) AS x MATCH (n:H {d: x}) RETURN count(n) AS c")).isEqualTo(20L);
    assertThat(count("UNWIND range(0, 19) AS x MATCH (n:HU {d: x}) RETURN count(n) AS c")).as("schemaless").isEqualTo(0L);
    assertThat(count("UNWIND range(0, 19) AS x MATCH (n:HU {d: toString(x)}) RETURN count(n) AS c")).isEqualTo(20L);
  }

  /**
   * A join of two unconnected parts on {@code c.code = p.num} keeps the pairs the same WHERE accepts over their product:
   * by a hash join when nothing is indexed, and by an index seek per row when {@code code} is.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void aJoinOnADeclaredStringPropertyKeepsThePairsTheComparisonAccepts(final boolean indexed) {
    database.command("sql", "CREATE VERTEX TYPE JC");
    database.command("sql", "CREATE PROPERTY JC.code STRING");
    if (indexed)
      database.command("sql", "CREATE INDEX ON JC (code) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE JP");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.command("opencypher", "CREATE (:JC {id: " + i + ", code: " + i + "})").close();
      database.command("opencypher", "CREATE (:JC {id: 100, code: true}), (:JC {id: 101, code: '5.0'}), (:JC {id: 102, code: 'x'})").close();
      // Past 2^53 two longs share one double key: only the comparison above the join tells them apart
      database.command("opencypher", "CREATE (:JC {id: 103, code: 9007199254740993}), (:JC {id: 104, code: '9007199254740992'})").close();
      database.command("opencypher", "CREATE (:JC {id: 105, code: '02134'}), (:JC {id: 106, code: '1-2'}), (:JC {id: 107, code: -0.0})").close();
      // Texts made of a number's characters in no number's shape: their own keys, never a parse
      database.command("opencypher", "CREATE (:JC {id: 108, code: 'E'}), (:JC {id: 109, code: '2024-01'}), (:JC {id: 110, code: '1.'}), (:JC {id: 111, code: '1E'})").close();
      database.command("opencypher", "CREATE (:JP {id: 1, num: 5}), (:JP {id: 2, num: true}), (:JP {id: 3, num: 5.5}), (:JP {id: 4, num: '7'})")
          .close();
      database.command("opencypher", "CREATE (:JP {id: 5, num: 9007199254740993}), (:JP {id: 6, num: 2134}), (:JP {id: 7, num: 0.0})").close();
      // Either side of the 18-digit long fast path, and the longest long text there is
      database.command("opencypher", "CREATE (:JC {id: 112, code: 123456789012345678}), (:JC {id: 113, code: 1234567890123456789}), "
          + "(:JC {id: 114, code: -9223372036854775808})").close();
      database.command("opencypher", "CREATE (:JP {id: 8, num: 123456789012345678}), (:JP {id: 9, num: 1234567890123456789}), "
          + "(:JP {id: 10, num: -9223372036854775808})").close();
    });

    final String query = "MATCH (p:JP), (c:JC) WHERE c.code = p.num RETURN p.id AS a, c.id AS b ORDER BY a, b";
    final String product = query.replace("c.code = p.num", "(c.code = p.num OR rand() < 0)");
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains(indexed ? "IndexNestedLoopJoin" : "ValueHashJoin");
    }
    final List<String> joined = pairs(query);
    assertThat(joined).containsExactly("1/5", "2/100", "4/7", "5/103", "8/112", "9/113", "10/114");
    assertThat(joined).isEqualTo(pairs(product));
  }

  private List<String> pairs(final String query) {
    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        pairs.add(row.<Object>getProperty("a") + "/" + row.<Object>getProperty("b"));
      }
    }
    return pairs;
  }

  /** Over schemaless properties a join keeps Cypher typing: '5' and 5 share a hash key, and the comparison still rejects them. */
  @Test
  void aJoinOnASchemalessPropertyKeepsCypherTyping() {
    database.command("sql", "CREATE VERTEX TYPE SC");
    database.command("sql", "CREATE VERTEX TYPE SP2");
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.command("opencypher", "CREATE (:SC {id: " + i + ", code: '" + i + "'})").close();
      database.command("opencypher", "CREATE (:SP2 {id: 1, num: 5}), (:SP2 {id: 2, num: '5'})").close();
    });
    assertThat(pairs("MATCH (p:SP2), (c:SC) WHERE c.code = p.num RETURN p.id AS a, c.id AS b ORDER BY a, b")).containsExactly("2/5");
    assertThat(count("MATCH (c:SC) WHERE c.code IN [5] RETURN count(c) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (c:SC) WHERE c.code <> 5 RETURN count(c) AS c")).isEqualTo(30L);
  }

  /** NaN equals nothing, not even the 'NaN' it was stored as; a signed zero is found by the text it was stored as. */
  @Test
  void nanAndSignedZero() {
    declare("Z", null);
    database.transaction(() -> database.command("opencypher", "CREATE (:Z {id: 1, d: 0.0 / 0.0}), (:Z {id: 2, d: -0.0}), (:Z {id: 3, d: 0.0})").close());
    assertThat(count("MATCH (p:Z {d: 0.0 / 0.0}) RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:Z) WHERE p.d = 0.0 / 0.0 RETURN count(p) AS c")).isEqualTo(0L);
    assertThat(count("MATCH (p:Z {d: -0.0}) RETURN count(p) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (p:Z {d: 0.0}) RETURN count(p) AS c")).isEqualTo(1L);
    database.transaction(() -> database.command("opencypher", "MERGE (p:Z {d: -0.0})").close());
    assertThat(count("MATCH (p:Z) RETURN count(p) AS c")).isEqualTo(3L);
  }

  /** A node reached through a persisted link is bound as its RID until a property is read: it is coerced all the same. */
  @Test
  void aNodeBoundThroughALinkIsCoercedToo() {
    declare("LT", null);
    database.command("sql", "CREATE VERTEX TYPE LH");
    database.transaction(() -> {
      final RID target = database.newVertex("LT").set("d", 3).save().getIdentity();
      database.newVertex("LH").set("link", target).save();
    });
    assertThat(count("MATCH (h:LH) WITH h.link AS t WHERE t.d = 3 RETURN count(*) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (h:LH) WITH h.link AS t WHERE t.d IN [3] RETURN count(*) AS c")).isEqualTo(1L);
  }

  @Test
  void mergeNodeProcedureIsIdempotent() {
    declare("PN", null);
    database.transaction(() -> database.command("opencypher", "CALL merge.node(['PN'], {d: 9}) YIELD node RETURN node").close());
    database.transaction(() -> database.command("opencypher", "CALL merge.node(['PN'], {d: 9}) YIELD node RETURN node").close());
    assertThat(count("MATCH (p:PN) RETURN count(p) AS c")).isEqualTo(1L);
  }
}
