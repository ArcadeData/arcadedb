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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10775: with several indexed predicates on one node the openCypher planner anchored on the first indexed
 * property a {@code HashMap} iterated to, without comparing the costs it had computed, so which of two indexes was used
 * depended on the property names. The anchor is now the cheapest candidate access path of the node - an equality seek,
 * an IN-list seek, a seek per disjunct of an OR, or a range scan - and a tie goes to the predicate written first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10775AnchorPicksCheapestIndexTest extends TestHelper {
  private static final int ROWS = 2_000;

  // {unique property, non-unique property}: every pair appears in both roles, so a name that wins by its hash fails one of them
  private static final String[][] PAIRS = {
      { "uid", "grp" }, { "grp", "uid" }, { "a_id", "z_part" }, { "z_part", "a_id" }, { "doc_id", "group_name" },
      { "group_name", "doc_id" } };

  @Override
  protected void beginTest() {
    for (int k = 0; k < PAIRS.length; k++)
      createType("T10775_" + k, PAIRS[k][0], PAIRS[k][1]);

    // A label disjunction whose two roots carry the same pair of indexes, in both roles
    createType("A10775", "uid", "grp");
    createType("B10775", "uid", "grp");
    createType("C10775", "grp", "uid");
    createType("D10775", "grp", "uid");

    // A composite UNIQUE index beside a single-property UNIQUE one: only its whole key finds one row
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("E10775");
      type.createProperty("uid", String.class);
      type.createProperty("cat", String.class);
      type.createProperty("seq", Integer.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uid");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "cat", "seq");
    });
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("E10775").set("uid", "id" + i).set("cat", i % 2 == 0 ? "even" : "odd").set("seq", i).save();
    });
  }

  private void createType(final String typeName, final String uniqueProperty, final String plainProperty) {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType(typeName);
      type.createProperty(uniqueProperty, String.class);
      type.createProperty(plainProperty, String.class);
      type.createProperty("score", Integer.class);
      type.createProperty("rank", Integer.class);
      type.createProperty("note", String.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, uniqueProperty);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, plainProperty);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "score");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "rank");
    });
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex(typeName)
            .set(uniqueProperty, "id" + i)
            .set(plainProperty, i < ROWS / 2 ? "A" : "B")
            .set("score", i)
            .set("rank", i % 10)
            .set("note", i % 2 == 0 ? "even" : "odd")
            .save();
    });
  }

  @Test
  void inListOnBothPropertiesAnchorsOnTheUniqueIndexWhateverTheNames() {
    for (int k = 0; k < PAIRS.length; k++) {
      final String type = "T10775_" + k;
      final String u = PAIRS[k][0];
      final String n = PAIRS[k][1];
      final String query = "MATCH (r:" + type + ") WHERE r." + u + " IN $ids AND r." + n + " IN $groups RETURN r." + u + " AS e";
      final Map<String, Object> params = Map.of("ids", List.of("id0", "id1", "id2", "id3", "id4"), "groups", List.of("A"));

      assertThat(plan(query, params)).as("unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
      assertThat(values(query, params)).containsExactlyInAnyOrder("id0", "id1", "id2", "id3", "id4");
    }
  }

  @Test
  void equalityOnBothPropertiesAnchorsOnTheUniqueIndexWhateverTheNames() {
    for (int k = 0; k < PAIRS.length; k++) {
      final String type = "T10775_" + k;
      final String u = PAIRS[k][0];
      final String n = PAIRS[k][1];
      final String query = "MATCH (r:" + type + ") WHERE r." + u + " = $u AND r." + n + " = $g RETURN r." + u + " AS e";
      final Map<String, Object> params = Map.of("u", "id1", "g", "A");

      final String plan = plan(query, params);
      assertThat(plan).as("unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
      // A unique index resolved by its whole key finds one row at most
      assertThat(plan).as("unique=%s non-unique=%s", u, n).contains("rows=1]");
      assertThat(values(query, params)).containsExactly("id1");

      // The same predicates as an inline property map
      final String inline = "MATCH (r:" + type + " {" + n + ": 'A', " + u + ": 'id1'}) RETURN r." + u + " AS e";
      assertThat(plan(inline, Map.of())).as("inline unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
      assertThat(values(inline, Map.of())).containsExactly("id1");
    }
  }

  @Test
  void anInListOnTheUniqueIndexBeatsAnEqualityOnTheNonUniqueOne() {
    final String query = "MATCH (r:T10775_0) WHERE r.grp = $g AND r.uid IN $ids RETURN r.uid AS e";
    final Map<String, Object> params = Map.of("g", "A", "ids", List.of("id1", "id2", "id1300"));

    assertThat(plan(query, params)).contains("index=T10775_0[uid]");
    assertThat(values(query, params)).containsExactlyInAnyOrder("id1", "id2");

    final String swapped = "MATCH (r:T10775_1) WHERE r.uid = $g AND r.grp IN $ids RETURN r.grp AS e";
    assertThat(plan(swapped, params)).contains("index=T10775_1[grp]");
    assertThat(values(swapped, params)).containsExactlyInAnyOrder("id1", "id2");
  }

  @Test
  void aPrefixOfACompositeUniqueKeyIsNotCostedAsOneRow() {
    // cat alone is a prefix of the unique (cat, seq) key: one value of it is half the type, not one row
    final String prefix = "MATCH (r:E10775) WHERE r.cat IN ['even'] AND r.uid IN ['id2', 'id4'] RETURN r.uid AS e";
    assertThat(plan(prefix, Map.of())).contains("index=E10775[uid]");
    assertThat(values(prefix, Map.of())).containsExactlyInAnyOrder("id2", "id4");

    // With seq bound too the composite key is whole, and one seek of it beats two of uid
    final String wholeKey = "MATCH (r:E10775) WHERE r.cat IN ['even'] AND r.seq = 2 AND r.uid IN ['id2', 'id4'] RETURN r.uid AS e";
    assertThat(plan(wholeKey, Map.of())).contains("index=E10775[cat,seq]");
    assertThat(values(wholeKey, Map.of())).containsExactly("id2");
  }

  @Test
  void anOrOfSeeksOnTheUniqueIndexBeatsAnEqualityOnTheNonUniqueOne() {
    // Two unique seeks against one non-unique seek estimated at 10% of the type: with no distinct-key statistics the
    // model rates them close on a small type, so the fixture is sized for the 10% to be clearly the dearer one
    final String query = "MATCH (r:T10775_0) WHERE (r.uid = 'id1' OR r.uid = 'id1500') AND r.grp = 'A' RETURN r.uid AS e";

    assertThat(plan(query, Map.of())).contains("index=T10775_0[uid]");
    assertThat(values(query, Map.of())).containsExactly("id1");
  }

  @Test
  void anIndexedRangeIsUsedBesideAnEqualityThatHasNoIndex() {
    final String query = "MATCH (r:T10775_0) WHERE r.note = 'even' AND r.score >= 10 AND r.score < 20 RETURN r.uid AS e";

    final String plan = plan(query, Map.of());
    assertThat(plan).contains("NodeIndexRangeScan").contains("index=T10775_0[score]");
    assertThat(values(query, Map.of())).containsExactlyInAnyOrder("id10", "id12", "id14", "id16", "id18");
  }

  @Test
  void anEqualitySeekBeatsARangeOnAnotherProperty() {
    final String query = "MATCH (r:T10775_0) WHERE r.score > 5 AND r.uid = 'id7' RETURN r.uid AS e";

    assertThat(plan(query, Map.of())).contains("NodeIndexSeek").contains("index=T10775_0[uid]");
    assertThat(values(query, Map.of())).containsExactly("id7");
  }

  @Test
  void aBoundedRangeBeatsAHalfOpenOneWhateverTheNames() {
    final String byScore = "MATCH (r:T10775_0) WHERE r.rank > 2 AND r.score >= 10 AND r.score < 20 RETURN r.uid AS e";
    assertThat(plan(byScore, Map.of())).contains("index=T10775_0[score]");
    assertThat(values(byScore, Map.of())).containsExactlyInAnyOrder("id13", "id14", "id15", "id16", "id17", "id18", "id19");

    final String byRank = "MATCH (r:T10775_0) WHERE r.score > 2 AND r.rank >= 1 AND r.rank < 2 RETURN r.uid AS e";
    assertThat(plan(byRank, Map.of())).contains("index=T10775_0[rank]");
    assertThat(values(byRank, Map.of())).hasSize(ROWS / 10 - 1);
  }

  @Test
  void equalCostSeeksAnchorOnThePredicateWrittenFirst() {
    // Two seeks of the same cost: the tie goes to the order of the query, never to the names
    assertThat(plan("MATCH (r:T10775_0) WHERE r.score = 5 AND r.rank = 5 RETURN r", Map.of())).contains("index=T10775_0[score]");
    assertThat(plan("MATCH (r:T10775_0) WHERE r.rank = 5 AND r.score = 5 RETURN r", Map.of())).contains("index=T10775_0[rank]");
  }

  @Test
  void aUniqueEqualityBeatsAUniqueInOverAParameterOfTheSameEstimate() {
    // Both are one seek of one row as far as the planner can tell ($ids is one list it cannot size): the tie goes to the
    // equality, which is one key for certain, whichever of the two is written first
    final Map<String, Object> params = Map.of("u", "id1", "ids", List.of("id1", "id2", "id3"));

    final String inFirst = "MATCH (r:T10775_0) WHERE r.uid IN $ids AND r.uid = $u RETURN r.uid AS e";
    assertThat(plan(inFirst, params)).contains("index=T10775_0[uid]").doesNotContain("uid=IN");
    assertThat(values(inFirst, params)).containsExactly("id1");

    final String bothUnique = "MATCH (r:E10775) WHERE r.uid IN $ids AND r.cat = 'odd' AND r.seq = $s RETURN r.uid AS e";
    final Map<String, Object> seqParams = Map.of("ids", List.of("id1", "id3"), "s", 1);
    assertThat(plan(bothUnique, seqParams)).contains("index=E10775[cat,seq]");
    assertThat(values(bothUnique, seqParams)).containsExactly("id1");
  }

  @Test
  void aLabelDisjunctionAnchorsOnTheUniqueIndexWhateverTheNames() {
    final String byUid = "MATCH (r:A10775|B10775) WHERE r.grp = 'A' AND r.uid = 'id1' RETURN r.uid AS e";
    final String uidPlan = plan(byUid, Map.of());
    assertThat(uidPlan).contains("NodeByLabelDisjunctionIndexSeek").contains("index=A10775[uid]").contains("index=B10775[uid]");
    assertThat(values(byUid, Map.of())).containsExactly("id1", "id1");

    final String byGrp = "MATCH (r:C10775|D10775) WHERE r.uid = 'A' AND r.grp = 'id1' RETURN r.grp AS e";
    final String grpPlan = plan(byGrp, Map.of());
    assertThat(grpPlan).contains("NodeByLabelDisjunctionIndexSeek").contains("index=C10775[grp]").contains("index=D10775[grp]");
    assertThat(values(byGrp, Map.of())).containsExactly("id1", "id1");
  }

  private String plan(final String query, final Map<String, Object> params) {
    try (final ResultSet result = database.query("opencypher", "EXPLAIN " + query, params)) {
      return result.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  private List<String> values(final String query, final Map<String, Object> params) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet result = database.query("opencypher", query, params)) {
      while (result.hasNext()) {
        final Result row = result.next();
        values.add(row.getProperty("e"));
      }
    }
    return values;
  }
}
