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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7059: Neo4j 5 dynamic-label syntax {@code SET n:$(expression)} was parsed by the grammar
 * but never interpolated. {@code CypherASTBuilder.visitSetClause} flattened the whole {@code nodeLabels}
 * subtree with {@code getText()} and split the result on {@code [:&|]}, so the <i>source text</i> of the
 * expression became the label: a vertex type literally named {@code $(node.labels)} was created and
 * attached to every affected node, with no error at write time.
 * <p>
 * {@code visitRemoveClause} carried the mirror-image defect with the opposite symptom - it iterated
 * {@code nodeLabels().labelType()} only, so a dynamic label was silently dropped and {@code REMOVE n:$(x)}
 * was a no-op.
 * <p>
 * Node <i>patterns</i> already resolved {@code $(expr)} labels per row, so the gap was confined to the
 * two write clauses.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class CypherDynamicLabelSetIssue7059Test extends TestHelper {

  /** The exact shape graphiti emits for bulk node saves. */
  @Test
  void bulkMergeWithDynamicLabelAppliesTheInterpolatedLabel() {
    final List<Map<String, Object>> nodes = List.of(
        Map.of("uuid", "a", "labels", "Finding", "name", "x"),
        Map.of("uuid", "b", "labels", "Finding", "name", "y"));

    database.transaction(() -> database.command("opencypher", """
        UNWIND $nodes AS node
        MERGE (n:Entity {uuid: node.uuid})
        SET n:$(node.labels)
        SET n = node
        RETURN n.uuid
        """, Map.of("nodes", nodes)).close());

    // No junk type named after the uninterpolated expression, in any composite position.
    assertThat(schemaTypeNames()).noneMatch(name -> name.contains("$("));

    // Both nodes carry Entity AND the interpolated Finding label.
    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity) RETURN n.uuid AS uuid, labels(n) AS l ORDER BY uuid");
    final List<String> uuids = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      uuids.add(row.getProperty("uuid"));
      final List<String> labels = row.getProperty("l");
      assertThat(labels).containsExactlyInAnyOrder("Entity", "Finding");
    }
    assertThat(uuids).containsExactly("a", "b");
  }

  /** A dynamic label reachable through the {@code Finding} label proves the type really was created. */
  @Test
  void theInterpolatedLabelIsMatchable() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n:$($label)", Map.of("label", "Finding")).close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Finding) RETURN count(n) AS c");
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
  }

  /** A dynamic label expression may yield a list: every entry becomes a label, as in a node pattern. */
  @Test
  void aListValuedDynamicLabelAddsEveryLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n:$($labels)",
          Map.of("labels", List.of("Finding", "Claim"))).close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactlyInAnyOrder("Entity", "Finding", "Claim");
    assertThat(schemaTypeNames()).noneMatch(name -> name.contains("$("));
  }

  /** Static and dynamic labels mix in one SET item, in either order. */
  @Test
  void staticAndDynamicLabelsCombineInOneSetItem() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n:Audited:$($label):Reviewed",
          Map.of("label", "Finding")).close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l"))
        .containsExactlyInAnyOrder("Entity", "Audited", "Finding", "Reviewed");
  }

  /** The {@code IS} spelling of the same syntax: {@code SET n IS $(expr)}. */
  @Test
  void theIsSpellingInterpolatesToo() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n IS $($label)", Map.of("label", "Finding")).close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactlyInAnyOrder("Entity", "Finding");
    assertThat(schemaTypeNames()).noneMatch(name -> name.contains("$("));
  }

  /** REMOVE had the opposite symptom - the dynamic label was dropped from the AST, so the clause no-op'd. */
  @Test
  void removeWithADynamicLabelActuallyRemovesIt() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity:Finding {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) REMOVE n:$($label)", Map.of("label", "Finding")).close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactly("Entity");
  }

  /** A per-row expression, not just a parameter: each row contributes its own label. */
  @Test
  void theExpressionIsEvaluatedPerRow() {
    database.transaction(() -> database.command("opencypher", """
        UNWIND [{uuid: 'a', labels: 'Finding'}, {uuid: 'b', labels: 'Claim'}] AS node
        CREATE (n:Entity {uuid: node.uuid})
        SET n:$(node.labels)
        """).close());

    assertThat(database.query("opencypher", "MATCH (n:Finding) RETURN count(n) AS c").next().<Long>getProperty("c"))
        .isEqualTo(1L);
    assertThat(database.query("opencypher", "MATCH (n:Claim) RETURN count(n) AS c").next().<Long>getProperty("c"))
        .isEqualTo(1L);
    assertThat(schemaTypeNames()).noneMatch(name -> name.contains("$("));
  }

  /**
   * A label expression that resolves to something that is not a usable label name is rejected rather
   * than {@code toString()}-ed into a junk type: silently creating a type named {@code 42} or {@code {a=1}}
   * is the same class of schema corruption this issue is about.
   */
  @Test
  void anUnusableDynamicLabelValueIsRejected() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (n:Entity {uuid: 'a'}) SET n:$($label)", Map.of("label", 42)).close()))
        .hasMessageContaining("label");

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (n:Entity {uuid: 'a'}) SET n:$($label)", Map.of("label", "")).close()))
        .hasMessageContaining("label");

    assertThat(schemaTypeNames()).noneMatch(name -> name.equals("42"));
  }

  /** A dynamic label that evaluates to null contributes nothing, matching the read-path contract. */
  @Test
  void aNullDynamicLabelContributesNothing() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n:$(n.missing)").close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactly("Entity");
  }

  /**
   * The label expression can be the <i>only</i> place a binding is read. {@code CypherVariableUsage} decides which
   * bindings a write clause keeps alive, and it looked at the SET target and the value/target expressions only - so
   * an edge read solely from a dynamic label would have been dropped, the expression would have evaluated to null,
   * and the clause would have silently written nothing. Issues #5137 and #5013 are the same defect on the other
   * right-hand sides.
   */
  @Test
  void anEdgeReadOnlyByTheLabelExpressionStaysBound() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Entity {uuid: 'a'})-[:LINK {kind: 'Finding'}]->(b:Entity {uuid: 'b'})").close());

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:Entity {uuid: 'a'})-[r:LINK]->(b) SET a:$(r.kind)").close());

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactlyInAnyOrder("Entity", "Finding");
  }

  /** The same, for the REMOVE side. */
  @Test
  void anEdgeReadOnlyByTheRemoveLabelExpressionStaysBound() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Entity:Finding {uuid: 'a'})-[:LINK {kind: 'Finding'}]->(b:Entity {uuid: 'b'})").close());

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:Entity {uuid: 'a'})-[r:LINK]->(b) REMOVE a:$(r.kind)").close());

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactly("Entity");
  }

  /**
   * SET is a simultaneous assignment: every right-hand side reads the state the clause began with, so a label
   * expression reading a property the same clause overwrites must still see the pre-clause value (issue #5190).
   */
  @Test
  void theLabelExpressionReadsThePreClauseState() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a', kind: 'Finding'})").close());

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:Entity {uuid: 'a'}) SET n.kind = 'Claim', n:$(n.kind)").close());

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l, n.kind AS k");
    final Result row = rs.next();
    assertThat(row.<List<String>>getProperty("l")).containsExactlyInAnyOrder("Entity", "Finding");
    assertThat(row.<String>getProperty("k")).isEqualTo("Claim");
  }

  /** A dynamic label carrying the composite-type separator would decompose into two labels on read-back. */
  @Test
  void aDynamicLabelCarryingTheCompositeSeparatorIsRejected() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (n:Entity {uuid: 'a'}) SET n:$($label)", Map.of("label", "A~B")).close()))
        .hasMessageContaining("label");

    assertThat(schemaTypeNames()).doesNotContain("A~B");
  }

  /**
   * The REMOVE counterpart of {@link #theLabelExpressionReadsThePreClauseState()}. A clause assigns
   * simultaneously in Cypher, so a label expression reading a property the same clause removes must still
   * see the pre-clause value - {@code RemoveStep} applied its items in source order, so the property was
   * already gone and the expression evaluated to null, making the label removal a silent no-op.
   */
  @Test
  void theRemoveLabelExpressionReadsThePreClauseState() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Entity {uuid: 'a', kind: 'Finding'})").close();
      database.command("opencypher", "MATCH (n:Entity {uuid: 'a'}) SET n:$(n.kind)").close();
    });

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:Entity {uuid: 'a'}) REMOVE n.kind, n:$(n.kind)").close());

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l, n.kind AS k");
    final Result row = rs.next();
    assertThat(row.<List<String>>getProperty("l")).containsExactly("Entity");
    assertThat(row.<String>getProperty("k")).isNull();
  }

  /**
   * {@code visitNodePattern} strips the backticks off an escaped variable, and so does the REMOVE side of
   * this clause pair, so a SET that keeps them cannot find the row value and drops the label write.
   */
  @Test
  void aBacktickEscapedVariableAcceptsADynamicLabel() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Entity {uuid: 'a'})").close());

    database.transaction(() -> database.command("opencypher",
        "MATCH (`my node`:Entity {uuid: 'a'}) SET `my node`:$($label)", Map.of("label", "Finding")).close());

    final ResultSet rs = database.query("opencypher", "MATCH (n:Entity {uuid: 'a'}) RETURN labels(n) AS l");
    assertThat(rs.next().<List<String>>getProperty("l")).containsExactlyInAnyOrder("Entity", "Finding");
  }

  private List<String> schemaTypeNames() {
    return database.getSchema().getTypes().stream().map(t -> t.getName()).toList();
  }
}
