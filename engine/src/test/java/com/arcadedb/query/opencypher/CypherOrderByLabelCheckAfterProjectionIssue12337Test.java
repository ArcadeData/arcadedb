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
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LabelCheckExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.rewriter.ExpressionRewriter;
import com.arcadedb.query.opencypher.rewriter.ProjectedExpressionSubstituter;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12337: after a collapsing projection that renames a variable ({@code RETURN n AS m, count(*) AS c}), an ORDER BY
 * label check of that variable ({@code ORDER BY n:A}, {@code ORDER BY n:$(expr)}) failed with "UndefinedVariable", while
 * {@code ORDER BY n.name} in the same position sorted on {@code m.name}. The label check is re-pointed at the output column
 * the same way: its subject and every dynamic label expression are rewritten, its labels, operator and predicate kept.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOrderByLabelCheckAfterProjectionIssue12337Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:A {name:'a', kind:'C'}), (:B {name:'b', kind:'B'}), (:C {name:'c', kind:'A'}), (:A:B {name:'ab', kind:'C'})").close());
  }

  private List<Object> names(final String query) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("name"));
    }
    return out;
  }

  @Test
  void staticLabelCheckAfterAggregationSortsOnTheProjectedColumn() {
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:A DESC, name"))
        .containsExactly("a", "ab", "b", "c");
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:A ASC, name"))
        .containsExactly("b", "c", "a", "ab");
  }

  @Test
  void dynamicLiteralLabelCheckAfterAggregationSortsOnTheProjectedColumn() {
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:$('B') DESC, name"))
        .containsExactly("ab", "b", "a", "c");
  }

  @Test
  void dynamicLabelReadingThePropertyOfTheRenamedVariableIsRewrittenToo() {
    // n:$(n.kind): only 'b' (label B, kind B) carries the label its own kind names, so n.kind must follow n to m as well
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:$(n.kind) DESC, name"))
        .containsExactly("b", "a", "ab", "c");
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:$(n.kind) ASC, name"))
        .containsExactly("a", "ab", "c", "b");
  }

  @Test
  void disjunctionAndLabelExpressionKeepTheirOperator() {
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:B|C DESC, name"))
        .containsExactly("ab", "b", "c", "a");
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY n:A&!B DESC, name"))
        .containsExactly("a", "ab", "b", "c");
  }

  @Test
  void distinctProjectionAndWithClauseResolveTheSameWay() {
    assertThat(names("MATCH (n) RETURN DISTINCT n.name AS name, n AS m ORDER BY n:A DESC, name"))
        .containsExactly("a", "ab", "b", "c");
    assertThat(names("MATCH (n) WITH n.name AS name, n AS m, count(*) AS c ORDER BY n:A DESC, name RETURN name"))
        .containsExactly("a", "ab", "b", "c");
  }

  /**
   * The same gap, one node type over: every predicate or value node the base rewriter did not descend into stranded a
   * reference to the renamed variable, and the ORDER BY failed as an undefined variable.
   */
  @Test
  void predicatesAndValueNodesOverTheRenamedVariableAreRewrittenToo() {
    final String prefix = "MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY ";
    assertThat(names(prefix + "n.kind IS NULL, name")).containsExactly("a", "ab", "b", "c");
    assertThat(names(prefix + "n.kind IN ['A', 'B'] DESC, name")).containsExactly("b", "c", "a", "ab");
    assertThat(names(prefix + "n.name STARTS WITH 'a' DESC, name")).containsExactly("a", "ab", "b", "c");
    assertThat(names(prefix + "n.name =~ '.b' DESC, name")).containsExactly("ab", "a", "b", "c");
    assertThat(names(prefix + "CASE WHEN n:C THEN 0 WHEN n.kind = 'B' THEN 1 ELSE 2 END, name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "CASE n.kind WHEN 'A' THEN 0 ELSE 1 END, name")).containsExactly("c", "a", "ab", "b");
    assertThat(names(prefix + "CASE n.kind WHEN > 'A' THEN 0 ELSE 1 END, name")).containsExactly("a", "ab", "b", "c");
    assertThat(names(prefix + "[n.kind, n.name][0], name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "{k: n.kind}.k, name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "n.kind[0..1], name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "labels(n)[0], name")).containsExactly("a", "ab", "b", "c");
    assertThat(names(prefix + "NOT n:A, name")).containsExactly("a", "ab", "b", "c");
    assertThat(names(prefix + "n:A AND n:B DESC, name")).containsExactly("ab", "a", "b", "c");
    assertThat(names(prefix + "n:C XOR n.kind = 'B' DESC, name")).containsExactly("b", "c", "a", "ab");
    assertThat(names(prefix + "n {k: n.kind}.k, name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "n {.kind}.kind, name")).containsExactly("c", "b", "a", "ab");
  }

  @Test
  void scopedExpressionsRewriteOnlyWhatTheirLocalVariableDoesNotShadow() {
    final String prefix = "MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY ";
    assertThat(names(prefix + "[x IN [n.kind] | x + n.name][0], name")).containsExactly("c", "b", "a", "ab");
    assertThat(names(prefix + "any(x IN [n.kind] WHERE x = 'A') DESC, name")).containsExactly("c", "a", "ab", "b");
    assertThat(names(prefix + "reduce(s = n.kind, x IN [n.name] | s + x), name")).containsExactly("c", "b", "a", "ab");
    // the local n shadows the projected n: it is the list element, not the node, so nothing is rewritten and nothing fails
    assertThat(names(prefix + "size([n IN [1, 2] | n]), name")).containsExactly("a", "ab", "b", "c");
    // the local m would capture the column n is projected as: the list is outside the scope and still follows n to m
    assertThat(names(prefix + "[m IN [n.kind] | m][0], name")).containsExactly("c", "b", "a", "ab");
    // ... while a reference to the outer n inside that scope is left as written and reported, never read from the local m
    assertThatThrownBy(() -> names(prefix + "[m IN [1] | n.kind][0], name")).hasMessageContaining("n");
  }

  /**
   * A quoted binder ({@code `n`}) used to be kept with its backticks while the body's references are decoded, so the local
   * bound a different name than the body read: the evaluation missed it, and the shadowing guard did not see that {@code `n`}
   * shadows the projected {@code n}.
   */
  @Test
  void aQuotedBinderIsTheSameVariableItsBodyReads() {
    assertThat(column("RETURN [`x` IN [1, 2] | x * 10] AS v")).containsExactly(List.of(10L, 20L));
    assertThat(column("RETURN any(`x` IN [1, 2] WHERE x = 2) AS v")).containsExactly(true);
    assertThat(column("RETURN reduce(`s` = 0, `x` IN [1, 2, 3] | s + x) AS v")).containsExactly(6L);
    assertThat(column("MATCH (`n` {name:'a'}) RETURN `n` {.kind}.kind AS v")).containsExactly("C");

    // The local `n` shadows the projected n: the sort key is the constant 'z', so the rows keep the name order
    assertThat(names("MATCH (n) RETURN n.name AS name, n AS m, count(*) AS c ORDER BY [`n` IN [{kind:'z'}] | n.kind][0], name"))
        .containsExactly("a", "ab", "b", "c");
  }

  private List<Object> column(final String query) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty("v"));
    }
    return out;
  }

  @Test
  void aLabelCheckOfAVariableTheProjectionDroppedIsStillRefused() {
    database.transaction(() -> database.command("opencypher", "MATCH (a {name:'a'}), (b {name:'b'}) CREATE (a)-[:R]->(b)").close());
    assertThatThrownBy(() -> names("MATCH (n)-[:R]->(x) RETURN n.name AS name, count(*) AS c ORDER BY x:B DESC"))
        .hasMessageContaining("x");
  }

  @Test
  void rewriterRebuildsSubjectAndDynamicLabelsAndKeepsTheRest() {
    final ExpressionRewriter substituter = new ProjectedExpressionSubstituter(Map.of("n", "m"), Map.of("n", "m"));

    final LabelCheckExpression check = new LabelCheckExpression(new VariableExpression("n"), List.of("A"),
        List.of(new VariableExpression("n")), LabelCheckExpression.LabelOperator.OR, "n:A|$(n)");
    final Object rewritten = substituter.rewrite(check);

    assertThat(rewritten).isInstanceOf(LabelCheckExpression.class).isNotSameAs(check);
    final LabelCheckExpression result = (LabelCheckExpression) rewritten;
    assertThat(((VariableExpression) result.getVariableExpression()).getVariableName()).isEqualTo("m");
    assertThat(((VariableExpression) result.getDynamicLabels().getFirst()).getVariableName()).isEqualTo("m");
    assertThat(result.getLabels()).containsExactly("A");
    assertThat(result.getOperator()).isEqualTo(LabelCheckExpression.LabelOperator.OR);
    assertThat(result.getText()).isEqualTo("n:A|$(n)");

    // Nothing to rewrite: the very same instance comes back
    final LabelCheckExpression untouched = new LabelCheckExpression(new VariableExpression("x"), List.of("A"),
        List.of((Expression) new LiteralExpression("B", "'B'")), LabelCheckExpression.LabelOperator.AND, "x:A:$('B')");
    assertThat(substituter.rewrite(untouched)).isSameAs(untouched);
    assertThat(substituter.rewrite(untouched)).isInstanceOf(BooleanExpression.class);
  }
}
