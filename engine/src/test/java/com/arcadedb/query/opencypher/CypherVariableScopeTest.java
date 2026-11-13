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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Variable scope across clauses: stale projections, REMOVE scope, positional variable kinds and pattern comprehension scope.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherVariableScopeTest {
  /**
   * Issue #5671, part 2: variable-kind checks (the function-argument-type phase - {@code type(m)} needs a
   * relationship, {@code labels(m)} needs a node, and so on) used to be built as one end-state map for the
   * whole statement, applied to every expression in it wherever that expression sits. A name a later
   * {@code WITH} re-binds to something kindless (or to a different kind) lost its kind - or its wrongness -
   * for every clause <i>before</i> that {@code WITH} too, so a check that should have fired against the
   * clause as written silently didn't.
   * <p>
   * {@code MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m}: {@code type(m)} is written against a
   * {@code MATCH}-bound node, which is always a type error - {@code type()} needs a relationship - regardless
   * of what a later clause does with the name {@code m}. The old end-state map disagreed: by the time it was
   * built, {@code m} was whatever the last clause left it as ({@code WITH 1 AS m} makes it a SCALAR), so the
   * check read the wrong kind and never fired.
   * <p>
   * Now {@link com.arcadedb.query.opencypher.parser.CypherExpressionWalker} advances the variable-kind scope
   * clause by clause as it walks (issue #5671's {@code Visitor#forClauseEntry}), so a clause's own expressions
   * are checked against the scope as of that clause, not the statement's end state - both at the top level and
   * inside a {@code CALL { ... }} body, the shape the issue's own example uses.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5671PositionalVariableKinds {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/testissue5671positional").create();
      database.getSchema().createVertexType("P");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void typeOnANodeBeforeARebindingWithIsStillRejectedAtTopLevel() {
      // m is a node from MATCH; type() requires a relationship. The later "WITH 1 AS m" must not retroactively
      // excuse the type() call written against the node.
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    @Test
    void typeOnANodeBeforeARebindingWithIsStillRejectedInsideACallSubqueryBody() {
      // The issue's own example, executed: CALL { ... } bodies are validated positionally too, not just the
      // top-level statement.
      assertThatThrownBy(() -> database.query("opencypher",
          "CALL { MATCH (m:P) WHERE type(m) = 'KNOWS' WITH 1 AS m RETURN m } RETURN m"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    @Test
    void typeOnANodeIsRejectedEvenWithNoTrailingWithAtAll() {
      // Sanity check the pre-existing behavior (no WITH involved) is unaffected by the positional rework.
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WHERE type(m) = 'KNOWS' RETURN m"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    @Test
    void typeOnARelationshipCarriedThroughAWithIsStillAccepted() {
      // Sanity check the other direction: a kind that a WITH legitimately carries forward unchanged must not
      // start being rejected by the positional rework.
      database.getSchema().createEdgeType("KNOWS");
      try (ResultSet ignored = database.query("opencypher",
          "MATCH ()-[m:KNOWS]->() WITH m AS m RETURN type(m)")) {
        // Statement must parse and plan without a semantic error; an empty database yields zero rows, which is fine.
        assertThatCode(ignored::hasNext).doesNotThrowAnyException();
      }
    }

    @Test
    void typeOnANodeAfterAWithThatKeepsItANodeIsStillRejected() {
      // The check must still fire when the WITH does NOT change the kind - only a kind CHANGE/loss should shift
      // when the check does or doesn't fire.
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) WITH m AS m WHERE type(m) = 'KNOWS' RETURN m"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    /**
     * Found in code review of this issue's own fix: seeding a nested body's incremental walk from the body's
     * {@code buildVarTypes} <i>end state</i> (rather than from the truly-inherited outer scope) relocates the
     * exact contamination bug this issue is about to the body boundary instead of removing it. If the body's
     * first clause is a pass-through {@code WITH} of a name the outer query bound to one kind, and the SAME
     * name is later reassigned to a different kind further down in the body, the end-state-seeded walk would
     * read the reassigned kind for the {@code WITH}'s own pass-through - even though the true kind entering the
     * body, at that point, is still the one inherited from outside.
     */
    @Test
    void aNameReassignedLaterInABodyDoesNotContaminateAnEarlierPassThroughWithOfTheSameName() {
      database.getSchema().createEdgeType("KNOWS");
      // r is a relationship from the outer MATCH. Inside the body, the importing "WITH r" passes it through
      // unchanged and type(r) is checked right there - still genuinely a relationship at that point. Only later,
      // and only after an intervening WITH that does NOT carry r forward (dropping it from scope), does a fresh
      // MATCH reuse the name "r" for a node - a completely disconnected rebinding that must not reach backwards
      // and change what "r" meant at the WITH written above it. The body exports it as "p": a CALL subquery cannot return a
      // name the outer scope already binds (issue #11689).
      try (ResultSet ignored = database.query("opencypher",
          "MATCH ()-[r:KNOWS]->() "
              + "CALL { WITH r WHERE type(r) = 'KNOWS' WITH 1 AS x MATCH (r:P) RETURN r AS p, x } "
              + "RETURN r, p")) {
        assertThatCode(ignored::hasNext).doesNotThrowAnyException();
      }
    }

    /**
     * The query from the code-review comment that first raised this concern, kept as a named anchor. The column the body
     * returns is renamed, as a CALL subquery cannot return a name the outer scope already binds (issue #11689).
     */
    @Test
    void reviewCommentReproDoesNotFalselyReject() {
      database.getSchema().createEdgeType("KNOWS");
      try (ResultSet ignored = database.query("opencypher",
          "MATCH ()-[r:KNOWS]->() CALL { WITH r WHERE type(r) = 'KNOWS' WITH 1 AS r RETURN r AS s } RETURN r, s")) {
        assertThatCode(ignored::hasNext).doesNotThrowAnyException();
      }
    }

    /**
     * Found in code review: {@code UNWIND}'s own list expression (and, by the same reasoning, a procedure
     * {@code CALL}'s own arguments) is evaluated <i>before</i> the clause's binding takes effect, the same as a
     * {@code WITH}'s projection items - so it must be checked against the scope from before this clause, not the
     * one {@code forClauseEntry} advanced to. {@code m} is a node here; {@code UNWIND [...] AS m} reuses the name,
     * but {@code type(m)} inside the list is still the node from the {@code MATCH}, not yet rebound.
     */
    @Test
    void unwindListExpressionIsCheckedAgainstTheScopeBeforeItsOwnVariableIsRebound() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) UNWIND [type(m)] AS m RETURN m"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    /** Sanity check the other direction: an UNWIND that does not reuse an outer name is unaffected. */
    @Test
    void unwindListExpressionReferencingAnUnrelatedNodeIsUnaffected() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (m:P) UNWIND [type(m)] AS u RETURN u"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }

    /**
     * Found in a second code-review round: {@code FOREACH}'s inner clauses were walked flat, all against the
     * single scope {@code FOREACH} itself declared, so a later inner clause could not see what an earlier inner
     * clause in the SAME body had just bound - unlike every other clause list, where {@link #forClauseEntry}
     * advances the scope from one clause to the next. An inner {@code CREATE} followed by an inner {@code SET}
     * reading what it just created is the concrete case: {@code n} is a node the moment the {@code CREATE} runs,
     * and {@code type()} must still reject it inside the very next inner clause.
     */
    @Test
    void foreachInnerClauseSeesWhatAnEarlierInnerClauseInTheSameBodyJustDeclared() {
      assertThatThrownBy(() -> database.query("opencypher", "FOREACH (i IN [1] | CREATE (n:P) SET n.kind = type(n))"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("type() requires a relationship argument, got node");
    }
  }

  /**
   * Regression tests for issue #8104: {@code CypherSemanticValidator.validateVariableScope} skipped
   * {@code REMOVE} entirely, so a {@code REMOVE} naming a variable a preceding {@code WITH} had dropped -
   * or one that no clause ever bound - was accepted, removed nothing, and reported success instead of
   * raising {@code UndefinedVariable} the way Neo4j does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8104RemoveScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:Function {Name:'Zeta'})");
    }

    /** The reporter's first query: `f` is dropped by the projecting WITH, then referenced by REMOVE. */
    @Test
    void removePropertyOnDroppedVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          "MATCH (f:Function) WITH f.Name AS BusinessFunction REMOVE f.Name RETURN BusinessFunction").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /** The reporter's second query: `ghost` was never bound by anything. */
    @Test
    void removePropertyOnNeverBoundVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          "MATCH (f:Function) REMOVE ghost.Name RETURN f.Name AS BusinessFunction").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'ghost'");
    }

    /** Same shape as the label-removal form: REMOVE n:Label on a dropped variable. */
    @Test
    void removeLabelOnDroppedVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher",
          "MATCH (f:Function) WITH f.Name AS BusinessFunction REMOVE f:Function RETURN BusinessFunction").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * The dynamic-key form (REMOVE n[keyExpr]): the target variable `f` is in scope, but the key expression itself
     * references one that is not - the walk validateRemoveClauseScope gives item.getKeyExpression() (issue #8104).
     */
    @Test
    void removeDynamicPropertyKeyReferencingUnboundVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (f:Function) REMOVE f[missing]").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'missing'");
    }

    /**
     * The Cypher 25 dynamic-label form (REMOVE n:$(expr)): same shape, but for item.getLabelExpressions() instead of
     * the key expression.
     */
    @Test
    void removeDynamicLabelExpressionReferencingUnboundVariableThrows() {
      assertThatThrownBy(() -> database.command("opencypher", "MATCH (f:Function) REMOVE f:$(missing)").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'missing'");
    }

    /** Control: REMOVE on a variable that is genuinely in scope still works and takes effect. */
    @Test
    void removePropertyOnInScopeVariableSucceeds() {
      database.command("opencypher", "MATCH (f:Function) REMOVE f.Name").close();

      try (final ResultSet rs = database.query("opencypher", "MATCH (f:Function) RETURN f.Name AS name")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("name")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  /**
   * Issue #11005: a value projected from a property before a later clause writes that property must be the value the
   * property had when the projection ran, for every row, whatever the pull batch size.
   * <p>
   * Issue #11004: an empty FOREACH inside a {@code CALL (n) {..} IN TRANSACTIONS} must not change what the outer row reads
   * from a node written earlier in the same query.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11004And11005StaleProjection extends TestHelper {
    private static final String EAGER = "EAGER";

    @Test
    void projectionBeforeRemoveOfTheSameKeyKeepsTheValueForEveryRow() {
      final List<Object> snapshots = column("""
          CALL {
            CREATE (n:N {k: 42})
            UNWIND range(1, 101) AS duplicate
            RETURN n
          }
          WITH *, n.k AS snapshot
          REMOVE n.k
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(101).containsOnly(42L);
    }

    @Test
    void projectionBeforeRemoveWithRedundantWithStarIsUnchanged() {
      final List<Object> snapshots = column("""
          CALL {
            CREATE (n:N {k: 42})
            WITH *
            UNWIND range(1, 101) AS duplicate
            WITH *
            WITH *
            RETURN n
          }
          WITH *
          WITH *, n.k AS snapshot
          WITH *
          REMOVE n.k
          WITH *
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(101).containsOnly(42L);
    }

    @Test
    void projectionBeforeSetOfTheSameKeyOnARepeatedNodeKeepsTheValueForEveryRow() {
      database.command("opencypher", "CREATE (:M {k: 1})");
      final List<Object> snapshots = column("""
          MATCH (n:M)
          UNWIND range(1, 250) AS i
          WITH n, n.k AS before
          SET n.k = n.k + 1
          RETURN before""", "before");
      assertThat(snapshots).hasSize(250).containsOnly(1L);
      assertThat(column("MATCH (n:M) RETURN n.k AS k", "k")).containsExactly(251L);
    }

    @Test
    void emptyForeachInsideCallInTransactionsDoesNotStaleTheOuterRow() {
      database.command("opencypher", "CREATE ({k: 0})");
      final List<Object> current = column("""
          MATCH (n)
          SET n.k = 42
          CALL (n) {
            FOREACH (x IN [] | CREATE (:Never))
            RETURN 1 AS seen
          }
          IN TRANSACTIONS OF 1 ROWS
          RETURN n.k AS current""", "current");
      assertThat(current).containsExactly(42L);
      assertThat(column("MATCH (n) RETURN n.k AS stored", "stored")).containsExactly(42L);
      assertThat(column("MATCH (n:Never) RETURN count(n) AS c", "c")).containsExactly(0L);
    }

    @Test
    void callInTransactionsWithoutForeachReadsTheWrittenValue() {
      database.command("opencypher", "CREATE ({k: 0})");
      final List<Object> current = column("""
          MATCH (n)
          SET n.k = 42
          CALL (n) {
            RETURN 1 AS seen
          }
          IN TRANSACTIONS OF 1 ROWS
          RETURN n.k AS current""", "current");
      assertThat(current).containsExactly(42L);
    }

    @Test
    void projectionBeforeSetThroughAnotherVariableKeepsTheValueForEveryRow() {
      final List<Object> snapshots = column("""
          CREATE (b:B {k: 1})
          WITH b
          UNWIND range(1, 150) AS i
          CREATE (a:A {i: i})-[:R]->(b)
          WITH a, b AS target, b.k AS snapshot
          SET target.k = target.k + 1
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(150).containsOnly(1L);
      assertThat(column("MATCH (b:B) RETURN b.k AS k", "k")).containsExactly(151L);
    }

    @Test
    void projectionBeforeForeachSetKeepsTheValueForEveryRow() {
      final List<Object> snapshots = column("""
          CREATE (n:F {k: 7})
          WITH n
          UNWIND range(1, 120) AS i
          WITH n, n.k AS snapshot
          FOREACH (ignored IN [1] | SET n.k = null)
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(120).containsOnly(7L);
    }

    @Test
    void projectionBeforeAWritingSubqueryKeepsTheValueForEveryRow() {
      final List<Object> snapshots = column("""
          CREATE (n:S {k: 3})
          WITH n
          UNWIND range(1, 120) AS i
          WITH n, n.k AS snapshot
          CALL (n) { SET n.k = 0 }
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(120).containsOnly(3L);
    }

    @Test
    void projectionBeforeMergeOnMatchSetKeepsTheValueForEveryRow() {
      final List<Object> snapshots = column("""
          CREATE (g:G {id: 1, k: 5})
          WITH g
          UNWIND range(1, 120) AS i
          WITH g, g.k AS snapshot
          MERGE (m:G {id: 1}) ON MATCH SET m.k = 0
          RETURN snapshot""", "snapshot");
      assertThat(snapshots).hasSize(120).containsOnly(5L);
    }

    @Test
    void createNamingAMergedNodeDoesNotMakeItOnePerRow() {
      database.command("opencypher", "UNWIND range(1, 150) AS i CREATE (:X {i: i})");
      database.command("opencypher", "CREATE (:Q {id: 1, k: 0})");
      column("MATCH (x:X) MERGE (m:Q {id: 1}) CREATE (m)-[:R]->(:Z {v: m.k}) SET m.k = m.k + 1", "m");
      assertThat(column("MATCH (z:Z) RETURN z.v AS v", "v")).hasSize(150).containsOnly(0L);
      assertThat(column("MATCH (m:Q) RETURN m.k AS k", "k")).containsExactly(150L);
      // the merged node is the same on every row, so the SET waits for the CREATE of every row to read m.k first
      final String plan = explain("MATCH (x:X) MERGE (m:Q {id: 1}) CREATE (m)-[:R]->(:Z {v: m.k}) SET m.k = m.k + 1");
      assertThat(plan.substring(plan.indexOf("+ CREATE"))).contains(EAGER);
    }

    @Test
    void barrierIsPlannedWhereTheProjectionCanSeeAnotherRowsWrite() {
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b.k = 0 RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s REMOVE b.k RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (n:P) UNWIND [1, 2] AS i WITH n, n.k AS s SET n.k = 0 RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (n:P), (m:P) WITH n, m, m.k AS s SET n.k = 0 RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (n:P) WITH n, n.k AS s, n AS m SET m.k = 0 RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b += $props RETURN s")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, properties(b) AS p SET b.k = 0 RETURN p")).contains(EAGER);
    }

    @Test
    void barrierIsPlannedForEveryKindOfWrite() {
      // a MERGE action, and a write procedure, which can write any key
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s MERGE (m:Q {id: 1}) ON CREATE SET m.k = s RETURN s")).contains(EAGER);
      assertThat(explain("UNWIND range(1, 2) AS i CREATE (n:P {k: i}) WITH n, n.k AS s CALL merge.node(['Q'], {k: s}) YIELD node RETURN s"))
          .contains(EAGER);
      assertThat(explain("UNWIND range(1, 2) AS i CREATE (n:P {k: i}) WITH n CALL merge.node(['Q'], {k: 1}) YIELD node RETURN n"))
          .doesNotContain(EAGER);
      // the plan the optimizer builds: no WITH, the read is the right-hand side of an earlier SET
      assertThat(explain("MATCH (a)-[:R]->(b) SET a.x = b.k SET b.k = 0")).contains(EAGER);
      assertThat(explain("MATCH (n:P) SET n.x = n.k SET n.k = 0")).doesNotContain(EAGER);
      // a subquery that returns can hand on the same row several times
      assertThat(explain("MATCH (n:P) CALL (n) { RETURN 1 AS x UNION RETURN 2 AS x } WITH n, n.k AS s SET n.k = 0 RETURN s"))
          .contains(EAGER);
    }

    @Test
    void projectionThroughAVariableBoundOncePerRowKeepsStreaming() {
      assertThat(explain("MATCH (n:P) WITH n, n.k AS old SET n.k = old + 1 RETURN old")).doesNotContain(EAGER);
      assertThat(explain("MATCH (n:P) WITH n, n.k AS old REMOVE n.k RETURN old")).doesNotContain(EAGER);
      assertThat(explain("MATCH (n:P) WITH n WHERE n.k > 1 SET n.k = 0")).doesNotContain(EAGER);
      assertThat(explain("UNWIND range(1, 10) AS i CREATE (n:P {k: i}) WITH n, n.k AS s SET n.k = s * 2")).doesNotContain(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, count(*) AS c, b.k AS k SET b.k = k + c")).doesNotContain(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b.other = s RETURN s")).doesNotContain(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WITH b, b.k AS s SET b += {other: s} RETURN s")).doesNotContain(EAGER);
    }

    @Test
    void readsOfRowsThatCannotHoldARecordKeepStreaming() {
      assertThat(explain("UNWIND [{id: 1, v: 2}] AS row WITH row, row.id AS id MATCH (n:P {id: id}) SET n += row")).doesNotContain(EAGER);
      // a MATCH's own pattern and WHERE choose the rows, they project nothing into them
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
      // ... but a write that names a key the filter reads waits for the MATCH of every row (issue #11173)
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b.k = 2")).contains(EAGER);
    }

    private String explain(final String query) {
      try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<Object> column(final String query, final String column) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          final Object value = row.getProperty(column);
          values.add(value instanceof Number number ? (Object) number.longValue() : value);
        }
      }
      return values;
    }
  }
}
