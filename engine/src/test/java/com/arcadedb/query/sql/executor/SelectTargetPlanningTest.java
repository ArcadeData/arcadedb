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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for how a SELECT plans and reads its target: bound RID collections, target aliases and the index,
 * an index step restarted after a drop, correlated subqueries that scan, and the candidate row a filtered scan reuses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SelectTargetPlanningTest extends TestHelper {

  // ---- #5505: a bound collection of RIDs as the target ----
  /*
   * A collection of RIDs bound as a query parameter must be usable as a target for both SELECT and
   * TRAVERSE. Both planners used to build the per-element {@code Rid} AST node without flagging it as
   * legacy, so {@code Rid.toRecordId} dereferenced a null expression instead of reading bucket/position.
   * A singleton bound RID always worked, so the collection form is what the tests bind here.
   */
  /** Regression test for issue #5505. */
  @Test
  void selectFromBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM :seeds", Map.of("seeds", List.of(seeds.get(0), seeds.get(1))));

      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactlyInAnyOrder(seeds.get(0), seeds.get(1));
    });
  }

  /** Regression test for issue #5505. */
  @Test
  void traverseFromBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM (TRAVERSE out('" + EDGE + "') FROM :seeds MAXDEPTH 1)",
          Map.of("seeds", List.of(seeds.get(0), seeds.get(1))));

      // depth 0: the two seeds; depth 1: v1 (from v0) and v2 (from v1)
      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactlyInAnyOrder(seeds.get(0), seeds.get(1), seeds.get(2));
    });
  }

  /** Regression test for issue #5505. */
  @Test
  void selectFromSingleElementBoundRidCollection() {
    database.transaction(() -> {
      final List<RID> seeds = createChain();

      final ResultSet result = database.query("sql", "SELECT FROM :seeds", Map.of("seeds", List.of(seeds.getFirst())));

      final Set<RID> returned = collectRids(result);
      assertThat(returned).containsExactly(seeds.getFirst());
    });
  }

  private static final String VERTEX = "Issue5505V";
  private static final String EDGE    = "Issue5505E";

  /**
   * Builds v0 -> v1 -> v2 and returns their RIDs in order.
   */
  private List<RID> createChain() {
    database.getSchema().createVertexType(VERTEX);
    database.getSchema().createEdgeType(EDGE);

    final MutableVertex v0 = database.newVertex(VERTEX).set("name", "v0").save();
    final MutableVertex v1 = database.newVertex(VERTEX).set("name", "v1").save();
    final MutableVertex v2 = database.newVertex(VERTEX).set("name", "v2").save();
    v0.newEdge(EDGE, v1).save();
    v1.newEdge(EDGE, v2).save();

    return List.of(v0.getIdentity(), v1.getIdentity(), v2.getIdentity());
  }

  private static Set<RID> collectRids(final ResultSet result) {
    return result.stream().map(Result::getIdentity).map(Optional::orElseThrow).collect(Collectors.toSet());
  }

  // ---- #7153: a target alias keeps the index ----
  /*
   * Issue #7153, two faults that the reporter's query hit together.
   * <p>
   * A FROM target alias was parsed and thrown away, so {@code SELECT FROM Main m WHERE m.code = 'C1'} was planned as a
   * filter on a property literally named {@code m}: no index, a full scan, and no rows at all.
   * <p>
   * And an index search whose leftover condition read a per-record LET variable spliced the LET steps in AHEAD of the
   * index fetch, producing a plan whose first step had nothing to pull from ("Cannot execute a local LET on a query
   * without a target"). Those leftovers now stay in the WHERE clause, which is chained after the LET.
   */
  private void targetAliasSchema() {
    database.command("sql", "CREATE VERTEX TYPE Main");
    database.command("sql", "CREATE PROPERTY Main.code STRING");
    database.command("sql", "CREATE INDEX ON Main (code) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Data");
    database.command("sql", "CREATE PROPERTY Data.code STRING");
    database.command("sql", "CREATE EDGE TYPE relation");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("sql", "CREATE VERTEX Main SET code = 'C" + i + "'");

      database.command("sql", "CREATE VERTEX Data SET code = 'D1'");
      database.command("sql",
          "CREATE EDGE relation FROM (SELECT FROM Main WHERE code = 'C1') TO (SELECT FROM Data WHERE code = 'D1')");
    });
  }

  /** Regression test for issue #7153. */
  @Test
  void aliasQualifiedFilterUsesTheIndex() {
    targetAliasSchema();
    for (final String query : new String[] { //
        "SELECT FROM Main m WHERE m.code = 'C1'", //
        "SELECT FROM Main AS m WHERE m.code = 'C1'" }) {
      try (final ResultSet rs = database.query("sql", query)) {
        assertThat(plan(rs)).as(query).contains("FETCH FROM INDEX Main[code]");
        assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
        assertThat(rs.hasNext()).as(query).isFalse();
      }
    }
  }

  /** Regression test for issue #7153. */
  @Test
  void aliasResolvesInEveryClause() {
    targetAliasSchema();
    try (final ResultSet rs = database.query("sql",
        "SELECT m.code AS c FROM Main m LET $upper = m.code.toUpperCase() WHERE m.code = 'C1' ORDER BY m.code")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("c")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // A bare alias stands for the record itself, whether it is projected or a record attribute is read off it
    try (final ResultSet rs = database.query("sql", "SELECT m FROM Main m WHERE m.@rid IS NOT NULL AND m.code = 'C2'")) {
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * {@code m} inside a sub-query is NOT the outer alias: the sub-query has its own target, and the outer record is
   * reachable only through {@code $parent}. Stripping the outer alias there would silently re-point the reference at
   * the inner record.
   */
  @Test
  void aliasIsNotInheritedByASubQuery() {
    targetAliasSchema();
    // Data carries a property literally named like the outer alias, so the two readings are distinguishable: inside
    // the sub-query 'm' must stay that property and not become the sub-query's own record.
    database.transaction(() -> database.command("sql", "UPDATE Data SET m = 'a-property-not-an-alias'"));

    assertThat(rendered("SELECT code FROM Main m WHERE m.code IN (SELECT m FROM Data)"))//
        .contains("SELECT m FROM Data").doesNotContain("SELECT @this FROM Data");

    try (final ResultSet rs = database.query("sql",
        "SELECT m AS inner FROM Data WHERE code IN (SELECT code FROM Main m WHERE m.code = 'C1')")) {
      assertThat(rs.hasNext()).isFalse(); // Data has no 'code' equal to a Main code - the point is that it parses and runs
    }

    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main m WHERE m.code IN (SELECT code FROM Data WHERE code = 'D1') OR m.code = 'C3'")) {
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C3");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #7153. */
  @Test
  void aliasWorksOnUpdateAndDelete() {
    targetAliasSchema();
    database.transaction(() -> {
      database.command("sql", "UPDATE Main m SET touched = true WHERE m.code = 'C4'");
      try (final ResultSet rs = database.query("sql", "SELECT FROM Main WHERE code = 'C4'")) {
        assertThat(rs.next().<Boolean>getProperty("touched")).isTrue();
      }

      // the assignment target too: SET m.touched must set the record's own property, not one nested inside a
      // property named after the alias
      database.command("sql", "UPDATE Main m SET m.stamped = 'yes' WHERE m.code = 'C4'");
      try (final ResultSet rs = database.query("sql", "SELECT stamped FROM Main WHERE code = 'C4'")) {
        assertThat(rs.next().<String>getProperty("stamped")).isEqualTo("yes");
      }

      database.command("sql", "DELETE FROM Main m WHERE m.code = 'C4'");
      try (final ResultSet rs = database.query("sql", "SELECT FROM Main WHERE code = 'C4'")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * The rewrite rebuilds the modifier chain by hand, so every shape that is not {@code alias.property} - a method
   * call, an array selector, {@code .*} - has to keep applying to the record the alias stands for.
   */
  @Test
  void aBareAliasCarriesItsModifierChainOntoTheRecord() {
    targetAliasSchema();
    // a method call applies to the record the alias stands for
    try (final ResultSet rs = database.query("sql", "SELECT m.asJSON() AS j FROM Main m WHERE m.code = 'C1'")) {
      assertThat(rs.next().<Object>getProperty("j").toString()).contains("\"code\":\"C1\"");
      assertThat(rs.hasNext()).isFalse();
    }

    // and an array selector or ".*" keeps its place in the chain, now applied to the record. A string-keyed
    // selector reads null off a record whatever it is applied to - @this, a $variable - so the shape is pinned on
    // the statement rather than on a result that would say nothing about the rewrite.
    assertThat(rendered("SELECT m.* FROM Main m")).contains("@this.*").doesNotContain("m.*");
    assertThat(rendered("SELECT m[0] FROM Main m")).contains("@this[0]");
    assertThat(rendered("SELECT m.asJSON() FROM Main m")).contains("@this.asJSON()");
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * The reported query: the index on {@code Main.code} must survive the second AND term, which can only be decided
   * once the per-record LET has run.
   */
  @Test
  void indexSurvivesAConditionOnAPerRecordLetVariable() {
    targetAliasSchema();
    final String query = "SELECT FROM Main m LET $relation = out('relation') "//
        + "WHERE m.code = 'C1' AND $relation CONTAINS (code = 'D1')";

    try (final ResultSet rs = database.query("sql", query)) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      assertThat(plan).doesNotContain("FETCH FROM BUCKET");
      // the LET must be chained BEFORE the filter that reads the variable it defines. Both markers are asserted
      // present first: indexOf() answers -1 for a missing step, and -1 is less than anything.
      assertThat(plan).contains("LET (for each record)").contains("FILTER ITEMS WHERE");
      assertThat(plan.indexOf("LET (for each record)")).isLessThan(plan.indexOf("FILTER ITEMS WHERE"));

      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // and the deferred filter really filters: C2 has no relation, so the same shape must answer nothing
    try (final ResultSet rs = database.query("sql", "SELECT FROM Main LET $relation = out('relation') "//
        + "WHERE code = 'C2' AND $relation CONTAINS (code = 'D1')")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * OR branches never reach the deferral above: {@code handleTypeAsTargetWithIndexedFunction} claims a multi-block
   * WHERE first and plans one indexed sub-plan per branch, each chaining the LET ahead of its own residual filter -
   * correctly, because that sub-plan is not empty when {@code handleLet()} is handed it. Pinned here because the
   * deferral must not disturb it: there is one residual per branch and a single WHERE clause to defer into, so this
   * path has to keep filtering inside the sub-plans.
   */
  @Test
  void everyOrBranchKeepsItsIndexAndItsOwnLet() {
    targetAliasSchema();
    final String query = "SELECT code FROM Main LET $relation = out('relation') "//
        + "WHERE (code = 'C1' AND $relation.size() = 1) OR (code = 'C2' AND $relation.size() = 0)";

    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(plan(rs)).contains("FETCH FROM INDEX Main[code]").doesNotContain("FETCH FROM BUCKET");
      assertThat(rs.stream().map(r -> r.<String>getProperty("code"))).containsExactlyInAnyOrder("C1", "C2");
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * A type with no records of its own is answered by one indexed sub-plan per sub-type. Each sub-plan would need its
   * own deferred residual and there is a single WHERE clause to hold one, so the statement gives the index up for a
   * plain scan, which evaluates the filter after the LET by construction. Before issue #7153 this shape built a plan
   * whose index fetch pulled from a LET step that had no source, and raised at the first row.
   */
  @Test
  void perRecordLetResidualOnASubTypeFallsBackToTheScan() {
    targetAliasSchema();
    database.command("sql", "CREATE VERTEX TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.tag STRING");
    database.command("sql", "CREATE VERTEX TYPE Sub1 EXTENDS Base");
    database.command("sql", "CREATE VERTEX TYPE Sub2 EXTENDS Base");
    database.command("sql", "CREATE INDEX ON Sub1 (tag) UNIQUE");
    database.command("sql", "CREATE INDEX ON Sub2 (tag) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.newVertex("Sub1").set("tag", "A" + i).save();
        database.newVertex("Sub2").set("tag", "B" + i).save();
      }
    });

    try (final ResultSet rs = database.query("sql",
        "SELECT tag FROM Base b LET $relation = out('relation') WHERE b.tag = 'A1' AND $relation.size() >= 0")) {
      assertThat(plan(rs)).contains("FETCH FROM TYPE Base");
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("A1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * {@code FromItem.toString()} renders the alias back ({@code Main AS m}), and the hardwired {@code count(*)} plan
   * used to hand that rendering to {@code CountFromTypeStep}, which looks its argument up in the schema.
   */
  @Test
  void countStarOnAnAliasedTargetCountsTheType() {
    targetAliasSchema();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Main m")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(50L);
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * A nested TRAVERSE or MATCH gets its own alias scope: the enclosing SELECT's alias must not rewrite an identifier
   * that means something else inside them - a property of the TRAVERSE target's own rows, or a MATCH pattern alias.
   * <p>
   * Asserted on the statement the parser built, because that is where the rewrite happens and what it did is visible
   * there without depending on what the nested statement then computes: an inherited alias would have turned the
   * nested {@code m} into {@code @this}.
   */
  @Test
  void aNestedStatementDoesNotInheritTheAlias() {
    targetAliasSchema();
    assertThat(rendered("SELECT FROM Main m LET $t = (TRAVERSE m FROM Data) WHERE m.code = 'C1'"))//
        .contains("TRAVERSE m FROM Data").doesNotContain("TRAVERSE @this");

    assertThat(rendered("SELECT FROM Main m LET $x = (MATCH {type: Data, as: m} RETURN m.code AS mc) WHERE m.code = 'C1'"))//
        .contains("m.code AS mc").doesNotContain("@this");

    // and the enclosing statement's own references are still resolved
    assertThat(rendered("SELECT FROM Main m LET $t = (TRAVERSE m FROM Data) WHERE m.code = 'C1'"))//
        .contains("WHERE code = 'C1'");
  }

  private String rendered(final String query) {
    return ((SQLQueryEngine) database.getQueryEngine("sql")).parse(query, (DatabaseInternal) database).toString();
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * A dollar sign inside a string literal is not a LET reference. Matching a bare {@code "$"} would defer the
   * residual - or, on the sub-type path, give the index up altogether - for a condition no LET can influence.
   */
  @Test
  void aDollarInAStringLiteralIsNotALetReference() {
    targetAliasSchema();
    database.transaction(() -> database.command("sql", "UPDATE Main SET currency = 'US$' WHERE code = 'C1'"));

    final String query = "SELECT code FROM Main m LET $relation = out('relation') WHERE m.code = 'C1' AND currency = 'US$'";
    try (final ResultSet rs = database.query("sql", query)) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      // the residual mentions no LET variable, so it is filtered inside the fetch plan, ahead of the LET step
      assertThat(plan.indexOf("FILTER ITEMS WHERE")).isLessThan(plan.indexOf("LET (for each record)"));
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * The same {@code '$'}-in-a-literal false positive gated two other optimisations, both of which skip when the
   * WHERE clause "references a LET": the predicate pushed into the type scan, and the vertex-centric lookup that
   * answers {@code WHERE @out = <RID>} off the vertex's edge list instead of scanning the edge type. Neither was
   * ever a wrong answer - the condition just fell through to the post-LET filter - but both are lost for a reason
   * that has nothing to do with the LET.
   */
  @Test
  void aDollarInAStringLiteralKeepsThePushdownAndTheEdgeLookup() {
    targetAliasSchema();
    database.transaction(() -> database.command("sql", "UPDATE Main SET currency = 'US$' WHERE code = 'C1'"));

    // predicate pushdown: an unindexed condition is filtered inside the scan, not by a step chained after it
    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main LET $relation = out('relation') WHERE currency = 'US$'")) {
      assertThat(plan(rs)).contains("FETCH FROM TYPE Main WITH FILTER");
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // vertex-centric edge lookup
    final String mainRid = database.query("sql", "SELECT @rid AS r FROM Main WHERE code = 'C1'").next()
        .<Object>getProperty("r").toString();
    try (final ResultSet rs = database.query("sql", "SELECT @rid FROM relation LET $x = in() "//
        + "WHERE @out = " + mainRid + " AND label = 'US$'")) {
      assertThat(plan(rs)).doesNotContain("FETCH FROM TYPE relation");
      assertThat(rs.hasNext()).isFalse(); // no edge carries that label - the point is which plan answered
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * The grammar takes a LET name as a plain identifier, so {@code LET brain = out(...)} declares the variable that
   * {@code $brain} reads and only the reference carries the sigil. Matching the two spellings against each other is
   * what tells the planner this residual has to wait for the LET.
   */
  @Test
  void aLetDeclaredWithoutTheSigilIsStillRecognised() {
    targetAliasSchema();
    final String query = "SELECT code FROM Main m LET relation = out('relation') "//
        + "WHERE m.code = 'C1' AND $relation CONTAINS (code = 'D1')";

    try (final ResultSet rs = database.query("sql", query)) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      assertThat(plan).contains("LET (for each record)").contains("FILTER ITEMS WHERE");
      assertThat(plan.indexOf("LET (for each record)")).isLessThan(plan.indexOf("FILTER ITEMS WHERE"));
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * An OR whose branches are answered by an indexed function - {@code search_index()} here - is planned as one
   * sub-plan per branch. That branch chained its residual filter without the LET guard every sibling branch
   * applies, so a residual reading a per-record LET was evaluated with the variable unset and answered nothing.
   */
  @Test
  void anIndexedFunctionOrBranchWaitsForThePerRecordLet() {
    targetAliasSchema();
    database.command("sql", "CREATE PROPERTY Main.notes STRING");
    database.command("sql", "CREATE INDEX ON Main (notes) FULL_TEXT");
    database.transaction(() -> {
      database.command("sql", "UPDATE Main SET notes = 'linked to data' WHERE code = 'C1'");
      database.command("sql", "UPDATE Main SET notes = 'nothing here' WHERE code = 'C2'");
    });

    try (final ResultSet rs = database.query("sql", "SELECT code FROM Main LET $relation = out('relation') "//
        + "WHERE (search_index('Main[notes]', 'linked') = true AND $relation.size() = 1) "//
        + "OR (search_index('Main[notes]', 'nothing') = true AND $relation.size() = 0)")) {
      assertThat(rs.stream().map(r -> r.<String>getProperty("code"))).containsExactlyInAnyOrder("C1", "C2");
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * A quoted run is data. {@code WHERE note = '$relation'} spells a LET variable's name inside a string literal and
   * names no variable at all, and a backtick-quoted {@code `$relation`} is a property whose name merely starts with
   * the character.
   */
  @Test
  void aVariableNameInsideAQuotedRunIsNotAReference() {
    targetAliasSchema();
    database.transaction(() -> database.command("sql", "UPDATE Main SET note = '$relation' WHERE code = 'C1'"));

    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main LET $relation = out('relation') WHERE note = '$relation'")) {
      // the residual reads no variable, so it is pushed into the scan rather than deferred past the LET
      assertThat(plan(rs)).contains("FETCH FROM TYPE Main WITH FILTER");
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // and the index survives the same literal next to an indexed condition
    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main m LET $relation = out('relation') WHERE m.code = 'C1' AND note = '$relation'")) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      assertThat(plan).contains("FILTER ITEMS WHERE").contains("LET (for each record)");
      assertThat(plan.indexOf("FILTER ITEMS WHERE")).isLessThan(plan.indexOf("LET (for each record)"));
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }

    // a backtick-quoted identifier is a property name, even when it opens with the sigil
    database.transaction(() -> database.command("sql", "UPDATE Main SET `$relation` = 'a property' WHERE code = 'C1'"));

    try (final ResultSet rs = database.query("sql",
        "SELECT code FROM Main m LET $relation = out('relation') WHERE m.code = 'C1' AND `$relation` = 'a property'")) {
      final String plan = plan(rs);
      assertThat(plan).contains("FETCH FROM INDEX Main[code]");
      assertThat(plan).contains("FILTER ITEMS WHERE").contains("LET (for each record)");
      assertThat(plan.indexOf("FILTER ITEMS WHERE")).isLessThan(plan.indexOf("LET (for each record)"));
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * A target that is not a type name is resolved out of its rendered text, and an alias is not part of that text.
   * The contract is asserted on {@code FromItem} directly because the one planner branch that resolves a dotted
   * variable path this way cannot be reached from SQL today - a bare identifier carrying a modifier is read as a
   * type name first - while an aliased {@code $var} target, which can, has to keep working.
   */
  @Test
  void anAliasIsNotPartOfTheTargetsName() {
    targetAliasSchema();
    final FromItem target = ((SelectStatement) ((SQLQueryEngine) database.getQueryEngine("sql"))//
        .parse("SELECT FROM Main m", (DatabaseInternal) database)).getTarget().getItem();

    assertThat(target.toString()).isEqualTo("Main AS m");
    assertThat(target.toStringWithoutAlias()).isEqualTo("Main");

    try (final ResultSet rs = database.command("sqlscript",
        "LET $chosen = (SELECT FROM Main WHERE code = 'C1'); SELECT code FROM $chosen[0] AS c")) {
      assertThat(rs.next().<String>getProperty("code")).isEqualTo("C1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #7153.
   * <p>
   * When a column is given the same name as the target, ORDER BY and GROUP BY mean the column - that is what SQL
   * says, and what the sort step does anyway, since it resolves a bare name against the projected row. Rewriting
   * the name as the target would sort by the whole record instead, silently and in no useful order.
   */
  @Test
  void anOutputColumnOutranksTheTargetAliasInOrderBy() {
    targetAliasSchema();
    // DESC over the whole type, so that the answer differs from the order the rows are stored and read in: sorting
    // by the record instead of by the column leaves them exactly as the scan produced them, C0 first
    try (final ResultSet rs = database.query("sql", "SELECT code AS m FROM Main m ORDER BY m DESC")) {
      assertThat(rs.next().<String>getProperty("m")).isEqualTo("C9");
    }

    try (final ResultSet rs = database.query("sql", "SELECT code AS m, count(*) AS c FROM Main m "//
        + "WHERE m.code = 'C1' GROUP BY m")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("m")).isEqualTo("C1");
      assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }

    // a nested SELECT inside the ORDER BY must not take the output-column scope away from the items after it
    assertThat(rendered("SELECT code AS m FROM Main m ORDER BY (SELECT 1), m DESC"))//
        .contains(", m DESC").doesNotContain("@this");

    // a qualified reference still reads the alias as the target: a column needs no qualifier
    try (final ResultSet rs = database.query("sql", "SELECT code AS m FROM Main m WHERE m.code < 'C11' ORDER BY m.code DESC")) {
      assertThat(rs.stream().map(r -> r.<String>getProperty("m"))).containsExactly("C10", "C1", "C0");
    }
  }

  private static String plan(final ResultSet rs) {
    return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
  }

  // ---- #9331: an index step restarted after a DROP INDEX ----
  /*
   * Issue #9331, the restart path: a point lookup that overlapped a commit starts again
   * ({@code GetValueFromIndexEntryStep.restartLookup}), which resets the index step, and a reset step resolves its index again
   * by name. When a concurrent {@code DROP INDEX} took the index away in between, both reads of the name raised the
   * {@code SchemaException} "Index with name ... was not found", which the re-planning of the statement
   * ({@code Statement.openResultSetPlanningAgainOnStaleIndex}) does not catch, so a read-only SELECT failed instead of being
   * planned again (seen once in ~600k statements of {@code Issue9331Issue9332IndexDdlDuringReadsTest}).
   */
  /** Regression test for issue #9331. */
  @Test
  void anIndexStepRestartedAfterItsIndexWasDroppedAsksForANewPlan() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id LONG");
    database.command("sql", "CREATE INDEX ON Person (id) NOTUNIQUE");
    database.transaction(() -> database.newDocument("Person").set("id", 7L).save());

    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE id = 7")) {
      final FetchFromIndexStep step = findIndexStep(rs.getExecutionPlan().orElseThrow().getSteps());
      assertThat(step).as("the plan reads the index").isNotNull();
      assertThat(step.indexFileIds()).isNotEmpty();

      // What restartLookup does after a commit overlapped the lookup, and the drop that lands right then
      step.reset();
      database.command("sql", "DROP INDEX `Person[id]`");

      // Nothing is left to unpin of a dropped index, and the restarted read asks for a new plan
      assertThat(step.indexFileIds()).isEmpty();
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      assertThatThrownBy(() -> step.syncPull(context, 10)).isInstanceOf(IndexException.class);
    }

    // Planned again, the statement answers by a scan
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE id = 7")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  private static FetchFromIndexStep findIndexStep(final List<ExecutionStep> steps) {
    if (steps == null)
      return null;
    for (final ExecutionStep step : steps) {
      if (step instanceof FetchFromIndexStep indexStep)
        return indexStep;
      final FetchFromIndexStep nested = findIndexStep(step.getSubSteps());
      if (nested != null)
        return nested;
    }
    return null;
  }

  // ---- #10609: a correlated subquery scans on its own thread ----
  /*
   * Issue #10609: a subquery that reads {@code $parent.$current} and is run once per row of the enclosing scan used to scan in
   * parallel itself, so its workers read the very record the enclosing thread was holding, from several threads at once. A record
   * is not shared between threads (its binary content has a read position), and the workers logged thousands of
   * {@code Possible corrupted record} errors and compared rows against nothing: the answer of the statement depended on the
   * timing of the threads. A statement run under a row of an enclosing statement scans on its own thread.
   */
  private static final int ROWS = 2_000;

  /** Regression test for issue #10609. */
  @Test
  void aCorrelatedSubqueryCountsTheSameRowsWhicheverThreadsRunIt() {
    database.getSchema().createDocumentType("Doc", 2);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Doc").set("n", i).save();
    });

    // rows whose residue modulo 7 is shared by `perResidue` rows, brute-forced here
    final int[] members = new int[7];
    for (int i = 0; i < ROWS; i++)
      members[i % 7]++;
    final int target = members[0];
    int expected = 0;
    for (int residue = 0; residue < 7; residue++)
      if (members[residue] == target)
        expected += members[residue];

    try (final var rs = database.query("sql",
        "SELECT count(*) AS c FROM Doc WHERE (SELECT count(*) AS c FROM Doc d WHERE d.n % 7 = $parent.$current.n % 7)[0].c = ?",
        target)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) expected);
    }
  }

  /** Regression test for issue #10609. */
  @Test
  void aStatementRunUnderARowOfAnEnclosingStatementDoesNotScanInParallel() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final BasicCommandContext outer = new BasicCommandContext();
    outer.setDatabase(db);
    assertThat(ParallelTypeScan.isAllowed(outer)).as("a statement of its own").isTrue();

    final BasicCommandContext inner = new BasicCommandContext();
    inner.setDatabase(db);
    inner.setParent(outer);
    assertThat(ParallelTypeScan.isAllowed(inner)).as("a FROM-subquery or a LET of the whole statement: no row yet").isTrue();

    outer.setVariable("current", new ResultInternal());
    assertThat(ParallelTypeScan.isAllowed(inner)).as("run once per row of the enclosing scan").isFalse();

    final BasicCommandContext deeper = new BasicCommandContext();
    deeper.setDatabase(db);
    deeper.setParent(inner);
    assertThat(ParallelTypeScan.isAllowed(deeper)).as("a row anywhere up the chain").isFalse();
  }

  // ---- #10655: a filtered scan reuses a rejected candidate ----
  /*
   * Issue #10655 item 2: a filtered scan used to build a new {@link ResultInternal} for every record it read, before the
   * WHERE clause had looked at it, so a row the clause rejected cost a Result that nothing ever used. A rejected
   * candidate is now pointed at the next record instead, and only a row the clause accepts leaves the scan as its own
   * object.
   */
  private static final String TYPE    = "Scan10655";
  private static final String PROBE   = "probe10655";
  private static final int    RECORDS = 600;
  // EVERY 50TH RECORD MATCHES: grp = 0
  private static final int    GROUPS  = 50;
  private static final int    MATCHES = RECORDS / GROUPS;

  private void scanCandidateRecords() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1);
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("grp", i % GROUPS).set("tag", "a,b," + i).save();
    });
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * The WHERE clause sees one candidate object for every run of rejected records, not one per record: the probe records
   * the {@code $current} each evaluation ran against, and the scan may only allocate a new one after it handed the
   * previous one downstream as a match (plus one per unit of a parallel scan).
   */
  @Test
  void rejectedCandidatesShareOneRow() {
    scanCandidateRecords();
    final Map<Object, Boolean> seen = Collections.synchronizedMap(new IdentityHashMap<>());
    final List<RID> evaluated = Collections.synchronizedList(new ArrayList<>());
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(PROBE) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        final Object current = context.getVariable("current");
        seen.put(current, Boolean.TRUE);
        evaluated.add(((Result) current).getIdentity().get());
        return true;
      }

      @Override
      public String getSyntax() {
        return PROBE + "()";
      }
    });
    try {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE " + PROBE + "() = true AND grp = 0")) {
        while (rs.hasNext())
          rows.add(rs.next());
      }

      assertThat(rows).hasSize(MATCHES);
      assertThat(evaluated).as("the WHERE clause ran once per record, each time on its own record").hasSize(RECORDS);
      assertThat(new HashSet<>(evaluated)).hasSize(RECORDS);
      // ONE CANDIDATE PER MATCH HANDED DOWNSTREAM, PLUS THE ONE THE TRAILING REJECTED RUN SHARES; BEFORE THE FIX: ONE PER RECORD
      assertThat(seen.size()).as("candidate rows the WHERE clause saw for %d records", RECORDS).isLessThanOrEqualTo(MATCHES + 1);
    } finally {
      sql.getFunctionFactory().unregister(PROBE);
    }
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * A row the clause accepted is never the object the scan reuses: after every row has been read, each one still answers
   * its own record, though the scan rejected dozens of records between two matches.
   */
  @Test
  void acceptedRowsKeepTheirOwnRecord() {
    scanCandidateRecords();
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE grp = 7")) {
      while (rs.hasNext())
        rows.add(rs.next());
    }

    assertThat(rows).hasSize(MATCHES);
    final Set<Integer> ids = new HashSet<>();
    final Map<Result, Boolean> distinct = new IdentityHashMap<>();
    for (final Result row : rows) {
      assertThat((Integer) row.getProperty("grp")).isEqualTo(7);
      final Document element = row.getElement().get();
      assertThat(element.getInteger("grp")).isEqualTo(7);
      ids.add(row.getProperty("id"));
      distinct.put(row, Boolean.TRUE);
    }
    assertThat(ids).hasSize(MATCHES);
    assertThat(distinct).hasSize(MATCHES);
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * {@code $current} and a method call on a field read the record under evaluation, not one the candidate showed earlier.
   */
  @Test
  void currentResolvesToTheRecordUnderEvaluation() {
    scanCandidateRecords();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE $current.grp = 3")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo((long) MATCHES);
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + TYPE + " WHERE tag.split(',')[2] = '123'")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(123);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * A row something wrote to is never pointed at another record: what was written belongs to the record it was written on.
   */
  @Test
  void aWrittenRowIsNotRewrapped() {
    scanCandidateRecords();
    database.transaction(() -> {
      final List<Document> docs = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE id < 2 ORDER BY id")) {
        while (rs.hasNext())
          docs.add(rs.next().getElement().get());
      }

      final ResultInternal plain = new ResultInternal(docs.get(0));
      assertThat(plain.rewrapElement(docs.get(1))).isTrue();
      assertThat(plain.getElement().get().getIdentity()).isEqualTo(docs.get(1).getIdentity());

      final ResultInternal withMetadata = new ResultInternal(docs.get(0));
      withMetadata.setMetadata("score", 1);
      assertThat(withMetadata.rewrapElement(docs.get(1))).isFalse();
      assertThat(withMetadata.getElement().get().getIdentity()).isEqualTo(docs.get(0).getIdentity());

      final ResultInternal withTemporary = new ResultInternal(docs.get(0));
      withTemporary.setTemporaryProperty("t", 1);
      assertThat(withTemporary.rewrapElement(docs.get(1))).isFalse();

      final ResultInternal withRemoval = new ResultInternal(docs.get(0));
      withRemoval.removeProperty("grp");
      assertThat(withRemoval.rewrapElement(docs.get(1))).isFalse();

      final ResultInternal withContent = new ResultInternal(database);
      withContent.setElement(docs.get(0));
      assertThat(withContent.rewrapElement(docs.get(1))).isFalse();
    });
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * A correlated subquery in the WHERE clause reads the outer row through {@code $parent.$current}, and runs a filtered
   * scan of its own, with its own reused candidate, while the outer candidate is being evaluated.
   */
  @Test
  void correlatedSubqueryReadsTheOuterRecordUnderEvaluation() {
    scanCandidateRecords();
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT id FROM " + TYPE + " WHERE id IN (SELECT id FROM " + TYPE + " WHERE id = $parent.$current.id AND grp = 5) ORDER BY id")) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    final List<Integer> expected = new ArrayList<>();
    for (int i = 5; i < RECORDS; i += GROUPS)
      expected.add(i);
    assertThat(ids).isEqualTo(expected);
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * A parallel filtered scan runs one copy of the step per unit of pages, each on one worker at a time, so each copy keeps
   * its own rejected candidate: the rows it returns are the sequential scan's, each answering its own record.
   */
  @Test
  void parallelScanReturnsTheSequentialRowsEachWithItsOwnRecord() {
    scanCandidateRecords();
    final Object pagesPerUnit = database.getConfiguration().getValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    try {
      scanInParallel();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, pagesPerUnit);
    }
  }

  private void scanInParallel() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Par10655", 1);
      final String padding = "x".repeat(200);
      for (int i = 0; i < 20_000; i++)
        database.newDocument("Par10655").set("id", i).set("grp", i % GROUPS).set("pad", padding).save();
    });
    final String query = "SELECT FROM Par10655 WHERE grp = 9";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("(parallel)");
    }

    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    final List<Integer> ids = new ArrayList<>();
    final Map<Result, Boolean> distinct = new IdentityHashMap<>();
    for (final Result row : rows) {
      assertThat(row.getElement().get().getInteger("grp")).isEqualTo(9);
      ids.add(row.getElement().get().getInteger("id"));
      distinct.put(row, Boolean.TRUE);
    }
    final List<Integer> expected = new ArrayList<>();
    for (int i = 9; i < 20_000; i += GROUPS)
      expected.add(i);
    assertThat(ids).isEqualTo(expected);
    assertThat(distinct).hasSize(expected.size());
  }

  /**
   * Regression test for issue #10655.
   * <p>
   * Every field of a row besides its element is checked by {@code rewrapElement}: a field added later that carries row
   * state and is not checked would be moved to another record by a filtered scan. Each field is set on its own, to a
   * non-null value of its type, and the row must then refuse to be re-pointed.
   */
  @Test
  void everyFieldBesidesTheElementBlocksTheRewrap() throws Exception {
    scanCandidateRecords();
    final DocumentType type = database.getSchema().getType(TYPE);
    final Document first;
    final Document second;
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE id < 2 ORDER BY id")) {
      first = rs.next().getElement().get();
      second = rs.next().getElement().get();
    }

    int checked = 0;
    for (Class<?> c = ResultInternal.class; c != Object.class; c = c.getSuperclass()) {
      for (final Field field : c.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.getName().equals("element"))
          continue;
        final ResultInternal row = new ResultInternal(first);
        field.setAccessible(true);
        field.set(row, sampleOf(field.getType(), type));
        assertThat(row.rewrapElement(second)).as("a row whose '%s' is set must not be re-pointed", field.getName()).isFalse();
        ++checked;
      }
    }
    assertThat(checked).isGreaterThanOrEqualTo(9);
  }

  private Object sampleOf(final Class<?> fieldType, final DocumentType type) {
    if (fieldType == Map.class)
      return new HashMap<>();
    if (fieldType == Set.class)
      return new HashSet<>();
    if (fieldType == DocumentType.class)
      return type;
    if (fieldType.isInstance(database))
      return database;
    if (fieldType == Object.class)
      return "value";
    throw new AssertionError("no sample value for a field of type " + fieldType.getName() + ": extend sampleOf() and rewrapElement()");
  }
}
