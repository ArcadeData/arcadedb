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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #7169: inserting a no-op {@code OPTIONAL MATCH (:NoSuchLabel) WHERE false}
 * barrier plus a scope-preserving {@code WITH} into a query changed its answer.
 * <p>
 * The reported divergence - an {@code OPTIONAL MATCH} written straight after a {@code MERGE} bound {@code null}
 * where the barriered form bound the relationship the MERGE had just created - is the clause-scoped relationship
 * uniqueness bug of issue #7165, and the first test here pins the reporter's own pair against it. Cypher scopes
 * relationship uniqueness to a single clause, so the {@code OPTIONAL MATCH} may bind the very edge the
 * {@code MERGE} bound: the barriered form was the correct one.
 * <p>
 * Reproducing that pair exposed a second, independent defect in the same query, which the rest of this class
 * covers: {@code MERGE p = ...} did not bind {@code p} to the path it merged. It bound a flat list of only the
 * elements the pattern happened to <i>name</i>, so an anonymous hop simply disappeared - the reporter's
 * three-node, two-relationship pattern reported {@code length(p0) = 1}, and a pattern that names nothing at all
 * bound an empty path. MATCH and CREATE have always bound a real path over every element of the pattern,
 * named or not, and MERGE now does the same.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergePathBindingIssue7169Test extends TestHelper {

  private static final String BARRIER = "OPTIONAL MATCH (__optional_probe:NoSuchLabel) WHERE false";

  /**
   * The reporter's pair, reduced to the two clauses that carried the divergence. Both forms must report the
   * relationship the MERGE bound: the {@code OPTIONAL MATCH} is a clause of its own, so {@code r0} is not one of
   * <i>its</i> relationships and the uniqueness rule has nothing to reject.
   */
  @Test
  void anOptionalMatchAfterMergeBindsTheMergedRelationshipWithOrWithoutABarrier() {
    final String withoutBarrier = """
        MERGE p0 = (n0:l5:l3 {id: 128})<-[r0:rt3 {klist: ["9v6OzPkCL"]}]-(:l3:l8:l5 {id: 129})<-[:rt9 {id: 398}]-(:l3:l8 {id: 130})
        OPTIONAL MATCH (:l3&l5&l8)-[r1:rt3|rt5 {klist: ["9v6OzPkCL"]}]->(n0)
        RETURN r1.klist[0] AS bound, length(p0) AS len""";
    final String withBarrier = """
        MERGE p0 = (n0:l5:l3 {id: 128})<-[r0:rt3 {klist: ["9v6OzPkCL"]}]-(:l3:l8:l5 {id: 129})<-[:rt9 {id: 398}]-(:l3:l8 {id: 130})
        """ + BARRIER + """

        WITH n0, r0, p0
        """ + BARRIER + """

        WITH n0, r0, p0
        OPTIONAL MATCH (:l3&l5&l8)-[r1:rt3|rt5 {klist: ["9v6OzPkCL"]}]->(n0)
        RETURN r1.klist[0] AS bound, length(p0) AS len""";

    final List<Result> plain = command(withoutBarrier);
    wipe();
    final List<Result> barriered = command(withBarrier);

    assertThat(plain).hasSize(1);
    assertThat(barriered).hasSize(1);
    assertThat((Object) plain.get(0).getProperty("bound"))
        .as("the OPTIONAL MATCH is a clause of its own, so the edge the MERGE bound is one it may bind")
        .isEqualTo("9v6OzPkCL");
    assertThat((Object) barriered.get(0).getProperty("bound")).isEqualTo(plain.get(0).getProperty("bound"));
    assertThat(((Number) plain.get(0).getProperty("len")).intValue())
        .as("the merged pattern is two relationships long, the second one being anonymous")
        .isEqualTo(2);
    assertThat((Object) barriered.get(0).getProperty("len")).isEqualTo(plain.get(0).getProperty("len"));
  }

  /**
   * The reporter's path variable. Two relationships were written, so the path is two long - the second hop being
   * anonymous does not remove it from the path any more than an anonymous node removes a node.
   */
  @Test
  void aMergedPathSpansEveryHopEvenWhenTheHopIsAnonymous() {
    final String query = """
        MERGE p = (a:A {id: 1})<-[r:R1 {t: 'x'}]-(b:B {id: 2})<-[:R2 {t: 'y'}]-(c:C {id: 3})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""";

    assertPathShape(query, 2, 3, 2);
    // ...and identically on the second run, which takes the match branch rather than the create branch.
    assertPathShape(query, 2, 3, 2);
  }

  /** Nothing in the pattern is named, so the old implementation had nothing to list and bound an empty path. */
  @Test
  void aMergedPathIsBoundEvenWhenThePatternNamesNothing() {
    final String query = """
        MERGE p = (:A {id: 21})-[:R1 {t: 'z'}]->(:B {id: 22})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""";

    assertPathShape(query, 1, 2, 1);
    assertPathShape(query, 1, 2, 1);
  }

  /**
   * MERGE reaches an existing path through three different walkers - a full scan when no endpoint is bound, an
   * index seek over the unbound endpoint of a single-relationship pattern, and an outward walk from a bound
   * anchor - and each one has to describe the path it found the same way.
   */
  @Test
  void everyMatchBranchDescribesTheFoundPathTheSameWay() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE A IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY A.id IF NOT EXISTS INTEGER");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON A (id) UNIQUE");
    });
    database.transaction(() -> database.command("opencypher",
        "CREATE (:A {id: 31})-[:R1 {t: 'q'}]->(:B {id: 32})-[:R2 {t: 'w'}]->(:C {id: 33})"));

    // full scan: no endpoint is bound and the pattern spans two relationships
    assertPathShape("""
        MERGE p = (:A {id: 31})-[:R1 {t: 'q'}]->(:B {id: 32})-[:R2 {t: 'w'}]->(:C {id: 33})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""", 2, 3, 2);

    // index seek: a single relationship whose unbound endpoint carries an indexed property
    assertPathShape("""
        MATCH (b:B {id: 32})
        MERGE p = (:A {id: 31})-[:R1 {t: 'q'}]->(b)
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""", 1, 2, 1);

    // anchor walk: the bound anchor sits in the middle, so the walk runs right then left
    assertPathShape("""
        MATCH (b:B {id: 32})
        MERGE p = (:A {id: 31})-[:R1 {t: 'q'}]->(b)-[:R2 {t: 'w'}]->(:C {id: 33})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""", 2, 3, 2);
  }

  /** A single-node MERGE binds a zero-length path over that node, on both the create and the match branch. */
  @Test
  void aSingleNodeMergeBindsAZeroLengthPath() {
    final String named = """
        MERGE p = (a:A {id: 41})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""";
    assertPathShape(named, 0, 1, 0);
    assertPathShape(named, 0, 1, 0);

    final String anonymous = """
        MERGE p = (:A {id: 42})
        RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount""";
    assertPathShape(anonymous, 0, 1, 0);
    assertPathShape(anonymous, 0, 1, 0);
  }

  /**
   * The real assertion behind all of the above: a path is a path whichever clause bound it. MERGE's create
   * branch, MERGE's match branch, CREATE and MATCH must all describe the same three-node pattern identically -
   * which is what stops MERGE from drifting into a shape of its own again.
   */
  @Test
  void mergeMatchAndCreateDescribeTheSamePathIdentically() {
    final String projection =
        "RETURN length(p) AS len, size(nodes(p)) AS nodeCount, size(relationships(p)) AS relCount, p AS p";

    final Result created = single("CREATE p = (:A {id: 51})<-[:R1 {t: 'c'}]-(:B {id: 52})<-[:R2 {t: 'd'}]-(:C {id: 53}) " + projection);
    final Result merged = single("MERGE p = (:A {id: 51})<-[:R1 {t: 'c'}]-(:B {id: 52})<-[:R2 {t: 'd'}]-(:C {id: 53}) " + projection);
    final Result matched = single("MATCH p = (:A {id: 51})<-[:R1 {t: 'c'}]-(:B {id: 52})<-[:R2 {t: 'd'}]-(:C {id: 53}) " + projection);

    for (final String column : new String[] { "len", "nodeCount", "relCount" }) {
      assertThat((Object) merged.getProperty(column)).as(column).isEqualTo(created.getProperty(column));
      assertThat((Object) matched.getProperty(column)).as(column).isEqualTo(created.getProperty(column));
    }
    assertThat(merged.<Object>getProperty("p"))
        .as("MERGE must bind the same kind of path value MATCH and CREATE bind, not a bare list")
        .hasSameClassAs(matched.<Object>getProperty("p"));
  }

  private void assertPathShape(final String query, final int length, final int nodeCount, final int relCount) {
    final Result row = single(query);
    assertThat(((Number) row.getProperty("len")).intValue()).as("length(p) of: %s", query).isEqualTo(length);
    assertThat(((Number) row.getProperty("nodeCount")).intValue()).as("nodes(p) of: %s", query).isEqualTo(nodeCount);
    assertThat(((Number) row.getProperty("relCount")).intValue()).as("relationships(p) of: %s", query).isEqualTo(relCount);
  }

  private Result single(final String query) {
    final List<Result> rows = command(query);
    assertThat(rows).as("one row from: %s", query).hasSize(1);
    return rows.get(0);
  }

  private List<Result> command(final String query) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
    });
    return rows;
  }

  /** Empties the graph so the two halves of a left/right comparison start from the same state. */
  private void wipe() {
    database.transaction(() -> {
      for (final String type : new String[] { "rt3", "rt9", "l3", "l5", "l8" })
        if (database.getSchema().existsType(type))
          database.command("sql", "DELETE FROM " + type);
    });
  }
}
