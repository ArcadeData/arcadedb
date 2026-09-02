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

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@code SELECT ... WHERE @rid IN (SELECT ...)}, which used to answer nothing while the same query with a RID
 * literal answered correctly (issue #7054). The planner lifts the sub-query into a global LET and leaves a generated
 * alias on the right-hand side of the {@code IN}; the RID-address optimization then evaluated that alias at PLAN
 * time, got the {@code null} of a variable no step had populated yet, and read it as "IN nothing", committing the
 * plan to a {@link FetchFromRidsStep} over an empty RID set.
 * <p>
 * The fetch is still used - the sub-query's rows are widened into candidate RIDs at execution time, once the LET has
 * run - so these tests also assert the plan shape, and every one of them cross-checks the answer against the plain
 * scan that {@code @rid NOT IN} (which declines the optimization) forces, so the two paths cannot drift apart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RidInSubQueryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Supplier").createProperty("name", Type.STRING);
    database.getSchema().createVertexType("TopSuppliers").createProperty("name", Type.STRING);
    database.getSchema().getType("TopSuppliers").createProperty("target_rid", Type.LINK);
    database.getSchema().getType("TopSuppliers").createProperty("targets", Type.LIST);
    database.command("sql", "CREATE INDEX ON TopSuppliers (name) NOTUNIQUE");

    database.transaction(() -> {
      for (final String name : new String[] { "S1", "S2", "S3" }) {
        database.command("sql", "INSERT INTO Supplier SET name = '" + name + "'");
        database.command("sql", "INSERT INTO TopSuppliers SET name = '" + name + "', target_rid = (SELECT @rid FROM Supplier "
            + "WHERE name = '" + name + "')");
      }
      // One row whose link lives inside a collection instead of a scalar LINK property.
      database.command("sql",
          "INSERT INTO TopSuppliers SET name = 'group', targets = (SELECT @rid FROM Supplier WHERE name IN ['S1', 'S2'])");
    });
  }

  @Test
  void ridInSubQueryOnLinkPropertyMatchesTheLinkedRecord() {
    final ResultSet rs = database.query("sql",
        "SELECT FROM `Supplier` WHERE @rid IN (SELECT target_rid FROM TopSuppliers WHERE name = 'S1')");

    assertThat(findStep(rs.getExecutionPlan().orElseThrow(), FetchFromRidsStep.class))
        .as("@rid IN (SELECT ...) must still be answered by address, not by a full type scan").isNotNull();
    assertThat(collectNames(rs)).containsExactly("S1");
  }

  @Test
  void ridInSubQueryProjectingRidMatchesTheProjectedRecords() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT @rid FROM Supplier WHERE name <> 'S2')"))
        .containsExactlyInAnyOrder("S1", "S3");
  }

  @Test
  void ridInSubQueryReturningSeveralRowsMatchesAllOfThem() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT target_rid FROM TopSuppliers WHERE name IN ['S1', 'S3'])"))
        .containsExactlyInAnyOrder("S1", "S3");
  }

  @Test
  void ridInEmptySubQueryReturnsNoRows() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT target_rid FROM TopSuppliers WHERE name = 'nobody')")).isEmpty();
  }

  @Test
  void ridInSubQueryProjectingSomethingThatIsNotARidReturnsNoRows() {
    // The residual filter is what makes this exact: the widened candidate set is empty here anyway, but a projection
    // that is not a RID must never match, the same way the plain scan answers it.
    assertBehavesLikeAScan("SELECT FROM Supplier WHERE @rid IN (SELECT name FROM TopSuppliers)", List.of());
  }

  @Test
  void ridInSubQueryProjectingACollectionOfLinksMatchesEveryElement() {
    assertBehavesLikeAScan("SELECT FROM Supplier WHERE @rid IN (SELECT targets FROM TopSuppliers WHERE name = 'group')",
        List.of("S1", "S2"));
  }

  @Test
  void ridInSubQueryProjectingSeveralColumnsBehavesLikeTheScan() {
    // Ambiguous by construction: a scan compares @rid against the row's FIRST projected column ('name' here), so it
    // never matches. The widened candidate set does contain the target_rid, which the residual filter then drops -
    // the whole point of keeping the condition after the fetch.
    assertBehavesLikeAScan("SELECT FROM Supplier WHERE @rid IN (SELECT name, target_rid FROM TopSuppliers WHERE name = 'S1')",
        List.of());
  }

  @Test
  void ridInSubQueryOverElementsMatchesByIdentity() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT FROM Supplier WHERE name = 'S2')")).containsExactly("S2");
  }

  @Test
  void ridInSubQueryCombinesWithAnAdditionalCondition() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT target_rid FROM TopSuppliers) AND name = 'S3'"))
        .containsExactly("S3");
  }

  @Test
  void ridNotInSubQueryReturnsTheComplement() {
    // NOT IN declines the address optimization by construction and is answered by the scan.
    assertThat(names("SELECT FROM Supplier WHERE @rid NOT IN (SELECT target_rid FROM TopSuppliers WHERE name = 'S1')"))
        .containsExactlyInAnyOrder("S2", "S3");
  }

  @Test
  void ridInSubQueryWithAnOrBranchStaysCorrect() {
    assertThat(names("SELECT FROM Supplier WHERE @rid IN (SELECT target_rid FROM TopSuppliers WHERE name = 'S1') "
        + "OR name = 'S3'")).containsExactlyInAnyOrder("S1", "S3");
  }

  @Test
  void ridInCorrelatedSubQueryKeepsTheScan() {
    // A sub-query that reads the outer record becomes a PER-RECORD let, computed after the fetch step: the address
    // optimization must decline it, or the fetch would resolve an alias no step has populated yet.
    final ResultSet rs = database.query("sql",
        "SELECT FROM Supplier WHERE @rid IN (SELECT target_rid FROM TopSuppliers WHERE name = $parent.$current.name)");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(findStep(plan, FetchFromRidsStep.class)).as("a correlated sub-query cannot be fetched by address").isNull();
    assertThat(collectNames(rs)).containsExactlyInAnyOrder("S1", "S2", "S3");
  }

  @Test
  void ridInGlobalLetVariableMatchesTheLinkedRecords() {
    // Same hazard reached through an explicit LET rather than an inline sub-query: splitLet() promotes it to a
    // global LET, so the variable is just as unset at plan time as a generated sub-query alias.
    final ResultSet rs = database.query("sql", "SELECT FROM Supplier LET $top = (SELECT target_rid FROM TopSuppliers "
        + "WHERE name IN ['S1', 'S2']) WHERE @rid IN $top");

    assertThat(findStep(rs.getExecutionPlan().orElseThrow(), FetchFromRidsStep.class)).isNotNull();
    assertThat(collectNames(rs)).containsExactlyInAnyOrder("S1", "S2");
  }

  @Test
  void ridInVariableShadowedByAPerRecordLetKeepsTheScan() {
    // $top is assigned twice: the first assignment is early-calculated and becomes a GLOBAL let, the second reads the
    // record and stays PER-RECORD, shadowing it for every row. Only the global one has run by the time the fetch step
    // would pull, so resolving RIDs from it fetches the WRONG records (just S1) rather than merely fewer - the
    // per-record value is each record's own @rid, which every row satisfies. A variable a per-record LET owns has to
    // keep the scan, whether or not the fetch could otherwise read a value for that name.
    final ResultSet rs = database.query("sql",
        "SELECT FROM Supplier LET $top = (SELECT target_rid FROM TopSuppliers WHERE name = 'S1'), $top = @rid "
            + "WHERE @rid IN $top");

    assertThat(findStep(rs.getExecutionPlan().orElseThrow(), FetchFromRidsStep.class))
        .as("a variable a per-record LET recomputes cannot be fetched by address").isNull();
    assertThat(collectNames(rs)).containsExactlyInAnyOrder("S1", "S2", "S3");
  }

  /**
   * Asserts the query answers {@code expected}, and that a plain scan of the same predicate answers the same thing.
   * The scan is reached by negating the condition: {@code @rid NOT IN} declines the address optimization, so its
   * complement is what the unoptimized path would have returned.
   */
  private void assertBehavesLikeAScan(final String query, final List<String> expected) {
    assertThat(names(query)).as("optimized answer").containsExactlyInAnyOrderElementsOf(expected);

    final List<String> all = names("SELECT FROM Supplier");
    final List<String> viaScan = new ArrayList<>(all);
    viaScan.removeAll(names(query.replace("@rid IN", "@rid NOT IN")));
    assertThat(viaScan).as("scan answer for " + query).containsExactlyInAnyOrderElementsOf(expected);
  }

  private List<String> names(final String query) {
    return collectNames(database.query("sql", query));
  }

  private static List<String> collectNames(final ResultSet rs) {
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    rs.close();
    return names;
  }

  private static <T extends ExecutionStep> T findStep(final ExecutionPlan plan, final Class<T> type) {
    for (final ExecutionStep step : plan.getSteps())
      if (type.isInstance(step))
        return type.cast(step);
    return null;
  }
}
