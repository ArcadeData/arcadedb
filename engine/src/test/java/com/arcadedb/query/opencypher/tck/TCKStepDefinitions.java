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
package com.arcadedb.query.opencypher.tck;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cucumber step definitions for the OpenCypher TCK.
 */
public class TCKStepDefinitions {

  private static final AtomicInteger DB_COUNTER = new AtomicInteger();
  private static final Set<String> EXPECTED_FAILURES = loadExpectedFailures();

  private Database database;
  private Map<String, Object> parameters;
  private List<Map<String, Object>> queryResults;
  private List<String> resultColumns;
  private Exception queryException;
  private TCKSideEffectChecker sideEffectChecker;
  private String scenarioName;

  @Before
  public void setUp(final Scenario scenario) {
    scenarioName = scenario.getName();

    // Skip expected failures via JUnit assumption
    if (isExpectedFailure(scenarioName))
      assumeTrue(false, "Expected failure: " + scenarioName);

    parameters = new HashMap<>();
    queryResults = null;
    resultColumns = null;
    queryException = null;
    sideEffectChecker = new TCKSideEffectChecker();
  }

  @After
  public void tearDown() {
    if (database != null) {
      try {
        database.drop();
      } catch (final Exception ignored) {
      }
      database = null;
    }
  }

  // ---- Given steps ----

  @Given("an empty graph")
  public void anEmptyGraph() {
    createFreshDatabase();
  }

  @Given("any graph")
  public void anyGraph() {
    createFreshDatabase();
  }

  @Given("the binary-tree-1 graph")
  public void theBinaryTree1Graph() {
    createFreshDatabase();
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:A {name: 'a'}), (b1:X {name: 'b1'}), (b2:X {name: 'b2'}), "
              + "(b3:X {name: 'b3'}), (b4:X {name: 'b4'}), "
              + "(c11:X {name: 'c11'}), (c12:X {name: 'c12'}), "
              + "(c21:X {name: 'c21'}), (c22:X {name: 'c22'}), "
              + "(c31:X {name: 'c31'}), (c32:X {name: 'c32'}), "
              + "(c41:X {name: 'c41'}), (c42:X {name: 'c42'}) "
              + "CREATE (a)-[:KNOWS]->(b1), (a)-[:KNOWS]->(b2), "
              + "(a)-[:FOLLOWS]->(b3), (a)-[:FOLLOWS]->(b4) "
              + "CREATE (b1)-[:FRIEND]->(c11), (b1)-[:FRIEND]->(c12), "
              + "(b2)-[:FRIEND]->(c21), (b2)-[:FRIEND]->(c22), "
              + "(b3)-[:FRIEND]->(c31), (b3)-[:FRIEND]->(c32), "
              + "(b4)-[:FRIEND]->(c41), (b4)-[:FRIEND]->(c42) "
              + "CREATE (b1)-[:FRIEND]->(b2), (b2)-[:FRIEND]->(b3), "
              + "(b3)-[:FRIEND]->(b4), (b4)-[:FRIEND]->(b1)");
    });
  }

  @Given("the binary-tree-2 graph")
  public void theBinaryTree2Graph() {
    createFreshDatabase();
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:A {name: 'a'}), (b1:X {name: 'b1'}), (b2:X {name: 'b2'}), "
              + "(b3:X {name: 'b3'}), (b4:X {name: 'b4'}), "
              + "(c11:X {name: 'c11'}), (c12:Y {name: 'c12'}), "
              + "(c21:X {name: 'c21'}), (c22:Y {name: 'c22'}), "
              + "(c31:X {name: 'c31'}), (c32:Y {name: 'c32'}), "
              + "(c41:X {name: 'c41'}), (c42:Y {name: 'c42'}) "
              + "CREATE (a)-[:KNOWS]->(b1), (a)-[:KNOWS]->(b2), "
              + "(a)-[:FOLLOWS]->(b3), (a)-[:FOLLOWS]->(b4) "
              + "CREATE (b1)-[:FRIEND]->(c11), (b1)-[:FRIEND]->(c12), "
              + "(b2)-[:FRIEND]->(c21), (b2)-[:FRIEND]->(c22), "
              + "(b3)-[:FRIEND]->(c31), (b3)-[:FRIEND]->(c32), "
              + "(b4)-[:FRIEND]->(c41), (b4)-[:FRIEND]->(c42) "
              + "CREATE (b1)-[:FRIEND]->(b2), (b2)-[:FRIEND]->(b3), "
              + "(b3)-[:FRIEND]->(b4), (b4)-[:FRIEND]->(b1)");
    });
  }

  // ---- And steps (setup) ----

  @And("having executed:")
  public void havingExecuted(final String query) {
    database.transaction(() -> {
      try {
        database.command("opencypher", query.trim());
      } catch (final Exception e) {
        throw new RuntimeException("Setup query failed: " + query, e);
      }
    });
  }

  @And("parameters are:")
  public void parametersAre(final DataTable dataTable) {
    parameters = new HashMap<>();
    for (final List<String> row : dataTable.asLists()) {
      if (row.size() >= 2) {
        final String key = row.get(0).trim();
        final Object value = TCKValueParser.parse(row.get(1).trim());
        parameters.put(key, value);
      }
    }
  }

  @Given("^there exists a procedure .+$")
  public void thereExistsAProcedure(final DataTable dataTable) {
    // TCK procedure support - ArcadeDB does not support custom procedures
    assumeTrue(false, "Custom procedures not supported");
  }

  // ---- When steps ----

  @When("executing query:")
  public void executingQuery(final String query) {
    executeQueryInternal(query.trim());
  }

  @When("executing control query:")
  public void executingControlQuery(final String query) {
    // Control query is a follow-up verification query
    executeQueryInternal(query.trim());
  }

  private void executeQueryInternal(final String query) {
    queryResults = new ArrayList<>();
    resultColumns = new ArrayList<>();
    queryException = null;

    // Take snapshot before query for side-effect checking
    sideEffectChecker.snapshot(database);

    try {
      database.transaction(() -> {
        final ResultSet rs;
        if (parameters != null && !parameters.isEmpty())
          rs = database.command("opencypher", query, parameters);
        else
          rs = database.command("opencypher", query);

        // Collect column names from first result if available
        boolean columnsCollected = false;
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (!columnsCollected) {
            resultColumns = new ArrayList<>(row.getPropertyNames());
            columnsCollected = true;
          }
          queryResults.add(TCKResultMatcher.resultToMap(row, resultColumns));
        }
      });
    } catch (final Exception e) {
      queryException = e;
      queryResults = null;
    }
  }

  // ---- Then steps (result assertions) ----

  @Then("the result should be, in any order:")
  public void theResultShouldBeInAnyOrder(final DataTable dataTable) {
    assertNoQueryException();
    final List<String> columns = dataTable.row(0);
    final List<Map<String, Object>> expected = parseExpectedTable(dataTable);
    ensureColumnsMatch(columns);
    TCKResultMatcher.assertResultsUnordered(queryResults, expected, columns, false);
  }

  @Then("the result should be, in order:")
  public void theResultShouldBeInOrder(final DataTable dataTable) {
    assertNoQueryException();
    final List<String> columns = dataTable.row(0);
    final List<Map<String, Object>> expected = parseExpectedTable(dataTable);
    ensureColumnsMatch(columns);
    TCKResultMatcher.assertResultsOrdered(queryResults, expected, columns, false);
  }

  @Then("the result should be \\(ignoring element order for lists):")
  public void theResultShouldBeIgnoringListOrder(final DataTable dataTable) {
    assertNoQueryException();
    final List<String> columns = dataTable.row(0);
    final List<Map<String, Object>> expected = parseExpectedTable(dataTable);
    ensureColumnsMatch(columns);
    TCKResultMatcher.assertResultsUnordered(queryResults, expected, columns, true);
  }

  @Then("the result should be, in order \\(ignoring element order for lists):")
  public void theResultShouldBeInOrderIgnoringListOrder(final DataTable dataTable) {
    assertNoQueryException();
    final List<String> columns = dataTable.row(0);
    final List<Map<String, Object>> expected = parseExpectedTable(dataTable);
    ensureColumnsMatch(columns);
    TCKResultMatcher.assertResultsOrdered(queryResults, expected, columns, true);
  }

  @Then("the result should be empty")
  public void theResultShouldBeEmpty() {
    assertNoQueryException();
    assertThat(queryResults).as("Expected empty result set").isEmpty();
  }

  @Then("^a (\\w+) should be raised at compile time: (.+)$")
  public void errorAtCompileTime(final String errorType, final String detail) {
    assertErrorRaised(errorType, detail);
  }

  @Then("^a (\\w+) should be raised at runtime: (.+)$")
  public void errorAtRuntime(final String errorType, final String detail) {
    assertErrorRaised(errorType, detail);
  }

  @Then("^a (\\w+) should be raised at any time: (.+)$")
  public void errorAtAnyTime(final String errorType, final String detail) {
    assertErrorRaised(errorType, detail);
  }

  // ---- And steps (side effects) ----

  @And("no side effects")
  public void noSideEffects() {
    if (queryException != null)
      return; // Don't check side effects if query errored
    sideEffectChecker.assertNoSideEffects(database);
  }

  @And("the side effects should be:")
  public void theSideEffectsShouldBe(final DataTable dataTable) {
    if (queryException != null)
      return;
    final Map<String, Integer> effects = new LinkedHashMap<>();
    for (final List<String> row : dataTable.asLists()) {
      if (row.size() >= 2)
        effects.put(row.get(0).trim(), Integer.parseInt(row.get(1).trim()));
    }
    sideEffectChecker.assertSideEffects(database, effects);
  }

  // ---- Helper methods ----

  private void createFreshDatabase() {
    if (database != null) {
      try {
        database.drop();
      } catch (final Exception ignored) {
      }
    }
    final String dbPath = "./target/databases/tck-" + DB_COUNTER.incrementAndGet();
    final DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  private List<Map<String, Object>> parseExpectedTable(final DataTable dataTable) {
    final List<String> headers = dataTable.row(0);
    final List<Map<String, Object>> rows = new ArrayList<>();

    for (int i = 1; i < dataTable.height(); i++) {
      final List<String> row = dataTable.row(i);
      final Map<String, Object> map = new LinkedHashMap<>();
      for (int j = 0; j < headers.size(); j++) {
        final String cellValue = j < row.size() ? row.get(j) : "";
        map.put(headers.get(j), TCKValueParser.parse(cellValue));
      }
      rows.add(map);
    }
    return rows;
  }

  private void ensureColumnsMatch(final List<String> expectedColumns) {
    if (queryResults == null || queryResults.isEmpty()) {
      // If we have no results, columns from the query may not be available
      // Set them from the expected table for comparison
      if (resultColumns == null || resultColumns.isEmpty())
        resultColumns = new ArrayList<>(expectedColumns);
      return;
    }
    // Verify all expected columns exist in results
    for (final String col : expectedColumns)
      assertThat(resultColumns).as("Missing column in results: " + col).contains(col);
  }

  private void assertNoQueryException() {
    if (queryException != null)
      fail("Query should have succeeded but threw: " + queryException.getMessage(), queryException);
    assertThat(queryResults).as("No results collected").isNotNull();
  }

  private void assertErrorRaised(final String errorType, final String detail) {
    assertThat(queryException).as("Expected error " + errorType + "/" + detail + " but query succeeded. Results: " + queryResults).isNotNull();
  }

  private boolean isExpectedFailure(final String name) {
    if (name == null)
      return false;
    for (final String pattern : EXPECTED_FAILURES) {
      if (name.contains(pattern) || pattern.equals(name))
        return true;
    }
    return false;
  }

  private static Set<String> loadExpectedFailures() {
    final Set<String> failures = new HashSet<>();
    try (final InputStream is = TCKStepDefinitions.class.getResourceAsStream("/opencypher/tck/expected-failures.txt")) {
      if (is == null)
        return failures;
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (!line.isEmpty() && !line.startsWith("#"))
            failures.add(line);
        }
      }
    } catch (final IOException e) {
      System.err.println("Warning: Could not load expected-failures.txt: " + e.getMessage());
    }
    return failures;
  }
}
