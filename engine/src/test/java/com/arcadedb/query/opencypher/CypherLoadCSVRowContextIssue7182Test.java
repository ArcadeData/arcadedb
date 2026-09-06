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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7182: a query that rebuilt each LOAD CSV row through a map, {@code collect} and {@code UNWIND} got
 * {@code null} from {@code file()}, where the same query without the round trip got the CSV URL.
 * <p>
 * <b>That much is not a bug, and this test pins it as the answer.</b> {@code file()} and {@code linenumber()} are
 * functions of the row, and an aggregating clause does not carry a row through - it folds many into one. Neo4j,
 * the openCypher reference implementation, reads both functions off a {@code ResourceLinenumber} stamped on the
 * row by {@code LoadCSVPipe}, and its aggregation tables build their output row with {@code QueryState.newRow},
 * which starts from the query's initial row and so carries no stamp. {@code file()} is therefore {@code null}
 * after {@code WITH collect(...)} in Neo4j too, and {@code null} for the rest of the query, because the
 * {@code UNWIND} that expands the list back out copies a row that never had the context. Preserving it would also
 * have to answer what {@code linenumber()} means for one row folded out of three, and there is no answer.
 * <p>
 * <b>What the report did surface is one clause where the context was lost and Neo4j keeps it:</b> {@code WITH}
 * with an {@code ORDER BY}. That form defers SKIP/LIMIT and emits a merged scope for the sort to read, which a
 * {@code VariableProjectionStep} then strips back to the projected variables - and the strip took the row context
 * with it. So {@code WITH row ORDER BY row RETURN file()} answered {@code null} while {@code WITH row RETURN
 * file()} answered the file, and {@code RETURN file() ORDER BY ...} answered the file as well: three spellings of
 * the same thing, one of them disagreeing. Neo4j sorts the rows it was handed rather than building new ones, so
 * nothing is lost across its sort.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLoadCSVRowContextIssue7182Test {
  private Database database;
  private String   url;

  @BeforeEach
  void setUp() throws IOException {
    final File csv = new File("./target/databases/cypher-loadcsv-7182/arcade-load.csv");
    csv.getParentFile().mkdirs();
    try (final PrintWriter writer = new PrintWriter(csv, "UTF-8")) {
      writer.println("id,name");
      writer.println("load_1,Loaded Alice");
      writer.println("load_2,Loaded Bob");
    }
    url = csv.getAbsolutePath();

    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-loadcsv-7182/db");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // The bug the report uncovered: ORDER BY on a WITH was the one projection form that lost the context
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void theRowContextSurvivesAWithThatCarriesAnOrderBy() {
    // The control: the same projection without the ORDER BY has answered correctly since issue #6402.
    assertThat(rows("WITH row RETURN file() AS r")).hasSize(3).allMatch(url::equals);

    assertThat(rows("WITH row ORDER BY linenumber() RETURN file() AS r")).hasSize(3).allMatch(url::equals);
    assertThat(rows("WITH row ORDER BY row RETURN file() AS r")).hasSize(3).allMatch(url::equals);
    // A second WITH after the sorted one must still see it: the context has to be on the row, not merely
    // left behind on the command context by whatever was evaluated last.
    assertThat(rows("WITH row ORDER BY row WITH row RETURN file() AS r")).hasSize(3).allMatch(url::equals);
  }

  @Test
  void theLineNumberFollowsTheRowAcrossTheSort() {
    // Not just "non-null": the value has to travel with its own row, so a descending sort has to hand back the
    // line numbers in descending order rather than three copies of whichever row was evaluated last.
    assertThat(rows("WITH row ORDER BY linenumber() DESC RETURN linenumber() AS r")).containsExactly(3, 2, 1);
    assertThat(rows("WITH row ORDER BY linenumber() RETURN linenumber() AS r")).containsExactly(1, 2, 3);
    assertThat(rows("WITH row ORDER BY linenumber() DESC LIMIT 2 RETURN linenumber() AS r")).containsExactly(3, 2);
    assertThat(rows("WITH row ORDER BY linenumber() DESC SKIP 1 RETURN linenumber() AS r")).containsExactly(2, 1);
  }

  @Test
  void theRowContextIsStillNotProjectedByReturnStarAfterASort() {
    // It rides through the strip as execution state, so it must not surface as a column of its own (issue #5444).
    try (final ResultSet resultSet = database.query("opencypher", load("WITH row ORDER BY row RETURN *"))) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        assertThat(result.getPropertyNames()).containsExactly("row");
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // The report's own two queries: the difference between them is Neo4j's, and stays
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void theReportedLeftQueryAnswersTheFile() {
    // The report's own query, quoted as it was filed.
    assertThat(query("""
        CALL () { RETURN ['x'] AS carrier }
        WITH carrier LIMIT 1
        LOAD CSV FROM '%s' AS row FIELDTERMINATOR ','
        RETURN row, file() AS r, carrier
        """.formatted(url))).hasSize(3).allMatch(url::equals);
  }

  @Test
  void theReportedRightQueryAnswersNullBecauseAggregationEndsTheRowContext() {
    // The report's second query, quoted as it was filed. Neo4j answers null here too: its aggregation table
    // builds the output row with QueryState.newRow, which carries no ResourceLinenumber, and the UNWIND
    // downstream copies that contextless row. Asserted rather than fixed, so that a later change to the
    // carry-over has to come past this test on purpose.
    assertThat(query("""
        CALL () { RETURN ['x'] AS carrier }
        WITH carrier LIMIT 1
        LOAD CSV FROM '%s' AS row FIELDTERMINATOR ','
        WITH {carrier: carrier, row: row} AS row1
        WITH collect(row1) AS rows1
        UNWIND rows1 AS row1
        WITH row1.carrier AS carrier, row1.row AS row
        RETURN row, file() AS r, carrier
        """.formatted(url))).hasSize(3).containsOnlyNulls();
  }

  @Test
  void everyAggregatingClauseEndsTheRowContextTheSameWay() {
    assertThat(rows("WITH collect(row) AS rows UNWIND rows AS row RETURN file() AS r")).containsOnlyNulls();
    assertThat(rows("WITH collect(row) AS rows UNWIND rows AS row RETURN linenumber() AS r")).containsOnlyNulls();
    assertThat(rows("WITH count(*) AS c RETURN file() AS r")).containsOnlyNulls();
    assertThat(rows("WITH row, count(*) AS c RETURN file() AS r")).containsOnlyNulls();
  }

  // ---------------------------------------------------------------------------------------------------------

  private String load(final String tail) {
    return "LOAD CSV FROM '" + url + "' AS row " + tail;
  }

  private List<Object> rows(final String tail) {
    return query(load(tail));
  }

  /** Collects the {@code r} column of a whole query, for the two cases quoted from the report verbatim. */
  private List<Object> query(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty("r"));
    }
    return values;
  }
}
