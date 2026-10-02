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
 * Issue #8908: a variable exported by a scoped {@code CALL (*) { ... UNION ... }} was reported as undefined once a
 * {@code WITH x, row} followed a {@code LOAD CSV}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCallUnionLoadCsvScopeIssue8908Test {
  private Database database;
  private String   url;

  @BeforeEach
  void setUp() throws IOException {
    final File csv = new File("./target/databases/cypher-8908/arcade-load.csv");
    csv.getParentFile().mkdirs();
    try (final PrintWriter writer = new PrintWriter(csv, "UTF-8")) {
      writer.println("value");
      writer.println("a");
      writer.println("b");
    }
    url = csv.getAbsolutePath();

    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-8908/db");
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

  @Test
  void scopedCallUnionThenWhereThenLoadCsvThenWith() {
    assertThat(run("""
        CALL (*) {
          RETURN null AS x
          UNION
          CALL db.propertyKeys() YIELD propertyKey
          RETURN null AS x
        }
        WITH x
        WHERE x IS NULL
        LOAD CSV FROM '%s' AS row
        WITH x, row
        RETURN x
        """.formatted(url))).hasSize(3).containsOnlyNulls();
  }

  @Test
  void simplerShapesKeepWorking() {
    assertThat(run("""
        CALL (*) { RETURN null AS x }
        WITH x
        LOAD CSV FROM '%s' AS row
        WITH x, row
        RETURN x
        """.formatted(url))).hasSize(3).containsOnlyNulls();
    assertThat(run("""
        CALL (*) { RETURN null AS x UNION RETURN null AS x }
        LOAD CSV FROM '%s' AS row
        WITH x, row
        RETURN x
        """.formatted(url))).hasSize(3).containsOnlyNulls();
  }

  @Test
  void sameQueryThroughCommandAsBoltDoesForWrites() {
    assertThat(runCommand("""
        CALL (*) {
          RETURN null AS x
          UNION
          CALL db.propertyKeys() YIELD propertyKey
          RETURN null AS x
        }
        WITH x
        WHERE x IS NULL
        LOAD CSV FROM '%s' AS row
        WITH x, row
        RETURN x
        """.formatted(url))).hasSize(3).containsOnlyNulls();
  }

  private List<Object> runCommand(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", cypher)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty("x"));
    }
    return values;
  }

  private List<Object> run(final String cypher) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", cypher)) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        values.add(result.getProperty("x"));
      }
    }
    return values;
  }
}
