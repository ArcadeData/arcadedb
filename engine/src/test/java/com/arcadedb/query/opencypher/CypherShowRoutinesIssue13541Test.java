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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13541: {@code SHOW PROCEDURES} and {@code SHOW FUNCTIONS} were answered over Bolt with no row whatever the {@code YIELD} and
 * {@code WHERE} said, and refused by the engine, so by gRPC and HTTP. A client asks the server which procedures it has with
 * {@code SHOW PROCEDURES YIELD name WHERE name = '...'}; it is now answered from the registries, with Neo4j's columns, on every
 * transport.
 */
class CypherShowRoutinesIssue13541Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-show-routines-13541").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aProcedureThatExistsIsListed() {
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name = 'db.labels' RETURN name")).containsExactly("db.labels");
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'db.' RETURN name")).contains("db.labels",
        "db.propertyKeys", "db.relationshipTypes");
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name = 'no.such.procedure' RETURN name")).isEmpty();
  }

  @Test
  void aProcedureIsListedUnderTheApocNameItIsResolvedBy() {
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name IN ['merge.relationship', 'apoc.merge.relationship'] RETURN name"))
        .containsExactlyInAnyOrder("merge.relationship", "apoc.merge.relationship");
  }

  /** The columns of a result set are a set; their order is checked over Bolt, where a record is a list. */
  @Test
  void aBareShowAnswersTheDefaultColumnsAndAYieldStarAllOfThem() {
    try (final ResultSet bare = database.query("opencypher", "SHOW PROCEDURES")) {
      assertThat(bare.next().getPropertyNames()).containsExactly("name", "description", "mode", "worksOnSystem");
    }
    try (final ResultSet all = database.query("opencypher", "SHOW PROCEDURES YIELD *")) {
      assertThat(all.next().getPropertyNames()).containsExactlyInAnyOrder("name", "description", "mode", "worksOnSystem", "signature",
          "argumentDescription", "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated",
          "deprecatedBy", "option");
    }
    try (final ResultSet bare = database.query("opencypher", "SHOW FUNCTIONS")) {
      assertThat(bare.next().getPropertyNames()).containsExactly("name", "category", "description");
    }
    try (final ResultSet all = database.query("opencypher", "SHOW FUNCTIONS YIELD *")) {
      assertThat(all.next().getPropertyNames()).containsExactlyInAnyOrder("name", "category", "description", "signature", "isBuiltIn",
          "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated",
          "deprecatedBy");
    }
  }

  @Test
  void theModeOfAProcedureSaysWhetherItWrites() {
    try (final ResultSet rows = database.query("opencypher",
        "SHOW PROCEDURES YIELD name, mode WHERE name IN ['db.labels', 'merge.relationship'] RETURN name, mode ORDER BY name")) {
      assertThat(rows.next().<String>getProperty("mode")).isEqualTo("READ");
      assertThat(rows.next().<String>getProperty("mode")).isEqualTo("WRITE");
    }
  }

  @Test
  void aFunctionIsListedUnderNeo4jSpelling() {
    assertThat(names("SHOW FUNCTIONS YIELD name WHERE name IN ['toString', 'abs', 'elementId', 'randomUUID'] RETURN name ORDER BY name"))
        .containsExactly("abs", "elementId", "randomUUID", "toString");
    assertThat(names("SHOW FUNCTIONS YIELD name WHERE name = 'tostring' RETURN name")).isEmpty();
  }

  @Test
  void theFunctionKindsAreFiltered() {
    assertThat(names("SHOW BUILT IN FUNCTIONS YIELD name WHERE name IN ['abs', 'text.indexOf'] RETURN name")).containsExactly("abs");
    assertThat(names("SHOW USER DEFINED FUNCTIONS YIELD name WHERE name IN ['abs', 'text.indexOf'] RETURN name"))
        .containsExactly("text.indexOf");
    assertThat(names("SHOW ALL FUNCTIONS YIELD name WHERE name IN ['abs', 'text.indexOf'] RETURN name ORDER BY name"))
        .containsExactly("abs", "text.indexOf");
    assertThat(names("SHOW FUNCTIONS YIELD name WHERE name = 'count' RETURN name")).containsExactly("count");
  }

  @Test
  void anAggregatingFunctionSaysSo() {
    try (final ResultSet rows = database.query("opencypher",
        "SHOW FUNCTIONS YIELD name, aggregating, isBuiltIn WHERE name IN ['collect', 'abs'] RETURN name, aggregating, isBuiltIn "
            + "ORDER BY name")) {
      Result abs = rows.next();
      assertThat(abs.<String>getProperty("name")).isEqualTo("abs");
      assertThat(abs.<Boolean>getProperty("aggregating")).isFalse();
      assertThat(abs.<Boolean>getProperty("isBuiltIn")).isTrue();
      Result collect = rows.next();
      assertThat(collect.<Boolean>getProperty("aggregating")).isTrue();
    }
  }

  @Test
  void theCommandsAreReadOnlyAndRunThroughCommandToo() {
    try (final ResultSet rows = database.command("opencypher", "SHOW PROCEDURES YIELD name WHERE name = 'db.labels' RETURN name")) {
      assertThat(rows.next().<String>getProperty("name")).isEqualTo("db.labels");
    }
  }

  private List<String> names(final String statement) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rows = database.query("opencypher", statement)) {
      while (rows.hasNext())
        names.add(rows.next().getProperty("name"));
    }
    return names;
  }
}
