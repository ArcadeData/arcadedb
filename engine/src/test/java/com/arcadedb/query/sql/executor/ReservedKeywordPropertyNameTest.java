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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/5092: using the reserved keyword FROM as
 * a property name in schema statements (CREATE/ALTER/DROP PROPERTY, CREATE INDEX) failed with a parsing
 * exception, while the symmetric keyword TO worked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReservedKeywordPropertyNameTest extends TestHelper {

  @Test
  void createPropertyNamedFromOnEdgeTypeScript() {
    // exact scenario from issue #5092
    database.command("sqlscript", """
        BEGIN;
        CREATE EDGE TYPE Edge;
        CREATE EDGE TYPE fulfilledBy EXTENDS Edge;
        CREATE PROPERTY fulfilledBy.From STRING;
        CREATE PROPERTY fulfilledBy.To STRING;
        COMMIT;
        """).close();

    final DocumentType type = database.getSchema().getType("fulfilledBy");
    assertThat(type.getProperty("From").getType()).isEqualTo(Type.STRING);
    assertThat(type.getProperty("To").getType()).isEqualTo(Type.STRING);
  }

  @Test
  void createAlterDropPropertyNamedFrom() {
    database.command("sql", "CREATE DOCUMENT TYPE Shipment").close();
    database.command("sql", "CREATE PROPERTY Shipment.From STRING").close();

    final DocumentType type = database.getSchema().getType("Shipment");
    assertThat(type.getProperty("From").getType()).isEqualTo(Type.STRING);

    database.command("sql", "ALTER PROPERTY Shipment.From MANDATORY true").close();
    assertThat(type.getProperty("From").isMandatory()).isTrue();

    database.command("sql", "DROP PROPERTY Shipment.From").close();
    assertThat(type.existsProperty("From")).isFalse();
  }

  @Test
  void createIndexOnPropertyNamedFrom() {
    database.command("sql", "CREATE DOCUMENT TYPE Route").close();
    database.command("sql", "CREATE PROPERTY Route.From STRING").close();
    database.command("sql", "CREATE INDEX ON Route (From) NOTUNIQUE").close();

    assertThat(database.getSchema().existsIndex("Route[From]")).isTrue();
  }

  @Test
  void readAndWritePropertyNamedFrom() {
    database.command("sql", "CREATE DOCUMENT TYPE Trip").close();
    database.command("sql", "CREATE PROPERTY Trip.From STRING").close();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Trip SET From = 'Rome', name = 't1'").close();
      database.command("sql", "INSERT INTO Trip (From, name) VALUES ('Paris', 't2')").close();
      database.command("sql", "UPDATE Trip SET From = 'London' WHERE name = 't1'").close();
    });

    try (final ResultSet rs = database.query("sql", "SELECT @this.From AS f FROM Trip WHERE name = 't2'")) {
      assertThat(rs.next().<String>getProperty("f")).isEqualTo("Paris");
    }

    // dotted access in an identifier chain
    try (final ResultSet rs = database.query("sql", "SELECT @this.From AS f FROM Trip")) {
      assertThat(rs.next().<String>getProperty("f")).isEqualTo("London");
    }

    // back-tick quoting must keep working for bare references
    try (final ResultSet rs = database.query("sql", "SELECT `From` AS f FROM Trip WHERE `From` = 'London'")) {
      assertThat(rs.next().<String>getProperty("f")).isEqualTo("London");
    }
  }

  @Test
  void selectFromTargetStillParses() {
    // guard: relaxing FROM as a property name must not break the plain form without projection
    database.command("sql", "CREATE DOCUMENT TYPE Plain").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Plain SET name = 'x'").close());

    try (final ResultSet rs = database.query("sql", "SELECT FROM Plain")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("x");
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM Plain WHERE name = 'x'")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }
}
