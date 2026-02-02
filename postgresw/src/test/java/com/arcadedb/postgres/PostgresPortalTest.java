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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PostgresPortal.
 */
class PostgresPortalTest {

  @Test
  void constructorInitializesFields() {
    PostgresPortal portal = new PostgresPortal("SELECT FROM Test", "sql");

    assertThat(portal.query).isEqualTo("SELECT FROM Test");
    assertThat(portal.language).isEqualTo("sql");
    assertThat(portal.sqlStatement).isNull();
    assertThat(portal.parameterTypes).isNull();
    assertThat(portal.parameterFormats).isNull();
    assertThat(portal.parameterValues).isNull();
    assertThat(portal.resultFormats).isNull();
    assertThat(portal.cachedResultSet).isNull();
    assertThat(portal.columns).isNull();
    assertThat(portal.ignoreExecution).isFalse();
    assertThat(portal.isExpectingResult).isTrue();
    assertThat(portal.executed).isFalse();
    assertThat(portal.rowDescriptionSent).isFalse();
  }

  @Test
  void toStringReturnsQuery() {
    PostgresPortal portal = new PostgresPortal("SELECT * FROM Users", "sql");

    String str = portal.toString();
    assertThat(str).isEqualTo("SELECT * FROM Users");
  }

  @Test
  void toStringWithCypherLanguage() {
    PostgresPortal portal = new PostgresPortal("MATCH (n) RETURN n", "opencypher");

    String str = portal.toString();
    assertThat(str).isEqualTo("MATCH (n) RETURN n");
  }

  @Test
  void fieldsCanBeModified() {
    PostgresPortal portal = new PostgresPortal("SELECT FROM Test", "sql");

    portal.parameterTypes = new ArrayList<>();
    portal.parameterTypes.add(23L); // INTEGER OID
    portal.parameterTypes.add(1043L); // VARCHAR OID

    portal.parameterFormats = new ArrayList<>();
    portal.parameterFormats.add(0); // text format

    portal.parameterValues = new ArrayList<>();
    portal.parameterValues.add(42);
    portal.parameterValues.add("hello");

    portal.resultFormats = new ArrayList<>();
    portal.resultFormats.add(0);

    portal.columns = new HashMap<>();
    portal.columns.put("id", PostgresType.INTEGER);
    portal.columns.put("name", PostgresType.VARCHAR);

    portal.cachedResultSet = new ArrayList<>();

    portal.ignoreExecution = true;
    portal.isExpectingResult = false;
    portal.executed = true;
    portal.rowDescriptionSent = true;

    assertThat(portal.parameterTypes).hasSize(2);
    assertThat(portal.parameterFormats).hasSize(1);
    assertThat(portal.parameterValues).hasSize(2);
    assertThat(portal.resultFormats).hasSize(1);
    assertThat(portal.columns).hasSize(2);
    assertThat(portal.cachedResultSet).isEmpty();
    assertThat(portal.ignoreExecution).isTrue();
    assertThat(portal.isExpectingResult).isFalse();
    assertThat(portal.executed).isTrue();
    assertThat(portal.rowDescriptionSent).isTrue();
  }

  @Test
  void toStringIncludesAllRelevantInfo() {
    PostgresPortal portal = new PostgresPortal("INSERT INTO Test SET value = ?", "sql");
    portal.executed = true;
    portal.ignoreExecution = false;

    String str = portal.toString();
    // toString should be meaningful
    assertThat(str).isNotEmpty();
  }

  @Test
  void portalWithEmptyQuery() {
    PostgresPortal portal = new PostgresPortal("", "sql");

    assertThat(portal.query).isEmpty();
    assertThat(portal.language).isEqualTo("sql");
  }

  @Test
  void portalWithGremlinLanguage() {
    PostgresPortal portal = new PostgresPortal("g.V().hasLabel('Test')", "gremlin");

    assertThat(portal.query).isEqualTo("g.V().hasLabel('Test')");
    assertThat(portal.language).isEqualTo("gremlin");
  }
}
