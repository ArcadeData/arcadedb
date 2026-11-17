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
package com.arcadedb.function.convert;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * valueType() of a list, as Neo4j answers it (issue #12395).
 * <p>
 * Every list answered {@code LIST<ANY> NOT NULL}, whatever its elements held, so a type check written against Neo4j
 * ({@code WHERE valueType(x) = 'LIST<INTEGER NOT NULL> NOT NULL'}) was false for every list.
 */
class ValueTypeListIssue12395Test {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final DatabaseFactory factory = new DatabaseFactory(
        "./target/databases/testIssue12395-" + testInfo.getTestMethod().get().getName());
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

  /** A list of elements of one type reports that type. */
  @Test
  void listOfOneTypeReportsThatType() {
    assertThat(valueType("[1, 2]")).isEqualTo("LIST<INTEGER NOT NULL> NOT NULL");
    assertThat(valueType("['a']")).isEqualTo("LIST<STRING NOT NULL> NOT NULL");
    assertThat(valueType("[1.5]")).isEqualTo("LIST<FLOAT NOT NULL> NOT NULL");
    assertThat(valueType("[true]")).isEqualTo("LIST<BOOLEAN NOT NULL> NOT NULL");
  }

  /** An empty list has no element type at all: Neo4j answers LIST&lt;NOTHING&gt;. */
  @Test
  void emptyListReportsNothing() {
    assertThat(valueType("[]")).isEqualTo("LIST<NOTHING> NOT NULL");
  }

  /** A list holding a null element has a nullable element type. */
  @Test
  void listHoldingANullHasANullableElementType() {
    assertThat(valueType("[1, null]")).isEqualTo("LIST<INTEGER> NOT NULL");
    assertThat(valueType("[null]")).isEqualTo("LIST<NULL> NOT NULL");
    assertThat(valueType("[1, 'a', null]")).isEqualTo("LIST<STRING | INTEGER> NOT NULL");
  }

  /** A list holding elements of more than one type reports the union of them, in the order Neo4j renders it. */
  @Test
  void listOfSeveralTypesReportsTheirUnion() {
    assertThat(valueType("[1, 'a']")).isEqualTo("LIST<STRING NOT NULL | INTEGER NOT NULL> NOT NULL");
    assertThat(valueType("['a', 1]")).isEqualTo("LIST<STRING NOT NULL | INTEGER NOT NULL> NOT NULL");
    assertThat(valueType("[1, 'a', true]")).isEqualTo("LIST<BOOLEAN NOT NULL | STRING NOT NULL | INTEGER NOT NULL> NOT NULL");
    assertThat(valueType("[1, 2.5]")).isEqualTo("LIST<INTEGER NOT NULL | FLOAT NOT NULL> NOT NULL");
  }

  /** A list of lists reports the type of the lists it holds. */
  @Test
  void listOfListsReportsTheTypeOfItsElements() {
    assertThat(valueType("[[1]]")).isEqualTo("LIST<LIST<INTEGER NOT NULL> NOT NULL> NOT NULL");
    assertThat(valueType("[[1], ['a']]")).isEqualTo(
        "LIST<LIST<STRING NOT NULL> NOT NULL | LIST<INTEGER NOT NULL> NOT NULL> NOT NULL");
    assertThat(valueType("[[1], [true]]")).isEqualTo(
        "LIST<LIST<BOOLEAN NOT NULL> NOT NULL | LIST<INTEGER NOT NULL> NOT NULL> NOT NULL");
    assertThat(valueType("[[]]")).isEqualTo("LIST<LIST<NOTHING> NOT NULL> NOT NULL");
  }

  /** A list of lists holding a null element has a nullable element type, as any other list does. */
  @Test
  void listOfListsHoldingANullHasANullableElementType() {
    assertThat(valueType("[[1], null]")).isEqualTo("LIST<LIST<INTEGER NOT NULL>> NOT NULL");
    assertThat(valueType("[[1], ['a'], null]")).isEqualTo(
        "LIST<LIST<STRING NOT NULL> | LIST<INTEGER NOT NULL>> NOT NULL");
  }

  /** A point is a point, and a map is a map, inside a list as well. */
  @Test
  void listOfMapsAndPointsReportsThem() {
    assertThat(valueType("[{a: 1}]")).isEqualTo("LIST<MAP NOT NULL> NOT NULL");
    assertThat(valueType("[point({x: 1, y: 2})]")).isEqualTo("LIST<POINT NOT NULL> NOT NULL");
  }

  /** The controls: the types that are not lists are untouched. */
  @Test
  void typesThatAreNotListsAreUntouched() {
    assertThat(valueType("1")).isEqualTo("INTEGER NOT NULL");
    assertThat(valueType("1.5")).isEqualTo("FLOAT NOT NULL");
    assertThat(valueType("'a'")).isEqualTo("STRING NOT NULL");
    assertThat(valueType("true")).isEqualTo("BOOLEAN NOT NULL");
    assertThat(valueType("null")).isEqualTo("NULL");
    assertThat(valueType("{a: 1}")).isEqualTo("MAP NOT NULL");
    assertThat(valueType("point({x: 1, y: 2})")).isEqualTo("POINT NOT NULL");
    assertThat(valueType("date()")).isEqualTo("DATE NOT NULL");
  }

  private String valueType(final String expression) {
    final ResultSet rs = database.query("cypher", "RETURN valueType(" + expression + ") AS v");
    assertThat(rs.hasNext()).as("no row for: %s", expression).isTrue();
    return (String) rs.next().getProperty("v");
  }
}
