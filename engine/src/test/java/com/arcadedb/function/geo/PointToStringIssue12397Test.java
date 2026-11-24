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
package com.arcadedb.function.geo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * toString() of a point, and the concatenation of a string with a map, as Neo4j answers them (issue #12397).
 * <p>
 * A point is a map at runtime, so toString() used to refuse it and the concatenation used to render it in the Java
 * {@code Map.toString()} form, which prints the internal keys of the point ({@code srid} included) in a shape no
 * point constructor or toString() accepts.
 */
class PointToStringIssue12397Test {
  private Database database;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    final DatabaseFactory factory = new DatabaseFactory(
        "./target/databases/testIssue12397-" + testInfo.getTestMethod().get().getName());
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

  /** A 2D cartesian point renders as the point literal, the way Neo4j renders it. */
  @Test
  void toStringRendersACartesianPoint() {
    assertThat(scalar("RETURN toString(point({x: 1, y: 2})) AS v"))
        .isEqualTo("point({x: 1.0, y: 2.0, crs: 'cartesian'})");
  }

  /** A WGS-84 point renders with the coordinates it was given and the name of its reference system. */
  @Test
  void toStringRendersAWgs84Point() {
    assertThat(scalar("RETURN toString(point({longitude: 1, latitude: 2})) AS v"))
        .isEqualTo("point({x: 1.0, y: 2.0, crs: 'wgs-84'})");
  }

  /** A 3D point renders its z coordinate too. */
  @Test
  void toStringRendersA3dPoint() {
    assertThat(scalar("RETURN toString(point({x: 1, y: 2, z: 3})) AS v"))
        .isEqualTo("point({x: 1.0, y: 2.0, z: 3.0, crs: 'cartesian-3d'})");
  }

  /** The coordinates are printed as the doubles they are. */
  @Test
  void toStringRendersTheCoordinatesAsDoubles() {
    assertThat(scalar("RETURN toString(point({x: 1.5, y: 2.25})) AS v"))
        .isEqualTo("point({x: 1.5, y: 2.25, crs: 'cartesian'})");
  }

  /** A point read back from storage renders the same way: it is the same map. */
  @Test
  void toStringRendersAPointReadBackFromStorage() {
    database.command("opencypher", "CREATE (:Place {loc: point({x: 1, y: 2})})");

    assertThat(scalar("MATCH (p:Place) RETURN toString(p.loc) AS v"))
        .isEqualTo("point({x: 1.0, y: 2.0, crs: 'cartesian'})");
  }

  /** A map that is not a point is still refused, as it is in Neo4j. */
  @Test
  void toStringStillRefusesAPlainMap() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN toString({a: 1}) AS v").next())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("cannot convert");
  }

  /** Concatenating a string with a point is refused, as it is in Neo4j. */
  @Test
  void concatenatingAStringWithAPointIsRefused() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN 'a' + point({x: 1, y: 2}) AS v").next())
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("Point");
  }

  /** Concatenating a string with a map is refused, as it is in Neo4j. */
  @Test
  void concatenatingAStringWithAMapIsRefused() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN 'a' + {k: 1} AS v").next())
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("Map");
  }

  /** The same holds the other way round: the map comes first. */
  @Test
  void concatenatingAMapWithAStringIsRefused() {
    assertThatThrownBy(() -> database.query("cypher", "RETURN {k: 1} + 'a' AS v").next())
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("Map");
  }

  /** The controls: concatenating two strings, or a string and a number, is untouched. */
  @Test
  void concatenatingStringsAndNumbersIsUntouched() {
    assertThat(scalar("RETURN 'a' + 'b' AS v")).isEqualTo("ab");
    assertThat(scalar("RETURN 'a' + 1 AS v")).isEqualTo("a1");
    assertThat(scalar("RETURN 1 + 'a' AS v")).isEqualTo("1a");
  }

  private String scalar(final String query) {
    final ResultSet rs = database.query("cypher", query);
    assertThat(rs.hasNext()).as("no row for: %s", query).isTrue();
    return (String) rs.next().getProperty("v");
  }
}
