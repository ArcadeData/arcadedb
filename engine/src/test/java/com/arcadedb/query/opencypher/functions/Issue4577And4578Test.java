/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for:
 * <ul>
 *   <li>#4578 - the 2-arg positional {@code point(a, b)} form must follow the GIS convention
 *       {@code point(x, y)} ≡ {@code point(longitude, latitude)} instead of the swapped
 *       {@code point(latitude, longitude)}.</li>
 *   <li>#4577 - {@code point.distance()} must not silently fall through to Euclidean distance
 *       when the two points are in different coordinate reference systems (WGS-84 vs Cartesian).
 *       Matching the Neo4j reference implementation, it returns {@code null}.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4577And4578Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-issue-4577-4578");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== #4578 ====================

  @Test
  @SuppressWarnings("unchecked")
  void positionalPointFollowsXYConvention() {
    final ResultSet result = database.command("opencypher", "RETURN point(1.0, 2.0) AS p");
    assertThat(result.hasNext()).isTrue();
    final Map<String, Object> point = (Map<String, Object>) result.next().getProperty("p");
    assertThat(point).isNotNull();
    // point(x, y): first arg is x (longitude), second is y (latitude)
    assertThat(((Number) point.get("x")).doubleValue()).isCloseTo(1.0, within(0.0001));
    assertThat(((Number) point.get("y")).doubleValue()).isCloseTo(2.0, within(0.0001));
    assertThat(((Number) point.get("longitude")).doubleValue()).isCloseTo(1.0, within(0.0001));
    assertThat(((Number) point.get("latitude")).doubleValue()).isCloseTo(2.0, within(0.0001));
  }

  // ==================== #4577 ====================

  @Test
  void mixedCRSDistanceReturnsNull() {
    // WGS-84 point first, Cartesian point second
    ResultSet result = database.command("opencypher",
        "RETURN point.distance(point({longitude: 0.0, latitude: 0.0}), point({x: 3.0, y: 4.0})) AS distance");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Object>getProperty("distance")).isNull();

    // Reverse order: Cartesian first, WGS-84 second
    result = database.command("opencypher",
        "RETURN point.distance(point({x: 3.0, y: 4.0}), point({longitude: 0.0, latitude: 0.0})) AS distance");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Object>getProperty("distance")).isNull();
  }

  @Test
  void sameCRSDistanceStillWorks() {
    // Sanity check that valid same-CRS distances are unaffected by the mismatch guard
    ResultSet result = database.command("opencypher",
        "RETURN point.distance(point({x: 0.0, y: 0.0}), point({x: 3.0, y: 4.0})) AS distance");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Double>getProperty("distance")).isCloseTo(5.0, within(0.001));

    result = database.command("opencypher",
        """
        RETURN point.distance(\
        point({longitude: 12.564590, latitude: 55.672874}), \
        point({longitude: 12.994341, latitude: 55.611784})) AS distance""");
    assertThat(result.hasNext()).isTrue();
    final Double geo = result.next().getProperty("distance");
    assertThat(geo).isGreaterThan(25000.0).isLessThan(32000.0);
  }
}
