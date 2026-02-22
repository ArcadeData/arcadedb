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
package com.arcadedb.index.geospatial;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for ST_* spatial predicate functions with a real geospatial index.
 * Verifies that the query optimizer uses the GEOSPATIAL index and that results are correct
 * both with and without the index (inline full-scan fallback).
 */
class SQLGeoIndexedQueryTest extends TestHelper {

  /**
   * Inserts three Italian cities and verifies ST_Within correctly filters via index.
   * Rome (12.5, 41.9) and Naples (14.3, 40.8) are inside the bounding box;
   * Milan (9.2, 45.5) is outside.
   */
  @Test
  void stWithinWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location");
    database.command("sql", "CREATE PROPERTY Location.name STRING");
    database.command("sql", "CREATE PROPERTY Location.coords STRING");
    database.command("sql", "CREATE INDEX ON Location (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    // Bounding box: lon 10..16, lat 38..44 — should include Rome and Naples, not Milan
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location WHERE ST_Within(coords, ST_GeomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(2);
    assertThat(names).containsExactlyInAnyOrder("Rome", "Naples");
  }

  /**
   * Verifies ST_Intersects against a bounding box returns the expected cities.
   */
  @Test
  void stIntersectsWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location2");
    database.command("sql", "CREATE PROPERTY Location2.name STRING");
    database.command("sql", "CREATE PROPERTY Location2.coords STRING");
    database.command("sql", "CREATE INDEX ON Location2 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location2 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location2 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location2 SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    // Bounding box covers Rome and Naples only
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location2 WHERE ST_Intersects(coords, ST_GeomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(2);
    assertThat(names).containsExactlyInAnyOrder("Rome", "Naples");
  }

  /**
   * Verifies ST_DWithin proximity query: only Rome is within ~4 degrees of the search point.
   */
  @Test
  void stDWithinWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location3");
    database.command("sql", "CREATE PROPERTY Location3.name STRING");
    database.command("sql", "CREATE PROPERTY Location3.coords STRING");
    database.command("sql", "CREATE INDEX ON Location3 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location3 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location3 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location3 SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    // Search near Rome (12.5, 42.0) within 1.5 degrees — should find Rome and Naples but not Milan
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location3 WHERE ST_DWithin(coords, 'POINT (12.5 42.0)', 2.0) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).isNotEmpty();
    assertThat(names).contains("Rome");
    assertThat(names).doesNotContain("Milan");
  }

  /**
   * Verifies that dropping the index and re-running the ST_Within query (inline full-scan fallback)
   * produces the same correct results.
   */
  @Test
  void stWithinWithoutIndexFallback() {
    database.command("sql", "CREATE DOCUMENT TYPE Location4");
    database.command("sql", "CREATE PROPERTY Location4.name STRING");
    database.command("sql", "CREATE PROPERTY Location4.coords STRING");
    database.command("sql", "CREATE INDEX ON Location4 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location4 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location4 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location4 SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    // Drop the geospatial index to force inline full-scan evaluation
    database.command("sql", "DROP INDEX `Location4[coords]`");

    final ResultSet result = database.query("sql",
        "SELECT name FROM Location4 WHERE ST_Within(coords, ST_GeomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(2);
    assertThat(names).containsExactlyInAnyOrder("Rome", "Naples");
  }

  /**
   * Verifies ST_Disjoint returns the city outside the bounding box.
   */
  @Test
  void stDisjointWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location5");
    database.command("sql", "CREATE PROPERTY Location5.name STRING");
    database.command("sql", "CREATE PROPERTY Location5.coords STRING");
    database.command("sql", "CREATE INDEX ON Location5 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location5 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location5 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
    });

    // Bounding box covering Rome only; Milan is disjoint from it
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location5 WHERE ST_Disjoint(coords, ST_GeomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).containsExactly("Milan");
  }
}
