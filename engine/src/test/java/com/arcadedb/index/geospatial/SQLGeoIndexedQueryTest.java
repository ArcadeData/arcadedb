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
   * Verifies ST_DWithin proximity query using the inline full-scan fallback path (the index
   * exists on the type but ST_DWithin always disables indexed execution).
   *
   * <p>Search point: POINT (12.0, 41.5), distance threshold: 1.0 degree (great-circle degrees
   * as computed by {@code SpatialContext.calcDistance()}).
   *
   * <p>Distances from the search point:
   * <ul>
   *   <li>Rome   (12.5, 41.9): sqrt(0.5^2 + 0.4^2) ≈ 0.64 degrees — within 1.0, included</li>
   *   <li>Naples (14.3, 40.8): sqrt(2.3^2 + 0.7^2) ≈ 2.40 degrees — outside 1.0, excluded</li>
   *   <li>Milan  ( 9.2, 45.5): far away              — outside 1.0, excluded</li>
   * </ul>
   *
   * <p>Only Rome qualifies.
   */
  @Test
  void stDWithinFallbackWithExistingIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location3");
    database.command("sql", "CREATE PROPERTY Location3.name STRING");
    database.command("sql", "CREATE PROPERTY Location3.coords STRING");
    database.command("sql", "CREATE INDEX ON Location3 (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location3 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location3 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location3 SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    // Search from POINT (12.0, 41.5) within 1.0 degree — only Rome qualifies
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location3 WHERE ST_DWithin(coords, 'POINT (12.0 41.5)', 1.0) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactlyInAnyOrder("Rome");
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
   * ST_Disjoint always uses the inline full-scan fallback because the GeoHash index
   * stores intersecting records — disjoint records are precisely those NOT returned
   * by the index, so the index cannot serve as a valid candidate superset.
   */
  @Test
  void stDisjointFallbackWithExistingIndex() {
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

  /**
   * Verifies ST_Contains with stored polygons using inline full-scan evaluation.
   * ST_Contains(coords, point) finds which stored polygon contains Rome.
   * No GEOSPATIAL index is created here because the index is optimised for searching
   * stored points inside a query polygon (ST_Within direction); ST_Contains queries
   * a small containee shape against large stored containers and the GeoHash detail
   * level for a point query is too coarse to locate polygon tokens reliably.
   */
  @Test
  void stContainsFallback() {
    database.command("sql", "CREATE DOCUMENT TYPE Location6");
    database.command("sql", "CREATE PROPERTY Location6.name STRING");
    database.command("sql", "CREATE PROPERTY Location6.coords STRING");

    database.transaction(() -> {
      // A polygon that contains Rome (12.5, 41.9) but not Milan (9.2, 45.5)
      database.command("sql", "INSERT INTO Location6 SET name = 'ItalyBox', coords = 'POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))'");
      // A polygon that contains Milan but not Rome
      database.command("sql", "INSERT INTO Location6 SET name = 'NorthBox', coords = 'POLYGON ((8 44, 11 44, 11 47, 8 47, 8 44))'");
    });

    // ST_Contains(coords, point) — find which stored polygon contains Rome
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location6 WHERE ST_Contains(coords, ST_GeomFromText('POINT (12.5 41.9)')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactly("ItalyBox");
  }

  /**
   * Verifies ST_Equals using inline full-scan evaluation.
   * Only the record at exactly (12.5, 41.9) matches the equality query.
   * No GEOSPATIAL index is created here because the GeoHash detail level for a point
   * query shape is too coarse to retrieve the stored point token at full precision.
   */
  @Test
  void stEqualsFallback() {
    database.command("sql", "CREATE DOCUMENT TYPE Location7");
    database.command("sql", "CREATE PROPERTY Location7.name STRING");
    database.command("sql", "CREATE PROPERTY Location7.coords STRING");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Location7 SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Location7 SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
      database.command("sql", "INSERT INTO Location7 SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
    });

    final ResultSet result = database.query("sql",
        "SELECT name FROM Location7 WHERE ST_Equals(coords, ST_GeomFromText('POINT (12.5 41.9)')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactly("Rome");
  }

  /**
   * Verifies ST_Crosses: a stored linestring that crosses a polygon boundary is returned.
   * The line from (9, 38) to (16, 45) crosses the boundary of the polygon
   * POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38)).
   */
  @Test
  void stCrossesWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location8");
    database.command("sql", "CREATE PROPERTY Location8.name STRING");
    database.command("sql", "CREATE PROPERTY Location8.coords STRING");
    database.command("sql", "CREATE INDEX ON Location8 (coords) GEOSPATIAL");

    database.transaction(() -> {
      // This line crosses the polygon boundary: starts outside (9,38), ends outside (16,45),
      // but passes through the interior — it enters and exits the polygon
      database.command("sql", "INSERT INTO Location8 SET name = 'CrossingLine', coords = 'LINESTRING (9 38, 16 45)'");
      // This line is fully inside the polygon — does not cross the boundary, so crosses() = false
      database.command("sql", "INSERT INTO Location8 SET name = 'InsideLine', coords = 'LINESTRING (11 39, 15 43)'");
    });

    final ResultSet result = database.query("sql",
        "SELECT name FROM Location8 WHERE ST_Crosses(coords, ST_GeomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactly("CrossingLine");
  }

  /**
   * Verifies ST_Overlaps: two polygons with partial overlap are returned, but a fully-contained
   * polygon is not (overlaps requires same-dimension partial intersection, not containment).
   */
  @Test
  void stOverlapsWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location9");
    database.command("sql", "CREATE PROPERTY Location9.name STRING");
    database.command("sql", "CREATE PROPERTY Location9.coords STRING");
    database.command("sql", "CREATE INDEX ON Location9 (coords) GEOSPATIAL");

    database.transaction(() -> {
      // Partially overlaps the query polygon (shares area but neither contains the other)
      database.command("sql", "INSERT INTO Location9 SET name = 'WestBox', coords = 'POLYGON ((10 38, 14 38, 14 43, 10 43, 10 38))'");
      // Fully contained inside the query polygon — overlaps() = false (containment, not overlap)
      database.command("sql", "INSERT INTO Location9 SET name = 'TinyInner', coords = 'POLYGON ((13 41, 14 41, 14 42, 13 42, 13 41))'");
      // Completely outside the query polygon — overlaps() = false
      database.command("sql", "INSERT INTO Location9 SET name = 'FarBox', coords = 'POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))'");
    });

    // Query polygon: POLYGON ((12 40, 16 40, 16 45, 12 45, 12 40))
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location9 WHERE ST_Overlaps(coords, ST_GeomFromText('POLYGON ((12 40, 16 40, 16 45, 12 45, 12 40))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactly("WestBox");
  }

  /**
   * Verifies ST_Touches: two polygons sharing exactly one edge touch each other.
   * The left polygon ends at x=12, the right polygon starts at x=12 — they share the boundary.
   */
  @Test
  void stTouchesWithIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Location10");
    database.command("sql", "CREATE PROPERTY Location10.name STRING");
    database.command("sql", "CREATE PROPERTY Location10.coords STRING");
    database.command("sql", "CREATE INDEX ON Location10 (coords) GEOSPATIAL");

    database.transaction(() -> {
      // Shares the edge at x=12 with the query polygon — interiors do not overlap
      database.command("sql", "INSERT INTO Location10 SET name = 'LeftBox', coords = 'POLYGON ((10 38, 12 38, 12 42, 10 42, 10 38))'");
      // Fully separate — does not touch
      database.command("sql", "INSERT INTO Location10 SET name = 'FarBox', coords = 'POLYGON ((20 38, 25 38, 25 42, 20 42, 20 38))'");
    });

    // Right polygon starting at x=12 touches LeftBox at the shared edge
    final ResultSet result = database.query("sql",
        "SELECT name FROM Location10 WHERE ST_Touches(coords, ST_GeomFromText('POLYGON ((12 38, 16 38, 16 42, 12 42, 12 38))')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).hasSize(1);
    assertThat(names).containsExactly("LeftBox");
  }
}
