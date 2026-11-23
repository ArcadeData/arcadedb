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
package com.arcadedb.function.sql.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * A shape is within itself and contains itself, as OGC {@code ST_Within}/{@code ST_Contains}, JTS and PostGIS answer, so
 * {@code geo.within(a, b)} always agrees with {@code geo.contains(b, a)} (issue #12271). Spatial4j's {@code relate} answers
 * CONTAINS for two equal shapes and INTERSECTS for two equal points, never WITHIN, and the GEOSPATIAL index re-checks with
 * the same predicate, so an indexed {@code geo.within(coords, <shape>)} left out the record holding that very shape.
 */
class GeoWithinEqualShapesTest extends TestHelper {
  private static final String PT   = "POINT (2.2945 48.8584)";
  private static final String BOX  = "POLYGON ((2 48, 3 48, 3 49, 2 49, 2 48))";
  private static final String TRI  = "POLYGON ((2 48, 3 48, 2 49, 2 48))";
  private static final String LINE = "LINESTRING (2 48, 3 49)";

  /** Shapes Spatial4j reads as something other than a JTS geometry, a point or a rectangle, or relates in its own way. */
  private static final List<String> EXOTIC = List.of(
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)), POINT (50 50))",
      "GEOMETRYCOLLECTION (POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)), POLYGON ((20 20, 30 20, 30 30, 20 30, 20 20)))",
      "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 2, 3 3))", "MULTIPOINT ((1 1), (2 2))",
      "BUFFER(LINESTRING (0 0, 10 0), 1)", "BUFFER(POINT (5 5), 1)");

  @Test
  void aShapeIsWithinAndContainsItself() {
    for (final String w : List.of(PT, BOX, TRI, LINE, "POINT (1 2)", "ENVELOPE (2, 3, 49, 48)",
        "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((5 5, 6 5, 6 6, 5 6, 5 5)))")) {
      assertThat(bool("SELECT geo.within(?, ?) AS v", w, w)).as("within %s", w).isTrue();
      assertThat(bool("SELECT geo.contains(?, ?) AS v", w, w)).as("contains %s", w).isTrue();
      assertThat(bool("SELECT ?.isWithin(?) AS v", w, w)).as("isWithin %s", w).isTrue();
    }
  }

  @Test
  void theSameAreaWrittenTwiceIsWithinAndContainsItself() {
    // the same box with an extra vertex: Spatial4j relates it to the box as CONTAINS one way and WITHIN the other
    final String sameBoxMoreVertices = "POLYGON ((2 48, 3 48, 3 49, 2 49, 2 48.5, 2 48))";
    for (final String[] pair : new String[][] { { BOX, sameBoxMoreVertices }, { sameBoxMoreVertices, BOX },
        { "ENVELOPE (2, 3, 49, 48)", BOX }, { BOX, "ENVELOPE (2, 3, 49, 48)" },
        { "ENVELOPE (2, 3, 49, 48)", sameBoxMoreVertices }, { sameBoxMoreVertices, "ENVELOPE (2, 3, 49, 48)" } }) {
      assertThat(bool("SELECT geo.within(?, ?) AS v", pair[0], pair[1])).as("within(%s, %s)", pair[0], pair[1]).isTrue();
      assertThat(bool("SELECT geo.contains(?, ?) AS v", pair[0], pair[1])).as("contains(%s, %s)", pair[0], pair[1]).isTrue();
    }
  }

  @Test
  void differentShapesKeepTheirAnswer() {
    assertThat(bool("SELECT geo.within(?, ?) AS v", "POINT (1 2)", "POINT (1 3)")).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", "POINT (1 2)", "POINT (1 3)")).isFalse();
    assertThat(bool("SELECT geo.within(?, ?) AS v", PT, BOX)).isTrue();
    assertThat(bool("SELECT geo.within(?, ?) AS v", BOX, PT)).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", BOX, PT)).isTrue();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", PT, BOX)).isFalse();
    assertThat(bool("SELECT geo.within(?, ?) AS v", TRI, BOX)).isTrue();
    assertThat(bool("SELECT geo.within(?, ?) AS v", BOX, TRI)).isFalse();
    assertThat(bool("SELECT geo.within(?, ?) AS v", LINE, BOX)).isTrue();
    assertThat(bool("SELECT geo.within(?, ?) AS v", BOX, LINE)).isFalse();
    assertThat(bool("SELECT ?.isWithin(?) AS v", BOX, TRI)).isFalse();
  }

  @Test
  void withinAgreesWithTheConverseOfContains() {
    final List<String> shapes = List.of(PT, BOX, TRI, LINE, "POINT (2 48)", "POINT (2.5 48.25)", "LINESTRING (2 48, 3 48)",
        "POLYGON ((2.2 48.2, 2.8 48.2, 2.8 48.8, 2.2 48.8, 2.2 48.2))", "ENVELOPE (2, 3, 49, 48)");
    for (final String a : shapes)
      for (final String b : shapes)
        assertThat(bool("SELECT geo.within(?, ?) AS v", a, b)).as("within(%s, %s)", a, b)
            .isEqualTo(bool("SELECT geo.contains(?, ?) AS v", b, a));
  }

  /**
   * A shape that has no JTS form (Spatial4j reads a GEOMETRYCOLLECTION of mixed members as a ShapeCollection, and BUFFER(..)
   * as a buffered line) answered false before the same-shape comparison existed, so it must still answer, not fail the
   * statement, whichever way round it is passed.
   */
  @Test
  void aShapeWithoutAJtsFormStillAnswers() {
    final String mixed = "GEOMETRYCOLLECTION (POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)), POINT (50 50))";
    final String inside = "POINT (5 5)";
    final String outside = "POINT (20 20)";

    assertThat(bool("SELECT geo.within(?, ?) AS v", inside, mixed)).isTrue();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", mixed, inside)).isTrue();
    assertThat(bool("SELECT geo.within(?, ?) AS v", mixed, inside)).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", inside, mixed)).isFalse();
    assertThat(bool("SELECT ?.isWithin(?) AS v", mixed, inside)).isFalse();
    assertThat(bool("SELECT ?.isWithin(?) AS v", inside, mixed)).isTrue();

    assertThat(bool("SELECT geo.within(?, ?) AS v", outside, mixed)).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", mixed, outside)).isFalse();
    assertThat(bool("SELECT geo.within(?, ?) AS v", mixed, outside)).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", outside, mixed)).isFalse();

    final String buffered = "BUFFER(LINESTRING (0 0, 10 0), 1)";
    assertThat(bool("SELECT geo.within(?, ?) AS v", "POINT (5 0)", buffered)).isTrue();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", buffered, "POINT (5 0)")).isTrue();
    assertThat(bool("SELECT geo.within(?, ?) AS v", buffered, "POINT (5 0)")).isFalse();
    assertThat(bool("SELECT geo.contains(?, ?) AS v", "POINT (5 0)", buffered)).isFalse();
    assertThat(bool("SELECT ?.isWithin(?) AS v", buffered, "POINT (5 0)")).isFalse();
  }

  /** Two collections written the same are the same set of points, whichever predicate asks. */
  @Test
  void aCollectionWithoutAJtsFormIsWithinAndContainsItself() {
    for (final String w : EXOTIC) {
      assertThat(bool("SELECT geo.within(?, ?) AS v", w, w)).as("within %s", w).isTrue();
      assertThat(bool("SELECT geo.contains(?, ?) AS v", w, w)).as("contains %s", w).isTrue();
      assertThat(bool("SELECT ?.isWithin(?) AS v", w, w)).as("isWithin %s", w).isTrue();
    }
  }

  /** Whatever the kinds of the two shapes, a predicate answers: the comparison of equal shapes never fails a statement. */
  @Test
  void everyPairOfShapeKindsAnswersInBothOrders() {
    final List<String> shapes = new ArrayList<>(EXOTIC);
    shapes.addAll(List.of(PT, BOX, TRI, LINE, "ENVELOPE (2, 3, 49, 48)", "POINT (5 5)"));
    for (final String a : shapes)
      for (final String b : shapes) {
        final String pair = a + " | " + b;
        assertThatCode(() -> bool("SELECT geo.within(?, ?) AS v", a, b)).as("within %s", pair).doesNotThrowAnyException();
        assertThatCode(() -> bool("SELECT geo.contains(?, ?) AS v", a, b)).as("contains %s", pair).doesNotThrowAnyException();
        assertThatCode(() -> bool("SELECT ?.isWithin(?) AS v", a, b)).as("isWithin %s", pair).doesNotThrowAnyException();
      }
  }

  @Test
  void anIndexedWithinFindsTheRecordHoldingTheShape() {
    database.command("sql", "CREATE DOCUMENT TYPE L");
    database.command("sql", "CREATE PROPERTY L.coords STRING");
    database.command("sql", "CREATE INDEX ON L (coords) GEOSPATIAL");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO L SET name = 'pt', coords = ?", PT);
      database.command("sql", "INSERT INTO L SET name = 'box', coords = ?", BOX);
      database.command("sql", "INSERT INTO L SET name = 'tri', coords = ?", TRI);
      database.command("sql", "INSERT INTO L SET name = 'line', coords = ?", LINE);
    });

    // the answers below are only the index's if the plan fetches through it, and not through the scan checked further down
    for (final String shape : List.of(PT, BOX, TRI, LINE))
      assertThat(planUsesIndex("SELECT name FROM L WHERE geo.within(coords, ?) = true", shape)).as("index used for %s", shape)
          .isTrue();
    assertThat(planUsesIndex("SELECT name FROM L WHERE geo.within(coords.trim(), ?) = true", PT)).isFalse();

    assertThat(names("SELECT name FROM L WHERE geo.within(coords, ?) = true", PT)).containsExactly("pt");
    assertThat(names("SELECT name FROM L WHERE geo.within(coords, ?) = true", BOX)).containsExactlyInAnyOrder("box", "line",
        "tri", "pt");
    assertThat(names("SELECT name FROM L WHERE geo.within(coords, ?) = true", TRI)).containsExactly("tri");
    assertThat(names("SELECT name FROM L WHERE geo.within(coords, ?) = true", LINE)).containsExactly("line");
    // the same through the scan, without the index
    assertThat(names("SELECT name FROM L WHERE geo.within(coords.trim(), ?) = true", PT)).containsExactly("pt");
    assertThat(names("SELECT name FROM L WHERE coords.isWithin(?) = true", TRI)).containsExactly("tri");
  }

  private boolean bool(final String sql, final Object... args) {
    try (final ResultSet result = database.query("sql", sql, args)) {
      return result.next().<Boolean>getProperty("v");
    }
  }

  private boolean planUsesIndex(final String sql, final Object... args) {
    try (final ResultSet result = database.query("sql", sql, args)) {
      while (result.hasNext())
        result.next();
      return result.getExecutionPlan().orElseThrow().prettyPrint(0, 2).contains("FETCH FROM INDEXED FUNCTION");
    }
  }

  private List<String> names(final String sql, final Object... args) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet result = database.query("sql", sql, args)) {
      result.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return names;
  }
}
