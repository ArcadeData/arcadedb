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
import com.arcadedb.database.Identifiable;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for #5601 (2): the geospatial query path now streams. The cursor returned by
 * {@code LSMTreeGeoIndex.get()} chains the covering cells of the search shape on demand, and
 * {@code SQLFunctionGeoPredicate.searchFromTarget} chains the per-bucket cursors the same way instead of loading every
 * candidate RECORD of every bucket before the geo.* re-check sees the first row.
 * <p>
 * These tests pin the behaviour that laziness must NOT change: the same rows, in whatever order, whether the cursor is
 * drained, abandoned halfway or closed early. The per-cell mechanics are unit-tested in {@link GeoIndexCursorTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeGeoIndexLazyCursorTest extends TestHelper {
  private static final String TYPE_NAME = "City";
  // a box wide enough that its decomposition covers many cells, so a partial read really does leave cells unvisited
  private static final String SEARCH_BOX = "POLYGON ((5 35, 20 35, 20 48, 5 48, 5 35))";

  @Test
  void aPartiallyReadCursorReturnsTheSameRowsAsAFullyDrainedOne() {
    createCities();

    final Set<String> full = new HashSet<>();
    forEachCandidate(rid -> full.add(rid.getRecord().asDocument().getString("name")));

    // the index answers with a SUPERSET of the match (the grid approximates the shape), so assert on what must be
    // there rather than on the exact candidate set, which depends on the grid precision
    assertThat(full).contains("Rome", "Naples", "Florence", "Bari");

    // read a single row, then abandon the cursor: nothing may be left in a state that breaks the next query
    for (final Index bucketIndex : geoIndexes()) {
      final IndexCursor cursor = bucketIndex.get(new Object[] { SEARCH_BOX });
      if (cursor.hasNext()) {
        assertThat(cursor.next()).isNotNull();
        cursor.close();
        break;
      }
    }

    final Set<String> again = new HashSet<>();
    forEachCandidate(rid -> again.add(rid.getRecord().asDocument().getString("name")));
    assertThat(again).isEqualTo(full);
  }

  @Test
  void closingACursorBeforeItIsDrainedEndsIt() {
    createCities();

    for (final Index bucketIndex : geoIndexes()) {
      final IndexCursor cursor = bucketIndex.get(new Object[] { SEARCH_BOX });
      cursor.close();
      assertThat(cursor.hasNext()).isFalse();
    }
  }

  @Test
  void aLimitedQueryReturnsAsManyRowsAsAsked() {
    createCities();

    final ResultSet limited = database.query("sql",
        "SELECT name FROM " + TYPE_NAME + " WHERE geo.within(coords, geo.geomFromText('" + SEARCH_BOX + "')) = true LIMIT 2");

    final List<String> names = new ArrayList<>();
    while (limited.hasNext())
      names.add(limited.next().getProperty("name"));

    assertThat(names).hasSize(2);
    assertThat(names).allSatisfy(n -> assertThat(n).isIn("Rome", "Naples", "Florence", "Bari"));
  }

  @Test
  void aPolygonIndexedUnderManyCellsIsReturnedOnce() {
    database.command("sql", "CREATE DOCUMENT TYPE Region");
    database.command("sql", "CREATE PROPERTY Region.name STRING");
    database.command("sql", "CREATE PROPERTY Region.area STRING");
    database.command("sql", "CREATE INDEX ON Region (area) GEOSPATIAL");

    database.transaction(() -> database.command("sql",
        "INSERT INTO Region SET name = 'Lazio', area = 'POLYGON ((11 41, 14 41, 14 43, 11 43, 11 41))'"));

    // A polygon decomposes into MANY cells, several of which the search shape also covers: without the cursor's
    // seen-set the same record would be emitted once per shared cell.
    final ResultSet result = database.query("sql",
        "SELECT name FROM Region WHERE geo.intersects(area, geo.geomFromText('" + SEARCH_BOX + "')) = true");

    final List<String> names = new ArrayList<>();
    while (result.hasNext())
      names.add(result.next().getProperty("name"));

    assertThat(names).containsExactly("Lazio");
  }

  @Test
  void deletedRecordsAreNotReturnedByAStreamingCursor() {
    createCities();

    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE name = 'Rome'"));

    final Set<String> names = new HashSet<>();
    forEachCandidate(rid -> names.add(rid.getRecord().asDocument().getString("name")));

    assertThat(names).doesNotContain("Rome");
    assertThat(names).contains("Naples", "Florence", "Bari");
  }

  // ---- helpers ----

  private void createCities() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".coords STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'Florence', coords = 'POINT (11.3 43.8)'");
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'Bari', coords = 'POINT (16.9 41.1)'");
      // outside the search box
      database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = 'Oslo', coords = 'POINT (10.7 59.9)'");
    });
  }

  private List<Index> geoIndexes() {
    final List<Index> result = new ArrayList<>();
    for (final TypeIndex typeIndex : database.getSchema().getType(TYPE_NAME).getAllIndexes(true))
      if (typeIndex.getType() == Schema.INDEX_TYPE.GEOSPATIAL)
        result.addAll(List.of(typeIndex.getIndexesOnBuckets()));
    return result;
  }

  private void forEachCandidate(final Consumer<Identifiable> consumer) {
    for (final Index bucketIndex : geoIndexes()) {
      final IndexCursor cursor = bucketIndex.get(new Object[] { SEARCH_BOX });
      try {
        while (cursor.hasNext()) {
          final Identifiable next = cursor.next();
          if (next != null)
            consumer.accept(next);
        }
      } finally {
        cursor.close();
      }
    }
  }
}
