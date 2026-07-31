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
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.geo.SQLFunctionGeoPredicate.GeoCandidateIterator;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle coverage for the per-bucket chain the geospatial predicates stream through (#5601). The happy path is
 * exercised end to end by the SQL tests; what is pinned here is the behaviour a planned query does not reach on its
 * own: exhaustion, an early close, a re-entry after close, and a bucket list holding something that is not a
 * geospatial index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GeoCandidateIteratorTest extends TestHelper {
  private static final String SEARCH_BOX = "POLYGON ((5 35, 20 35, 20 48, 5 48, 5 35))";

  @Test
  void nextThrowsOnceEveryBucketIsDrained() {
    createCities();

    final GeoCandidateIterator iterator = new GeoCandidateIterator(geoBucketIndexes(), searchShape());

    final List<Record> emitted = new ArrayList<>();
    while (iterator.hasNext())
      emitted.add(iterator.next());

    assertThat(emitted).isNotEmpty();
    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void closingMidIterationEndsTheChainAndIsIdempotent() {
    createCities();

    final GeoCandidateIterator iterator = new GeoCandidateIterator(geoBucketIndexes(), searchShape());
    assertThat(iterator.hasNext()).isTrue();
    iterator.next();

    iterator.close();

    assertThat(iterator.hasNext()).as("a closed chain must not resume").isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);

    // a second close must not reopen anything nor fail
    iterator.close();
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void closingBeforeTheFirstRowIsSafe() {
    createCities();

    final GeoCandidateIterator iterator = new GeoCandidateIterator(geoBucketIndexes(), searchShape());
    iterator.close();

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void anEmptyBucketListYieldsNothing() {
    createCities();

    final GeoCandidateIterator iterator = new GeoCandidateIterator(Collections.emptyList(), searchShape());

    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void aBucketThatIsNotAGeospatialIndexIsSkipped() {
    createCities();
    database.command("sql", "CREATE INDEX ON City (name) NOTUNIQUE");

    final List<Index> nonGeo = new ArrayList<>();
    for (final TypeIndex typeIndex : database.getSchema().getType("City").getAllIndexes(true))
      if (typeIndex.getType() != Schema.INDEX_TYPE.GEOSPATIAL)
        nonGeo.addAll(Arrays.asList(typeIndex.getIndexesOnBuckets()));
    assertThat(nonGeo).isNotEmpty();

    // a non-geospatial bucket contributes no cursor; the chain must step over it rather than spin or throw
    final GeoCandidateIterator iterator = new GeoCandidateIterator(nonGeo, searchShape());

    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void aNullBucketIsToleratedRatherThanRaising() {
    createCities();

    final List<Index> withNull = new ArrayList<>();
    withNull.add(null);
    withNull.addAll(geoBucketIndexes());

    // Arrays.asList over getIndexesOnBuckets() can carry a null on a schema anomaly; the eager loop this chain
    // replaced simply skipped it, so it must not become an NPE on the query path
    final GeoCandidateIterator iterator = new GeoCandidateIterator(withNull, searchShape());

    final List<Record> emitted = new ArrayList<>();
    while (iterator.hasNext())
      emitted.add(iterator.next());

    assertThat(emitted).isNotEmpty();
  }

  // ---- helpers ----

  private Shape searchShape() {
    return GeoUtils.parseGeometry(SEARCH_BOX);
  }

  private List<Index> geoBucketIndexes() {
    final List<Index> result = new ArrayList<>();
    for (final TypeIndex typeIndex : database.getSchema().getType("City").getAllIndexes(true))
      if (typeIndex.getType() == Schema.INDEX_TYPE.GEOSPATIAL)
        result.addAll(Arrays.asList(typeIndex.getIndexesOnBuckets()));
    assertThat(result).isNotEmpty();
    return result;
  }

  private void createCities() {
    database.command("sql", "CREATE DOCUMENT TYPE City");
    database.command("sql", "CREATE PROPERTY City.name STRING");
    database.command("sql", "CREATE PROPERTY City.coords STRING");
    database.command("sql", "CREATE INDEX ON City (coords) GEOSPATIAL");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO City SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO City SET name = 'Naples', coords = 'POINT (14.3 40.8)'");
      database.command("sql", "INSERT INTO City SET name = 'Florence', coords = 'POINT (11.3 43.8)'");
      database.command("sql", "INSERT INTO City SET name = 'Bari', coords = 'POINT (16.9 41.1)'");
    });
  }
}
