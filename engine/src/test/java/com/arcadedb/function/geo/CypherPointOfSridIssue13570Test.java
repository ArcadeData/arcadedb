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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13570: the point a wire protocol receives as an srid and its coordinates is the very map {@code point()} builds
 * for them, key for key and in the same order, so the two are equal, hash alike and are one value to {@code DISTINCT}.
 */
class CypherPointOfSridIssue13570Test {
  private static final CypherPointFunction POINT = new CypherPointFunction();

  @Test
  void aWgs84PointIsTheMapPointBuilds() {
    final Map<String, Object> received = CypherPointFunction.ofSrid(4326, 12.5, 41.9, null);

    assertSameMap(received, point("x", 12.5, "y", 41.9, "srid", 4326));
    assertSameMap(received, point("longitude", 12.5, "latitude", 41.9));
    assertThat(received).containsEntry("longitude", 12.5).containsEntry("latitude", 41.9)
        .containsEntry("crs", CypherPointFunction.CRS_WGS84_2D).doesNotContainKey("z");
  }

  @Test
  void aWgs84PointWithAHeightIsTheMapPointBuilds() {
    final Map<String, Object> received = CypherPointFunction.ofSrid(4979, 12.5, 41.9, 30.0);

    assertSameMap(received, point("x", 12.5, "y", 41.9, "z", 30.0, "srid", 4979));
    assertSameMap(received, point("longitude", 12.5, "latitude", 41.9, "height", 30.0));
    assertThat(received).containsEntry("height", 30.0).containsEntry("z", 30.0)
        .containsEntry("crs", CypherPointFunction.CRS_WGS84_3D);
  }

  @Test
  void theLongitudeOfAWgs84PointIsWrappedAsPointWrapsIt() {
    final Map<String, Object> received = CypherPointFunction.ofSrid(4326, 181.0, 10.0, null);

    assertSameMap(received, point("x", 181.0, "y", 10.0, "srid", 4326));
    assertThat(received).containsEntry("x", -179.0).containsEntry("longitude", -179.0);
  }

  @Test
  void aCartesianPointIsTheMapPointBuilds() {
    final Map<String, Object> flat = CypherPointFunction.ofSrid(7203, 1.0, 2.0, null);
    assertSameMap(flat, point("x", 1.0, "y", 2.0));
    assertThat(flat).containsEntry("crs", CypherPointFunction.CRS_CARTESIAN_2D).containsEntry("srid", 7203)
        .doesNotContainKeys("longitude", "latitude", "z");

    final Map<String, Object> solid = CypherPointFunction.ofSrid(9157, 1.0, 2.0, 3.0);
    assertSameMap(solid, point("x", 1.0, "y", 2.0, "z", 3.0));
    assertThat(solid).containsEntry("crs", CypherPointFunction.CRS_CARTESIAN_3D).containsEntry("srid", 9157)
        .containsEntry("z", 3.0);
  }

  /** point() refuses these two; a decoder cannot, so they keep the coordinates as sent, in the plain form. */
  @Test
  void aWgs84PointThatPointWouldRefuseKeepsThePlainForm() {
    final Map<String, Object> latitudeOutOfRange = CypherPointFunction.ofSrid(4326, 10.0, 91.0, null);
    assertThat(latitudeOutOfRange).containsEntry("x", 10.0).containsEntry("y", 91.0).containsEntry("srid", 4326)
        .doesNotContainKeys("longitude", "latitude");

    final Map<String, Object> belowTheSouthPole = CypherPointFunction.ofSrid(4326, 10.0, -91.0, null);
    assertThat(belowTheSouthPole).containsEntry("y", -91.0).doesNotContainKeys("longitude", "latitude");

    final Map<String, Object> heightOnA2dSrid = CypherPointFunction.ofSrid(4326, 10.0, 20.0, 5.0);
    assertThat(heightOnA2dSrid).containsEntry("x", 10.0).containsEntry("y", 20.0).containsEntry("z", 5.0)
        .containsEntry("srid", 4326).doesNotContainKeys("longitude", "latitude", "height");

    final Map<String, Object> noHeightOnA3dSrid = CypherPointFunction.ofSrid(4979, 10.0, 20.0, null);
    assertThat(noHeightOnA3dSrid).containsEntry("x", 10.0).containsEntry("y", 20.0).containsEntry("srid", 4979)
        .doesNotContainKeys("longitude", "latitude", "height", "z");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> point(final Object... keysAndValues) {
    final Map<String, Object> argument = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2)
      argument.put((String) keysAndValues[i], keysAndValues[i + 1]);
    return (Map<String, Object>) POINT.execute(new Object[] { argument }, null);
  }

  /** Equal, and with the keys in the same order: a point is serialized and rendered in the order of its map. */
  private static void assertSameMap(final Map<String, Object> actual, final Map<String, Object> expected) {
    assertThat(actual).isEqualTo(expected);
    assertThat(actual.hashCode()).isEqualTo(expected.hashCode());
    assertThat(new ArrayList<>(actual.keySet())).isEqualTo(new ArrayList<>(expected.keySet()));
  }
}
