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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12273: point.distance() and point.withinBBox() ignored the height/z of 3D points and point.distance() used a
 * different Earth radius than Neo4j. Expected values are the ones Neo4j 5 returns.
 */
class CypherPointDistance3dIssue12273Test extends TestHelper {

  private Object value(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS v")) {
      return rs.next().getProperty("v");
    }
  }

  private double distance(final String expression) {
    return ((Number) value(expression)).doubleValue();
  }

  @Test
  void wgs84DistanceUsesNeo4jRadius() {
    assertThat(distance("point.distance(point({longitude:0,latitude:0}), point({longitude:1,latitude:1}))"))
        .isCloseTo(157425.61, within(0.01));
    assertThat(distance("point.distance(point({longitude:0,latitude:90}), point({longitude:0,latitude:-90}))"))
        .isCloseTo(20037517.77, within(0.01));
  }

  @Test
  void wgs843dDistanceUsesHeight() {
    assertThat(distance(
        "point.distance(point({longitude:0,latitude:0,height:0}), point({longitude:0,latitude:0,height:1000}))"))
        .isCloseTo(1000.0, within(0.001));
    assertThat(distance(
        "point.distance(point({longitude:0,latitude:0,height:0}), point({longitude:1,latitude:0,height:1000}))"))
        .isCloseTo(111332.76, within(0.01));
    assertThat(distance(
        "point.distance(point({longitude:0,latitude:0,height:6371000}), point({longitude:90,latitude:0,height:6371000}))"))
        .isCloseTo(20026302.28, within(0.01));
  }

  @Test
  void mixedDimensionsGiveNull() {
    assertThat(value("point.distance(point({longitude:0,latitude:0}), point({longitude:1,latitude:0,height:1000}))")).isNull();
    assertThat(value("point.distance(point({x:0,y:0,z:0}), point({x:3,y:4}))")).isNull();
    assertThat(value("point.withinBBox(point({x:1,y:1}), point({x:0,y:0,z:0}), point({x:2,y:2,z:2}))")).isNull();
    assertThat(value("point.withinBBox(point({x:1,y:1,z:1}), point({x:0,y:0}), point({x:2,y:2}))")).isNull();
  }

  @Test
  void withinBBoxChecksThirdCoordinate() {
    assertThat(value("point.withinBBox(point({x:1,y:1,z:5}), point({x:0,y:0,z:0}), point({x:2,y:2,z:2}))")).isEqualTo(false);
    assertThat(value(
        "point.withinBBox(point({longitude:1,latitude:1,height:50}), point({longitude:0,latitude:0,height:0}), point({longitude:2,latitude:2,height:10}))"))
        .isEqualTo(false);
    assertThat(value("point.withinBBox(point({x:1,y:1,z:1}), point({x:0,y:0,z:0}), point({x:2,y:2,z:2}))")).isEqualTo(true);
    assertThat(value(
        "point.withinBBox(point({longitude:1,latitude:1,height:5}), point({longitude:0,latitude:0,height:0}), point({longitude:2,latitude:2,height:10}))"))
        .isEqualTo(true);
  }
}
