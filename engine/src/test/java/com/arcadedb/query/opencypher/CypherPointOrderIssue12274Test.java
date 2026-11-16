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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12274: ORDER BY, min() and max() ordered points as maps (WGS-84 by latitude first, CRS by key count), and two equal
 * points were not {@code <=} each other. Expected values are the ones Neo4j 5 returns.
 */
class CypherPointOrderIssue12274Test extends TestHelper {

  private static final String WGS84 = "[point({longitude:10,latitude:5}), point({longitude:-10,latitude:1}), point({longitude:0,latitude:9})]";

  private Object value(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      return rs.next().getProperty("v");
    }
  }

  @Test
  void orderByWgs84ByLongitudeFirst() {
    assertThat(value("UNWIND " + WGS84 + " AS p WITH p ORDER BY p RETURN collect(p.longitude) AS v"))
        .isEqualTo(List.of(-10.0, 0.0, 10.0));
    assertThat(value("UNWIND [point({longitude:10,latitude:5,height:0}), point({longitude:-10,latitude:1,height:9}), "
        + "point({longitude:0,latitude:9,height:3})] AS p WITH p ORDER BY p RETURN collect(p.longitude) AS v"))
        .isEqualTo(List.of(-10.0, 0.0, 10.0));
  }

  @Test
  void orderByCrs() {
    assertThat(value("UNWIND [point({x:1,y:1}), point({x:1,y:1,z:0}), point({longitude:0,latitude:0}), "
        + "point({longitude:0,latitude:0,height:0})] AS p WITH p ORDER BY p RETURN collect(p.srid) AS v"))
        .isEqualTo(List.of(4326, 4979, 7203, 9157));
  }

  @Test
  void minAndMax() {
    assertThat(value("UNWIND " + WGS84 + " AS p WITH min(p) AS m, max(p) AS M RETURN [m.longitude, M.longitude] AS v"))
        .isEqualTo(List.of(-10.0, 10.0));
    assertThat(value("UNWIND [point({x:1,y:1}), point({longitude:0,latitude:0})] AS p WITH min(p) AS m, max(p) AS M "
        + "RETURN [m.srid, M.srid] AS v")).isEqualTo(List.of(4326, 7203));
  }

  @Test
  void equalPointsAreLessOrEqual() {
    assertThat(value("RETURN point({x:1,y:2}) <= point({x:1,y:2}) AS v")).isEqualTo(true);
    assertThat(value("RETURN point({longitude:1,latitude:2}) >= point({longitude:1,latitude:2}) AS v")).isEqualTo(true);
    assertThat(value("RETURN point({x:1,y:2}) <= point({x:1,y:3}) AS v")).isNull();
    assertThat(value("RETURN point({x:1,y:2}) < point({x:1,y:3}) AS v")).isNull();
  }
}
