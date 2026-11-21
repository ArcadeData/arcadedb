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
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12762: {@code geo.buffer}'s {@code quadrantSegments} was unbounded (a few characters of query text became a
 * polygon of hundreds of millions of vertices) and a value below 1 was silently read as 1.
 */
class GeoBufferQuadrantSegmentsIssue12762Test extends TestHelper {

  @Test
  void aValueOutsideTheRangeIsRefusedBeforeAnythingIsBuilt() {
    for (final String value : new String[] { "0", "-1", "10001", "100000000", "2147483647" })
      assertThatThrownBy(() -> buffer(value)).as("quadrantSegments " + value).isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("quadrantSegments");
  }

  @Test
  void aValueInsideTheRangeStillBuilds() {
    assertThat(buffer("1")).isEqualTo("POLYGON ((1 0, 0 -1, -1 0, 0 1, 1 0))");
    // 4 * 8 + 1 coordinates, the JTS default
    assertThat(buffer("8").split(",")).hasSize(33);
    // the ceiling itself: 4 * 10000 + 1 coordinates
    assertThat(buffer("10000").split(",")).hasSize(40_001);
  }

  private String buffer(final String quadrantSegments) {
    try (final ResultSet rs = database.query("sql",
        "SELECT geo.asText(geo.buffer(geo.point(0, 0), 1, { quadrantSegments: " + quadrantSegments + " })) AS r")) {
      return rs.next().getProperty("r");
    }
  }
}
