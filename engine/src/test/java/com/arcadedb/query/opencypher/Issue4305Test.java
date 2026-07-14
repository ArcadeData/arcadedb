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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproducer for <a href="https://github.com/ArcadeData/arcadedb/issues/4305">issue #4305</a>:
 * point() over a latitude property surfaced as String triggered an internal ClassCastException.
 * <p>
 * The user's scenario is a database where the {@code City.lat} property is declared as
 * {@link Type#STRING} (or otherwise gets stored as a String). When they call
 * {@code point({latitude: c.lat, longitude: c.lon})}, the function fails with a raw
 * {@link ClassCastException} from {@code java.lang.String cannot be cast to java.lang.Number}.
 * The fix coerces numeric strings to {@link Number} and otherwise raises a clean
 * {@link CommandExecutionException} naming the offending key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4305Test extends TestHelper {
  @Test
  void propertyNamedLatStaysNumeric() {
    database.command("opencypher", "CREATE (:City {lat: 1.0, lon: 2.0})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:City) RETURN c.lat AS lat, c.lon AS lon")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("lat")).isInstanceOf(Number.class);
      assertThat(row.<Object>getProperty("lon")).isInstanceOf(Number.class);
    }
  }

  @Test
  void pointFromLatLonProperties() {
    database.command("opencypher", "CREATE (:City {lat: 1.0, lon: 2.0})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:City) RETURN point({latitude: c.lat, longitude: c.lon}) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      final Object point = rs.next().getProperty("p");
      assertThat(point).isInstanceOf(Map.class);
      final Map<?, ?> p = (Map<?, ?>) point;
      assertThat(((Number) p.get("latitude")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) p.get("longitude")).doubleValue()).isEqualTo(2.0);
    }
  }

  /**
   * The actual reproducer for the reported ClassCastException: declare the schema with
   * {@code lat} as a STRING so writes are coerced to {@link String}. point() must coerce
   * a numeric string back to a number rather than crashing.
   */
  @Test
  void pointCoercesNumericStringFromSchemaTypedStringProperty() {
    final VertexType city = database.getSchema().createVertexType("City");
    city.createProperty("lat", Type.STRING);
    city.createProperty("lon", Type.DOUBLE);
    database.command("opencypher", "CREATE (:City {lat: 1.0, lon: 2.0})");

    try (final ResultSet check = database.query("opencypher",
        "MATCH (c:City) RETURN c.lat AS lat, c.lon AS lon")) {
      assertThat(check.hasNext()).isTrue();
      final Result row = check.next();
      // lat is stored as String because the schema declares it so - reproduce the user's state
      assertThat(row.<Object>getProperty("lat")).isInstanceOf(String.class);
      assertThat(row.<Object>getProperty("lon")).isInstanceOf(Number.class);
    }

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:City) RETURN point({latitude: c.lat, longitude: c.lon}) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      final Object point = rs.next().getProperty("p");
      assertThat(point).as("point() must not crash on a numeric string and must return a usable point")
          .isInstanceOf(Map.class);
      final Map<?, ?> p = (Map<?, ?>) point;
      assertThat(((Number) p.get("latitude")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) p.get("longitude")).doubleValue()).isEqualTo(2.0);
    }
  }

  /**
   * When the value cannot be coerced (non-numeric string), point() must raise a clean,
   * user-facing error naming the bad key - never a raw ClassCastException.
   */
  @Test
  void pointRaisesCleanErrorOnNonNumericString() {
    final VertexType city = database.getSchema().createVertexType("Place");
    city.createProperty("lat", Type.STRING);
    city.createProperty("lon", Type.STRING);
    database.command("opencypher", "CREATE (:Place {lat: 'abc', lon: 'def'})");

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Place) RETURN point({latitude: p.lat, longitude: p.lon}) AS pt")) {
        rs.stream().toList();
      }
    })
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("point()")
        .hasMessageMatching("(?s).*(latitude|longitude).*")
        .hasMessageMatching("(?s).*(abc|def).*");
  }

  @Test
  void pointFromXvYvPropertiesControl() {
    // From the issue: using property names other than lat/lon works fine.
    database.command("opencypher", "CREATE (:Town {xv: 1.0, yv: 2.0})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (c:Town) RETURN point({latitude: c.xv, longitude: c.yv}) AS p")) {
      assertThat(rs.hasNext()).isTrue();
      final Object point = rs.next().getProperty("p");
      assertThat(point).isInstanceOf(Map.class);
      final Map<?, ?> p = (Map<?, ?>) point;
      assertThat(((Number) p.get("latitude")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) p.get("longitude")).doubleValue()).isEqualTo(2.0);
    }
  }
}
