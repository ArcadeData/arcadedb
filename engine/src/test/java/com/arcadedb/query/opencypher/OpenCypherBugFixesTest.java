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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for GitHub issues #3445-#3449.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherBugFixesTest extends TestHelper {

  // ===================== #3445: coll.indexOf with null should return null =====================

  @Test
  void testCollIndexOfWithNullValueReturnsNull() {
    // Issue #3445: coll.indexOf(['a'], null) should return null, not -1
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(['a'], null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void testCollIndexOfWithNullListReturnsNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(null, 'a') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  @Test
  void testCollIndexOfNormalBehavior() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.indexOf(['a', 'b', 'c'], 'b') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).longValue()).isEqualTo(1L);
    }
  }

  // ===================== #3446: coll.remove with out-of-bounds index should throw =====================

  @Test
  void testCollRemoveOutOfBoundsThrows() {
    // Issue #3446: coll.remove([1, 2], 10) should throw an error
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2], 10) AS result")) {
        rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void testCollRemoveNegativeIndexThrows() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2], -1) AS result")) {
        rs.next();
      }
    }).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void testCollRemoveNormalBehavior() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coll.remove([1, 2, 3], 1) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      @SuppressWarnings("unchecked")
      final List<Object> result = rs.next().getProperty("result");
      assertThat(result).hasSize(2);
      assertThat(((Number) result.get(0)).longValue()).isEqualTo(1L);
      assertThat(((Number) result.get(1)).longValue()).isEqualTo(3L);
    }
  }

  // ===================== #3447: isEmpty should not treat null as empty =====================

  @Test
  void testIsEmptyWithNullProperty() {
    // Issue #3447: isEmpty(p.address) where p.address is null should NOT return true
    database.transaction(() -> {
      database.getSchema().createVertexType("PersonTest");
      database.command("opencypher",
          "CREATE (p:PersonTest {name:'Jessica', address:''}), (q:PersonTest {name:'Keanu'})");
    });

    try (final ResultSet rs = database.command("opencypher",
        "MATCH (p:PersonTest) WHERE isEmpty(p.address) RETURN p.name AS result ORDER BY result")) {
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("result"));
      // Only Jessica has address='' (empty string), Keanu has no address property (null)
      assertThat(names).containsExactly("Jessica");
    }
  }

  @Test
  void testIsEmptyWithEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }
  }

  @Test
  void testIsEmptyWithNonEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('hello') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsEmptyWithNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ===================== #3448: toBooleanOrNull(1.5) should return null =====================

  @Test
  void testToBooleanOrNullWithFloat() {
    // Issue #3448: toBooleanOrNull(1.5) should return null, not true
    try (final ResultSet rs = database.command("opencypher",
        "RETURN toBooleanOrNull('not a boolean') AS str, toBooleanOrNull(1.5) AS float, toBooleanOrNull([]) AS array")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("str")).isNull();
      assertThat((Object) row.getProperty("float")).isNull();
      assertThat((Object) row.getProperty("array")).isNull();
    }
  }

  // ===================== #3449: size on vector should return vector length =====================

  @Test
  void testSizeOnVector() {
    // Issue #3449: size(vector([1, 2, 3, 4, 5], 5, INTEGER)) should return 5
    try (final ResultSet rs = database.command("opencypher",
        "RETURN size(vector([1, 2, 3, 4, 5], 5, INTEGER)) AS sizeResult")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("sizeResult")).longValue()).isEqualTo(5L);
    }
  }

  @Test
  void testSizeOnVectorThreeElements() {
    try (final ResultSet rs = database.command("opencypher",
        "RETURN size(vector([1.0, 2.0, 3.0], 3, FLOAT)) AS sizeResult")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("sizeResult")).longValue()).isEqualTo(3L);
    }
  }
}
