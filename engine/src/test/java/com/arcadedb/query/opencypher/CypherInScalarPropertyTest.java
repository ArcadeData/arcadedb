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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13563: {@code x IN n.p} compares a scalar stored value with x as a one-element list, as Neo4j does (every count below is
 * what Neo4j 5.26 answers); a right side whose type is known (a literal, a parameter) is still refused (#10541).
 */
class CypherInScalarPropertyTest extends TestHelper {
  @Test
  void aScalarPropertyOnTheRightIsAOneElementList() {
    database.getSchema().createVertexType("In");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:In {v: 'Accepted'}), (:In {v: 'Other'}), (:In {w: 1}), (:In {v: ['Accepted', 'x']})").close());

    assertThat(count("MATCH (n:In) WHERE $p IN n.v RETURN count(n) AS c", Map.of("p", "Accepted"))).isEqualTo(2);
    assertThat(count("MATCH (n:In) WHERE 'Accepted' IN n.v RETURN count(n) AS c", Map.of())).isEqualTo(2);
    assertThat(count("MATCH (n:In) WHERE NOT 'Other' IN n.v RETURN count(n) AS c", Map.of())).isEqualTo(2);
    assertThat(count("MATCH (n:In) RETURN sum(CASE WHEN $p IN n.v THEN 1 ELSE 0 END) AS c", Map.of("p", "Accepted"))).isEqualTo(2);
  }

  @Test
  void aLiteralOrAParameterThatIsNoListIsStillRefused() {
    assertThatThrownBy(() -> count("RETURN 'a' IN 'a' AS c", Map.of())).hasMessageContaining("IN requires a list");
    assertThatThrownBy(() -> count("RETURN 'a' IN $x AS c", Map.of("x", "a"))).hasMessageContaining("IN requires a list");
  }

  private Object count(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      final Object value = rs.next().getProperty("c");
      return value instanceof Number n ? (Object) n.intValue() : value;
    }
  }
}
