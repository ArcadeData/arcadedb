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
 * Regression tests for issue #5146: pattern comprehension with combined relationship inline
 * property filter and target-node inline label/property filter must return the correct match.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherPatternComprehension5146Test extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher",
        """
        CREATE
          (src:Q {name: 'src'}),
          (t2:A {name: 't2', v: 10}),
          (t3:A:B {name: 't3', v: 20}),
          (t4:B {name: 't4', v: 30}),
          (src)-[:QE {w: 1}]->(t2),
          (src)-[:QE {w: 2}]->(t3),
          (src)-[:QE {w: 3}]->(t4)""");
  }

  @Test
  void combinedRelAndNodeInlineFilters() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Q {name: 'src'})
        RETURN [(a)-[:QE {w: 1}]->(b:A {v: 10}) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("t2");
  }

  @Test
  void relFilterAlone() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Q {name: 'src'})
        RETURN [(a)-[:QE {w: 1}]->(b) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("t2");
  }

  @Test
  void nodeFilterAlone() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Q {name: 'src'})
        RETURN [(a)-[:QE]->(b:A {v: 10}) | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("t2");
  }

  @Test
  void explicitWhereForm() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Q {name: 'src'})
        RETURN [(a)-[r:QE]->(b:A) WHERE r.w = 1 AND b.v = 10 | b.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("t2");
  }
}
