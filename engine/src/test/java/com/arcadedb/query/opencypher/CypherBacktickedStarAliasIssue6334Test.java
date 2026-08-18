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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6334: a projection aliased to the backticked variable {@code `*`} was mistaken for
 * {@code WITH *} and forwarded the incoming scope instead of resetting it to the one name it projects, while its own
 * alias was never projected at all.
 * <p>
 * The star is now a property of the projection item - the parser builds it, and only the parser - rather than of the
 * name it carries, so a user variable that happens to be spelled {@code *} is an ordinary projection. That also
 * settles the one case the name-based test could never settle: the bare {@code WITH `*`}, which reads a variable
 * called {@code *} out of scope rather than forwarding everything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherBacktickedStarAliasIssue6334Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-backticked-star-alias-6334");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", "CREATE (a:P {name:'a'})-[:R]->(b:P {name:'b'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void anAliasNamedStarResetsTheScopeLikeAnyOtherAlias() {
    // After WITH n AS `*` the name n is out of scope, so the second MATCH binds a fresh n over both :P nodes.
    assertThat(count("MATCH (n:P {name:'a'}) WITH n AS `*` MATCH (n:P) RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void anOrdinaryAliasBehavesTheSameWay() {
    assertThat(count("MATCH (n:P {name:'a'}) WITH n AS other MATCH (n:P) RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void anAliasNamedStarIsProjected() {
    assertThat(single("MATCH (n:P {name:'a'}) WITH n AS `*` RETURN `*`.name AS v")).isEqualTo("a");
  }

  @Test
  void aVariableNamedStarCanBeProjectedOnwards() {
    assertThat(single("MATCH (n:P {name:'a'}) WITH n AS `*` WITH `*` AS x RETURN x.name AS v")).isEqualTo("a");
  }

  @Test
  void theRealStarStillForwardsTheScope() {
    // WITH * keeps n bound, so the second MATCH is answered from the single node the row already holds.
    assertThat(count("MATCH (n:P {name:'a'}) WITH * MATCH (n:P) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void theRealStarStillForwardsWhenCombinedWithAProjection() {
    assertThat(count("MATCH (n:P {name:'a'}) WITH *, n.name AS nm MATCH (n:P) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void twoColumnsNamedStarStillCollide() {
    // The duplicate-column check used to exempt any item whose name was "*", real star or not, so two projections
    // aliased to the backticked star slipped past it - the same name-vs-item confusion this issue is about.
    assertThatThrownBy(() -> database.query("opencypher",
        "MATCH (n:P) RETURN n.name AS `*`, n.name AS `*`"))
        .hasMessageContaining("ColumnNameConflict");
  }

  @Test
  void theRealStarIsNotAColumnAndCollidesWithNothing() {
    assertThat(count("MATCH (n:P {name:'a'}) WITH *, n.name AS nm RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void returnStarStillProjectsEverythingInScope() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:P {name:'a'}) RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getPropertyNames()).contains("n");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("v");
    }
  }
}
