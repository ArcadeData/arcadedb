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

/**
 * The column of an unaliased map projection, {@code RETURN n {.a}}, is named after the variable it projects, {@code n}, as
 * Neo4j names it (observed on Neo4j 5.26); {@code @neo4j/graphql} reads {@code record.this} of {@code RETURN this { ... }}. Any other
 * unaliased expression keeps the text that wrote it.
 */
class CypherMapProjectionColumnNameTest extends TestHelper {
  @Test
  void anUnaliasedMapProjectionIsNamedAfterItsVariable() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'a', age: 3})").close());

    for (final String projection : new String[] { "p { .name }", "p{.name}", "p { .name, older: p.age + 1 }", "p { .* }" }) {
      try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN " + projection)) {
        assertThat(rs.next().toMap()).as(projection).containsOnlyKeys("p");
      }
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN p { .name }")) {
      assertThat(rs.next().<Map<String, Object>>getProperty("p")).containsEntry("name", "a");
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN p.name, p { .name } AS m, {a: 1}, p.age + 1")) {
      assertThat(rs.next().toMap()).containsOnlyKeys("p.name", "m", "{a: 1}", "p.age + 1");
    }
  }
}
