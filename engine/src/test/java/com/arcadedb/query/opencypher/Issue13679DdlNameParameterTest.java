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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13679: {@code CREATE INDEX $name ...}, {@code CREATE CONSTRAINT $name ...}, {@code DROP INDEX $name} and
 * {@code DROP CONSTRAINT $name} named the schema object with the text {@code $name}. neo4j-graphrag and langchain-neo4j send
 * every index statement that way. The expectations below were observed on Neo4j 5.26.33: the name is the value of the
 * parameter, a non-string value is {@code TypeError: Expected String, but got: <type>} ({@code NO_VALUE} for null) and an unbound
 * parameter is {@code ParameterMissing}.
 */
class Issue13679DdlNameParameterTest extends TestHelper {

  @Test
  void aRangeIndexTakesTheNameOfTheParameter() {
    ddl("CREATE INDEX $n FOR (n:A) ON (n.p)", "idx_a");
    assertThat(indexNames()).containsExactly("idx_a");
    // a second statement with another name is another index on another property, not a refused duplicate of "$n"
    ddl("CREATE INDEX $n FOR (n:A) ON (n.q)", "idx_a2");
    assertThat(indexNames()).containsExactlyInAnyOrder("idx_a", "idx_a2");
  }

  @Test
  void aVectorAndAFullTextIndexTakeTheNameOfTheParameterAndIfNotExistsLooksItUp() {
    final String vector = "CREATE VECTOR INDEX $n IF NOT EXISTS FOR (n:D) ON n.e "
        + "OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}";
    ddl(vector, "v_d");
    ddl(vector, "v_d");
    ddl("CREATE FULLTEXT INDEX $n IF NOT EXISTS FOR (n:C) ON EACH [n.t]", "ft_c");
    assertThat(indexNames()).containsExactlyInAnyOrder("v_d", "ft_c");

    final List<Map<String, Object>> shown = rows("SHOW INDEXES YIELD name, type RETURN name, type ORDER BY name", Map.of());
    assertThat(shown).extracting(r -> r.get("name")).containsExactly("ft_c", "v_d");
    assertThat(shown).extracting(r -> r.get("type")).containsExactly("FULLTEXT", "VECTOR");
  }

  @Test
  void aConstraintTakesTheNameOfTheParameter() {
    ddl("CREATE CONSTRAINT $n FOR (n:B) REQUIRE n.id IS UNIQUE", "c_b");
    final List<Map<String, Object>> shown = rows("SHOW CONSTRAINTS YIELD name RETURN name", Map.of());
    assertThat(shown).extracting(r -> r.get("name")).containsExactly("c_b");
    ddl("DROP CONSTRAINT $n", "c_b");
    assertThat(rows("SHOW CONSTRAINTS YIELD name RETURN name", Map.of())).isEmpty();
  }

  @Test
  void dropByParameterDropsTheIndexOfThatName() {
    ddl("CREATE INDEX $n FOR (n:A) ON (n.p)", "idx_a");
    ddl("CREATE INDEX $n FOR (n:A) ON (n.q)", "idx_b");
    ddl("DROP INDEX $n", "idx_a");
    assertThat(indexNames()).containsExactly("idx_b");
    // IF EXISTS on a name nothing has is not an error
    ddl("DROP INDEX $n IF EXISTS", "idx_a");
    assertThat(indexNames()).containsExactly("idx_b");
  }

  @Test
  void aNameThatIsNotAStringIsATypeError() {
    assertThatThrownBy(() -> database.command("opencypher", "CREATE INDEX $n FOR (n:A) ON (n.p)", Map.of("n", 5L)).close())
        .hasMessageContaining("Expected String, but got: Long");
    final Map<String, Object> noValue = new java.util.HashMap<>();
    noValue.put("n", null);
    assertThatThrownBy(() -> database.command("opencypher", "DROP INDEX $n IF EXISTS", noValue).close())
        .hasMessageContaining("Expected String, but got: NO_VALUE");
    assertThat(indexNames()).isEmpty();
  }

  @Test
  void aLiteralNameStillWorks() {
    database.command("opencypher", "CREATE INDEX lit_a FOR (n:A) ON (n.p)").close();
    assertThat(indexNames()).containsExactly("lit_a");
    database.command("opencypher", "DROP INDEX lit_a").close();
    assertThat(indexNames()).isEmpty();
  }

  private void ddl(final String statement, final String name) {
    database.command("opencypher", statement, Map.of("n", name)).close();
  }

  private List<Map<String, Object>> rows(final String statement, final Map<String, Object> parameters) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", statement, parameters)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.toMap());
      }
    }
    return rows;
  }

  private List<String> indexNames() {
    final List<String> names = new ArrayList<>();
    for (final Map<String, Object> row : rows("SHOW INDEXES YIELD name RETURN name", Map.of()))
      names.add((String) row.get("name"));
    return names;
  }
}
