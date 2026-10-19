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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10194: an explicit NULL on a SQL INSERT is stored as NULL, but in Cypher a null in a property map means "no property" (Neo4j
 * never stores a null), so a property left null in {@code CREATE (n:T {df: null})} is absent and the schema default still fills it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10194CypherNullPropertyDefaultTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE VERTEX TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.df INTEGER (default 7)").close();
  }

  private Document only() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
      final Document doc = rs.next().getElement().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
      return doc;
    }
  }

  @Test
  void aNullInTheCreatePatternIsNoPropertyAndTheDefaultFillsIt() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, df: null})").close());

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void anOmittedPropertyGetsTheDefault() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1})").close());

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void aNullParameterIsNoPropertyEither() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("df", null);

    database.transaction(() -> database.command("opencypher", "CREATE (n:T {id: 1, df: $df})", parameters).close());

    assertThat(only().getInteger("df")).isEqualTo(7);
  }
}
