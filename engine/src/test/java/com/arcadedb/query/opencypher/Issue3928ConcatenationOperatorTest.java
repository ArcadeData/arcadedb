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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3928.
 * <p>
 * Cypher 25 adds the GQL-standard {@code ||} concatenation operator which must
 * concatenate lists and strings (same semantics as {@code +}). Previously, the
 * parser silently dropped the right-hand side of a {@code ||} expression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3928ConcatenationOperatorTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-concat-3928").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationWithNull() {
    // GitHub issue #3928: [1, 2] || [3, null] must concatenate into [1, 2, 3, null].
    final ResultSet resultSet = database.query("opencypher", "RETURN [1, 2] || [3, null] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactly(1L, 2L, 3L, null);
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationBasic() {
    // GitHub issue #3921 (duplicate of #3928): [1, 2] || [3, 4] must concatenate into [1, 2, 3, 4].
    final ResultSet resultSet = database.query("opencypher", "RETURN [1, 2] || [3, 4] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactly(1L, 2L, 3L, 4L);
  }

  @Test
  void stringConcatenation() {
    final ResultSet resultSet = database.query("opencypher", "RETURN 'Hello ' || 'World' AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    assertThat(r.<String>getProperty("result")).isEqualTo("Hello World");
  }

  @Test
  void stringConcatenationWithPipeAndNull() {
    // GitHub issue #3926: null propagates through || - 'Hello' || null returns null
    final ResultSet resultSet = database.query("opencypher", "RETURN 'Hello' || null AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    assertThat(r.<Object>getProperty("result")).isNull();
    assertThat(resultSet.hasNext()).isFalse();
  }

  @Test
  void stringConcatenationWithSpaces() {
    // GitHub issue #3927: 'Alpha' || 'Beta' and chained 'Alpha' || ' ' || 'Beta'
    final ResultSet resultSet = database.query("opencypher",
        "RETURN 'Alpha' || 'Beta' AS result1, 'Alpha' || ' ' || 'Beta' AS result2");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    assertThat(r.<String>getProperty("result1")).isEqualTo("AlphaBeta");
    assertThat(r.<String>getProperty("result2")).isEqualTo("Alpha Beta");
    assertThat(resultSet.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void listConcatenationChained() {
    final ResultSet resultSet = database.query("opencypher", "RETURN [1] || [2] || [3] AS result");

    assertThat(resultSet.hasNext()).isTrue();
    final Result r = resultSet.next();
    final List<Object> list = (List<Object>) r.getProperty("result");
    assertThat(list).containsExactly(1L, 2L, 3L);
  }
}
