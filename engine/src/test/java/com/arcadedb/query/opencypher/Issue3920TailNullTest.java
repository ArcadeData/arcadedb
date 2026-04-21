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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3920.
 * <p>
 * Cypher {@code tail(null)} must return {@code null}, not an empty list.
 * Null propagates through list-head/tail functions per the Cypher spec.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3920TailNullTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-tail-null-3920").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void tailWithNullReturnsNull() {
    final ResultSet rs = database.query("opencypher", "RETURN tail(null) AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }

  @SuppressWarnings("unchecked")
  @Test
  void tailWithListReturnsRest() {
    final ResultSet rs = database.query("opencypher", "RETURN tail([1, 2, 3]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("result");
    assertThat(list).containsExactly(2L, 3L);
  }

  @SuppressWarnings("unchecked")
  @Test
  void tailWithSingleElementReturnsEmptyList() {
    final ResultSet rs = database.query("opencypher", "RETURN tail([1]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("result");
    assertThat(list).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  void tailWithEmptyListReturnsEmptyList() {
    final ResultSet rs = database.query("opencypher", "RETURN tail([]) AS result");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("result");
    assertThat(list).isEmpty();
  }
}
