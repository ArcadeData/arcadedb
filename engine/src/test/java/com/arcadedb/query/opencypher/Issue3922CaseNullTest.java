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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3922.
 * <p>
 * In Cypher's three-valued logic, {@code null = null} evaluates to
 * {@code null}, not {@code true}. An extended CASE expression must therefore
 * never fire a WHEN branch when both operands are null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3922CaseNullTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-case-null-3922").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void caseNullWhenNullDoesNotMatch() {
    final ResultSet rs = database.query("opencypher",
        "RETURN CASE null WHEN null THEN 'matched' ELSE 'not_matched' END AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void caseValueWhenNullDoesNotMatch() {
    // Value on LHS, null WHEN: no match, ELSE fires
    final ResultSet rs = database.query("opencypher",
        "RETURN CASE 1 WHEN null THEN 'matched' ELSE 'not_matched' END AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
  }

  @Test
  void caseNullWhenValueDoesNotMatch() {
    // null on LHS, value WHEN: no match, ELSE fires
    final ResultSet rs = database.query("opencypher",
        "RETURN CASE null WHEN 1 THEN 'matched' ELSE 'not_matched' END AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("not_matched");
  }

  @Test
  void caseValueEqualsValueMatches() {
    // Sanity check: non-null equality still works
    final ResultSet rs = database.query("opencypher",
        "RETURN CASE 1 WHEN 1 THEN 'matched' ELSE 'not_matched' END AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("result")).isEqualTo("matched");
  }

  @Test
  void caseNullWithoutElseReturnsNull() {
    // No match and no ELSE -> null
    final ResultSet rs = database.query("opencypher",
        "RETURN CASE null WHEN null THEN 'matched' END AS result");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("result")).isNull();
  }
}
