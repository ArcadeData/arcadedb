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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9074: a HASH index created with NULL_STRATEGY ERROR accepted records whose indexed property was
 * null or missing, while an LSM index refuses them at commit. The strategy was stored on the bucket but never checked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9074HashIndexNullStrategyTest extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH", "NOTUNIQUE", "UNIQUE" })
  void errorStrategyRejectsNullAndMissingKeys(final String indexType) {
    createType(indexType, "ERROR");

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 1").close()))
        .isInstanceOf(TransactionException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 2, a = null").close()))
        .isInstanceOf(TransactionException.class);
    assertThat(count("SELECT count(*) AS c FROM T")).isZero();

    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close());
    assertThat(count("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  void errorStrategyRejectsAnUpdateToNull(final String indexType) {
    createType(indexType, "ERROR");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE T SET a = null WHERE b = 3").close()))
        .isInstanceOf(TransactionException.class);
    assertThat(count("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  void skipStrategyKeepsAcceptingNulls(final String indexType) {
    createType(indexType, "SKIP");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 1").close();
      database.command("sql", "INSERT INTO T SET b = 2, a = null").close();
      database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close();
    });
    assertThat(count("SELECT count(*) AS c FROM T")).isEqualTo(3);
    assertThat(count("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  void indexStrategyKeepsAcceptingNulls(final String indexType) {
    createType(indexType, "INDEX");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 1").close();
      database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close();
    });
    assertThat(count("SELECT count(*) AS c FROM T")).isEqualTo(2);
    assertThat(count("SELECT count(*) AS c FROM T WHERE a IS NULL")).isEqualTo(1);
  }

  private void createType(final String indexType, final String nullStrategy) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING");
      database.command("sql", "CREATE INDEX ON T (a) " + indexType + " NULL_STRATEGY " + nullStrategy).close();
    });
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Long>getProperty("c");
    }
  }
}
