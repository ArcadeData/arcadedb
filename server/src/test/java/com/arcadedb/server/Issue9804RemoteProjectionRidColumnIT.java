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
package com.arcadedb.server;

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9804, end to end: the Java remote driver deleted the {@code @rid} (and {@code @type}, {@code @in}, {@code @out}) column of
 * every projection row, so {@code SELECT @rid FROM Person} answered a row with no columns at all. The driver always asks for the
 * {@code @props} type hint, and a {@code @rid} column is a LINK, which arms it. The row embedded and the row over HTTP must agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9804RemoteProjectionRidColumnIT extends BaseGraphServerTest {

  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void selectingTheRidKeepsTheRidColumnOverTheRemoteDriver() {
    try (final RemoteDatabase database = remote()) {
      database.command("sql", "CREATE VERTEX TYPE Person9804");
      database.command("sql", "CREATE PROPERTY Person9804.name STRING");
      database.command("sql", "CREATE PROPERTY Person9804.age LONG");
      database.command("sql", "INSERT INTO Person9804 SET name = 'Jay', age = 42");

      try (final ResultSet resultSet = database.query("sql", "SELECT @rid, @type, name, age FROM Person9804")) {
        assertThat(resultSet.hasNext()).isTrue();
        final Result row = resultSet.next();
        assertThat(row.getPropertyNames()).containsExactly("@rid", "@type", "name", "age");
        assertThat(row.<Object>getProperty("@rid")).isInstanceOf(RID.class);
        assertThat(row.<String>getProperty("@type")).isEqualTo("Person9804");
        assertThat(row.<String>getProperty("name")).isEqualTo("Jay");
        assertThat(row.<Object>getProperty("age")).isEqualTo(42L);
      }

      try (final ResultSet resultSet = database.query("sql", "SELECT @rid FROM Person9804")) {
        final Result row = resultSet.next();
        assertThat(row.getPropertyNames()).as("a row of one LINK column, which arms the @props hint by itself")
            .containsExactly("@rid");
        assertThat(row.<Object>getProperty("@rid")).isNotNull();
      }

      try (final ResultSet resultSet = database.query("sql", "SELECT @rid AS r, name FROM Person9804")) {
        assertThat(resultSet.next().getPropertyNames()).as("the same query with the column renamed").containsExactly("r", "name");
      }
    }
  }
}
