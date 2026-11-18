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
package com.arcadedb.postgres;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11803: a statement refused for what the database holds now - a TRUNCATE or DROP of a type that still has records, a DROP
 * of an indexed property - answered {@code XX000} internal_error. It answers {@code 55006} object_in_use, and the other caller's
 * mistakes of the issue answer a class-22 or class-42 code instead of the server fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11803PostgresObjectInUseSqlStateTest extends TestHelper {

  @Test
  void aStatementRefusedForWhatTheDatabaseHoldsIsObjectInUse() {
    database.command("sql", "CREATE VERTEX TYPE V11803");
    database.command("sql", "CREATE PROPERTY V11803.id INTEGER");
    database.command("sql", "CREATE INDEX ON V11803 (id) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO V11803 SET id = 1"));

    for (final String statement : List.of("TRUNCATE TYPE V11803", "DROP TYPE V11803", "DROP PROPERTY V11803.id"))
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure(statement))).as(statement).isEqualTo("55006");

    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("SELECT FROM V11803 LIMIT 'a'"))).isEqualTo("42804");
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("ALTER PROPERTY V11803.id TYPE LONG"))).isEqualTo("42601");
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("REBUILD INDEX nosuch11803"))).isEqualTo("42P01");
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("CREATE EDGE V11803 FROM (SELECT FROM V11803) TO (SELECT FROM V11803)")))
        .isEqualTo("42P01");
  }

  private Throwable failure(final String statement) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.command("sql", statement)) {
        rs.stream().count();
      }
    });
    assertThat(error).as(statement).isNotNull();
    return error;
  }
}
