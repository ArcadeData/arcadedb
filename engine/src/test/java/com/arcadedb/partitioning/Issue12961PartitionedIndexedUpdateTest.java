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
package com.arcadedb.partitioning;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12961: on a type with partitioned bucket selection every update that changed ANY indexed property was refused
 * ("Cannot modify primary key"). Only a change of the partition key itself must be refused, and as a caller error.
 */
class Issue12961PartitionedIndexedUpdateTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Account BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Account.id INTEGER");
      database.command("sql", "CREATE PROPERTY Account.email STRING");
      database.command("sql", "CREATE PROPERTY Account.region STRING");
      database.command("sql", "CREATE INDEX ON Account (id) UNIQUE");
      database.command("sql", "CREATE INDEX ON Account (email) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Account (region, id) NOTUNIQUE");
      database.command("sql", "ALTER TYPE Account BUCKETSELECTIONSTRATEGY `partitioned('id')`");
      database.command("sql", "INSERT INTO Account SET id = 1, email = 'a@x', region = 'eu'");
    });
  }

  @Test
  void secondaryIndexedPropertiesCanBeUpdated() {
    database.transaction(() -> database.command("sql", "UPDATE Account SET email = 'b@x', region = 'us' WHERE id = 1"));
    assertEmail("b@x");
    assertThat(count("SELECT FROM Account WHERE region = 'us' AND id = 1")).isEqualTo(1);

    database.transaction(() -> database.command("sql", "UPDATE Account CONTENT {\"id\": 1, \"email\": \"c@x\"} WHERE id = 1"));
    assertEmail("c@x");

    database.transaction(() -> database.command("sql", "UPDATE Account MERGE {\"email\": \"d@x\"} WHERE id = 1"));
    assertEmail("d@x");

    database.transaction(() -> database.command("sql", "UPDATE Account REMOVE email WHERE id = 1"));
    assertThat(count("SELECT FROM Account WHERE email = 'd@x'")).isZero();

    database.transaction(() -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a.email = 'e@x'"));
    assertEmail("e@x");

    database.transaction(() -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a += {email: 'f@x', id: 1}"));
    assertEmail("f@x");
  }

  @Test
  void sameValueWriteOfThePartitionKeyIsAccepted() {
    database.transaction(() -> database.command("sql", "UPDATE Account SET id = 1, email = 'z@x' WHERE id = 1"));
    assertEmail("z@x");
  }

  @Test
  void partitionKeyChangeIsRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Account SET id = 2 WHERE id = 1")))
        .isInstanceOf(ValidationException.class).hasMessageContaining("partition key [id]");
    // openCypher wraps the refusal in a CommandExecutionException; the server classifies it along the cause chain
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (a:Account {id: 1}) SET a.id = 3, a.email = 'q@x'")))
        .hasRootCauseInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE Account REMOVE id WHERE id = 1")))
        .isInstanceOf(ValidationException.class);

    assertThat(count("SELECT FROM Account WHERE id = 1 AND email = 'a@x'")).isEqualTo(1);
    assertThat(count("SELECT FROM Account WHERE id <> 1")).isZero();
  }

  private void assertEmail(final String email) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Account WHERE email = ?", email)) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((Integer) r.getProperty("id")).isEqualTo(1);
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.stream().count();
    }
  }
}
