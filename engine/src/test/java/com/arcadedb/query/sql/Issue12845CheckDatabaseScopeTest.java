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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.BucketIdentifier;
import com.arcadedb.query.sql.parser.CheckDatabaseStatement;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.StatementCache;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12845: {@code CHECK DATABASE BUCKET <id>} handed the checker a parser node that never equalled a bucket name, so the
 * bucket was skipped, and an unknown TYPE or BUCKET name reported a clean empty database. With both TYPE and BUCKET, every
 * identifier was taken as a type name, so the BUCKET clause was lost.
 */
class Issue12845CheckDatabaseScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE VERTEX TYPE W");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V SET a = 1").close();
      database.command("sql", "INSERT INTO V SET a = 2").close();
      database.command("sql", "INSERT INTO W SET a = 3").close();
    });
  }

  @Test
  void aBucketIsCheckedWhetherNamedByNameOrId() {
    final int bucketId = database.getSchema().getType("V").getBuckets(false).getFirst().getFileId();
    final String bucketName = database.getSchema().getBucketById(bucketId).getName();

    assertThat(records("CHECK DATABASE BUCKET " + bucketName)).isEqualTo(2);
    assertThat(records("CHECK DATABASE BUCKET " + bucketId)).isEqualTo(2);
    assertThat(records("CHECK DATABASE TYPE V")).isEqualTo(2);
    assertThat(records("CHECK DATABASE")).isEqualTo(3);
  }

  @Test
  void typeAndBucketTogetherCheckTheirIntersection() {
    final int vBucketId = database.getSchema().getType("V").getBuckets(false).getFirst().getFileId();
    final String vBucketName = database.getSchema().getBucketById(vBucketId).getName();
    final String wBucketName = database.getSchema().getType("W").getBuckets(false).getFirst().getName();

    assertThat(records("CHECK DATABASE TYPE V BUCKET " + vBucketName)).isEqualTo(2);
    assertThat(records("CHECK DATABASE TYPE V BUCKET " + vBucketId)).isEqualTo(2);
    assertThat(records("CHECK DATABASE TYPE V, W BUCKET " + wBucketName)).isEqualTo(1);
    assertThat(records("CHECK DATABASE TYPE V BUCKET " + wBucketName)).isEqualTo(0);
  }

  @Test
  void identifiersAfterBucketAreBucketNames() {
    final CheckDatabaseStatement stmt = (CheckDatabaseStatement) new StatementCache(database, 10).get(
        "CHECK DATABASE TYPE V, W BUCKET V_0, 7, W_0");

    assertThat(stmt.types.stream().map(Identifier::getStringValue)).containsExactlyInAnyOrder("V", "W");
    assertThat(stmt.buckets.stream().filter(b -> b.bucketName != null).map(BucketIdentifier::getValue))
        .containsExactlyInAnyOrder("V_0", "W_0");
    assertThat(stmt.buckets.stream().filter(b -> b.bucketId != null).map(b -> b.bucketId.getValue().intValue()))
        .containsExactly(7);
  }

  @Test
  void aScopeThatNamesNothingIsRefused() {
    assertThatThrownBy(() -> records("CHECK DATABASE TYPE Nope")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE TYPE v")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE BUCKET Nope_0")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE BUCKET 9999")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE TYPE V BUCKET Nope_0")).isInstanceOf(SchemaException.class);
  }

  private long records(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      return rs.next().<Number>getProperty("totalActiveRecords").longValue();
    }
  }
}
