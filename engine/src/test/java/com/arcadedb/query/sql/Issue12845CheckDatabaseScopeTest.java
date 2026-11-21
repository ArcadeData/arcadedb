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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12845: {@code CHECK DATABASE BUCKET <id>} and {@code BUCKET :param} handed the checker a parser node that never equalled a
 * bucket name, so the bucket was skipped, and an unknown TYPE or BUCKET name reported a clean empty database.
 */
class Issue12845CheckDatabaseScopeTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO V SET a = 1").close();
      database.command("sql", "INSERT INTO V SET a = 2").close();
    });
  }

  @Test
  void aBucketIsCheckedWhetherNamedByNameIdOrParameter() {
    final int bucketId = database.getSchema().getType("V").getBuckets(false).getFirst().getFileId();
    final String bucketName = database.getSchema().getBucketById(bucketId).getName();

    assertThat(records("CHECK DATABASE BUCKET " + bucketName, Map.of())).isEqualTo(2);
    assertThat(records("CHECK DATABASE BUCKET " + bucketId, Map.of())).isEqualTo(2);
    assertThat(records("CHECK DATABASE TYPE V", Map.of())).isEqualTo(2);
    assertThat(records("CHECK DATABASE", Map.of())).isEqualTo(2);
  }

  @Test
  void aScopeThatNamesNothingIsRefused() {
    assertThatThrownBy(() -> records("CHECK DATABASE TYPE Nope", Map.of())).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE TYPE v", Map.of())).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE BUCKET Nope_0", Map.of())).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> records("CHECK DATABASE BUCKET 9999", Map.of())).isInstanceOf(SchemaException.class);
  }

  private long records(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.command("sql", sql, params)) {
      return rs.next().<Number>getProperty("totalActiveRecords").longValue();
    }
  }
}
