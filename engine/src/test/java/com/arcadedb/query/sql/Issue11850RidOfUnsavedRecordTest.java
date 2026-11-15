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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11850: {@code @rid} read by an INSERT's SET, before the record is saved, threw a bare NullPointerException
 * ({@code ResultInternal.getIdentity()} wrapped the missing identity in {@code Optional.of}), so the statement answered HTTP 500.
 * A record without an identity yet has no {@code @rid}: it reads as null, as {@code @rid} of a row with no record does.
 */
class Issue11850RidOfUnsavedRecordTest extends TestHelper {

  @Test
  void ridReadByTheSetOfAnInsertIsNull() {
    database.getSchema().createDocumentType("D");
    database.getSchema().createVertexType("V");

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO D SET name = 'a', m = @rid")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("a");
        assertThat(row.<Object>getProperty("m")).isNull();
        assertThat(row.getIdentity()).isPresent();
      }
      try (final ResultSet rs = database.command("sql", "INSERT INTO V SET z = @rid")) {
        assertThat(rs.next().<Object>getProperty("z")).isNull();
      }
    });

    assertThat(database.countType("D", false)).isEqualTo(1);
    assertThat(database.countType("V", false)).isEqualTo(1);
  }
}
