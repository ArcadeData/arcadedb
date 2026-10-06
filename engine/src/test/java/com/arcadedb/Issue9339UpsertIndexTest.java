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
package com.arcadedb;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9339: an UPSERT whose WHERE condition cannot use a unique single-property index (non-unique,
 * composite or missing index) must say so in the error, instead of the generic "must involve an index" wording that misled
 * users who did have an index on the property. A composite UNIQUE index is usable only when the WHERE matches all its properties.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9339UpsertIndexTest extends TestHelper {

  @Test
  void upsertOnUniqueIndexWorksWithNamedAndPositionalParameters() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.code STRING");
    database.command("sql", "CREATE INDEX ON Doc (code) UNIQUE");

    database.transaction(() -> {
      database.command("sql", "UPDATE Doc SET name = :a, code = :b UPSERT WHERE code = :b", Map.of("a", "x", "b", "c1"));
      database.command("sql", "UPDATE Doc SET name = ?, code = ? UPSERT WHERE code = ?", "y", "c1", "c1");
    });
    assertThat(database.countType("Doc", false)).isEqualTo(1);
    try (final ResultSet rs = database.query("sql", "SELECT code, name FROM Doc")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("code")).isEqualTo("c1");
      assertThat(row.<String>getProperty("name")).isEqualTo("y");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void upsertWithoutUniqueIndexExplainsTheRequirement() {
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.code STRING");
    database.command("sql", "CREATE INDEX ON Plain (code) NOTUNIQUE");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Plain SET code = ? UPSERT WHERE code = ?", "c1", "c1")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
  }

  @Test
  void upsertWithoutAnyIndexExplainsTheRequirement() {
    database.command("sql", "CREATE DOCUMENT TYPE NoIdx");
    database.command("sql", "CREATE PROPERTY NoIdx.code STRING");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE NoIdx SET code = ? UPSERT WHERE code = ?", "c1", "c1")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
  }

  @Test
  void upsertOnCompositeUniqueIndexNeedsAllItsProperties() {
    database.command("sql", "CREATE DOCUMENT TYPE Comp");
    database.command("sql", "CREATE PROPERTY Comp.tenant STRING");
    database.command("sql", "CREATE PROPERTY Comp.code STRING");
    database.command("sql", "CREATE INDEX ON Comp (tenant, code) UNIQUE");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE code = ?", "x", "c1")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");

    database.transaction(() -> {
      database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "x", "t", "c1");
      database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "y", "t", "c1");
    });
    assertThat(database.countType("Comp", false)).isEqualTo(1);
  }
}
