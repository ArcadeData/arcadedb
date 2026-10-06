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
 * Regression test for issue #9339: an UPSERT whose WHERE condition is not an equality on every property of a UNIQUE index
 * (non-unique, missing or partially matched composite index, range, IN, OR, duplicated predicate) must say so in the error, instead of the generic "must involve an index" wording that misled
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

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ?", "x", "t")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
    assertThat(database.countType("Comp", false)).isZero();

    database.transaction(() -> {
      database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "x", "t", "c1");
      database.command("sql", "UPDATE Comp SET name = ? UPSERT WHERE tenant = ? AND code = ?", "y", "t", "c1");
    });
    assertThat(database.countType("Comp", false)).isEqualTo(1);
  }

  @Test
  void upsertWithResidualPredicateStillUsesTheUniqueIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE Resid");
    database.command("sql", "CREATE PROPERTY Resid.code STRING");
    database.command("sql", "CREATE INDEX ON Resid (code) UNIQUE");

    database.transaction(() -> {
      database.command("sql", "UPDATE Resid SET name = ? UPSERT WHERE code = ? AND name = ?", "n1", "c1", "n1");
      database.command("sql", "UPDATE Resid SET name = ? UPSERT WHERE code = ? AND name = ?", "n1", "c1", "n1");
    });
    assertThat(database.countType("Resid", false)).isEqualTo(1);
  }

  @Test
  void upsertWithRangeOrInConditionIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Rng");
    database.command("sql", "CREATE PROPERTY Rng.code STRING");
    database.command("sql", "CREATE INDEX ON Rng (code) UNIQUE");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Rng SET name = ? UPSERT WHERE code > ?", "x", "a")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Rng SET name = ? UPSERT WHERE code IN ['a','b']", "x")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
    assertThat(database.countType("Rng", false)).isZero();
  }

  @Test
  void upsertWithDuplicatedPredicateOnCompositeIndexIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Dup");
    database.command("sql", "CREATE PROPERTY Dup.tenant STRING");
    database.command("sql", "CREATE PROPERTY Dup.code STRING");
    database.command("sql", "CREATE INDEX ON Dup (tenant, code) UNIQUE");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE Dup SET name = ? UPSERT WHERE tenant = ? AND tenant = ?", "x", "t", "t")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
    assertThat(database.countType("Dup", false)).isZero();
  }

  @Test
  void upsertWithOrConditionIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE OrCond");
    database.command("sql", "CREATE PROPERTY OrCond.code STRING");
    database.command("sql", "CREATE INDEX ON OrCond (code) UNIQUE");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "UPDATE OrCond SET name = ? UPSERT WHERE code = ? OR code = ?", "x", "a", "b")))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("UNIQUE index");
    assertThat(database.countType("OrCond", false)).isZero();
  }

  @Test
  void upsertOnSubqueryTargetWithUniqueFullKeyLookup() {
    database.command("sql", "CREATE DOCUMENT TYPE Sub");
    database.command("sql", "CREATE PROPERTY Sub.code STRING");
    database.command("sql", "CREATE INDEX ON Sub (code) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO Sub SET code = 'c1', name = 'old'"));

    database.transaction(() -> database.command("sql", "UPDATE Sub SET name = 'new' UPSERT WHERE code = 'c1'"));
    try (final ResultSet rs = database.query("sql", "SELECT name FROM Sub")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("new");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void upsertWithNonPlainKeyShapesIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Shapes");
    database.command("sql", "CREATE PROPERTY Shapes.code STRING");
    database.command("sql", "CREATE INDEX ON Shapes (code) UNIQUE");

    // reversed equality, null key, and a modified/wrapped left side are not a plain full-key lookup
    final String[] where = { "? = code", "code = ?", "code.toLowerCase() = ?", "code.length() = ?" };
    final Object[] value = { "c1", null, "c1", 2 };
    for (int k = 0; k < where.length; k++) {
      final String condition = where[k];
      final Object parameter = value[k];
      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "UPDATE Shapes SET name = ? UPSERT WHERE " + condition, "x", parameter)))
          .as(condition)
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("UNIQUE index");
    }
    assertThat(database.countType("Shapes", false)).isZero();
  }
}
