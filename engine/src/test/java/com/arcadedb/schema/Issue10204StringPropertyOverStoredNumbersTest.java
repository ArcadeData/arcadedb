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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10204: {@code CREATE PROPERTY T.n STRING} was accepted over records holding {@code n} as integers, without converting
 * them, so an index created on {@code n} afterwards compared as text what a scan compared as numbers: the range
 * {@code n >= 20 AND n < 100} answered 0 rows through the index and 80 by a scan. Every other declared type is held to the
 * stored values ({@code CREATE PROPERTY U.s INTEGER} over {@code 'abc'} is refused): STRING now is too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10204StringPropertyOverStoredNumbersTest extends TestHelper {

  @Test
  void stringOverStoredIntegersIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      for (int i = 1; i <= 200; i++)
        database.newDocument("T").set("id", i, "n", i).save();
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.n STRING"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Cannot create property 'T.n'")
        .hasMessageContaining("STRING")
        .hasMessageContaining("Integer");
    assertThat(database.getSchema().getType("T").existsProperty("n")).isFalse();

    // CONVERTED FIRST, THE DECLARATION IS ACCEPTED AND THE INDEX AGREES WITH A SCAN
    database.transaction(() -> database.command("sql", "UPDATE T SET n = n.asString()"));
    database.command("sql", "CREATE PROPERTY T.n STRING");
    database.command("sql", "CREATE INDEX ON T (n) NOTUNIQUE");
    assertThat(count("SELECT count(*) AS c FROM T WHERE n = '150'")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM T WHERE n >= '20' AND n < '30'"))
        .isEqualTo(count("SELECT count(*) AS c FROM T WHERE n.asString() >= '20' AND n.asString() < '30'"));
  }

  @Test
  void stringOverOtherStoredScalarsIsRefused() {
    final Object[] values = { 3L, (short) 7, 1.5f, 2.5, true, LocalDate.of(2026, 1, 2) };
    for (int i = 0; i < values.length; i++) {
      final String typeName = "S" + i;
      final Object value = values[i];
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
      database.transaction(() -> database.newDocument(typeName).set("n", value).save());

      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + typeName + ".n STRING"))
          .as(value.getClass().getSimpleName())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("STRING");
      assertThat(database.getSchema().getType(typeName).existsProperty("n")).isFalse();
    }
  }

  @Test
  void stringOverStoredStringsAndNullsIsAccepted() {
    database.command("sql", "CREATE DOCUMENT TYPE W");
    database.transaction(() -> {
      database.newDocument("W").set("n", "abc").save();
      database.newDocument("W").set("n", (Object) null).save();
      database.newDocument("W").set("other", 5).save();
    });

    database.command("sql", "CREATE PROPERTY W.n STRING");
    assertThat(database.getSchema().getType("W").getProperty("n").getType()).isEqualTo(Type.STRING);
  }

  @Test
  void aSubtypeRecordIsCheckedToo() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.transaction(() -> {
      database.newDocument("P").set("n", "a").save();
      database.newDocument("C").set("n", 42).save();
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY P.n STRING")).isInstanceOf(CommandExecutionException.class);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
