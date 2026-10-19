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
import com.arcadedb.database.Document;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10194, second point: an explicit NULL for a property that has a default stored the default. In SQL the default fills a column
 * that is OMITTED from the statement, and an explicit NULL is stored as NULL. {@code UPDATE ... APPLY DEFAULTS}, whose job is to
 * fill what is null, keeps doing so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10194ExplicitNullKeepsNullTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.command("sql", "CREATE PROPERTY T.id LONG").close();
    database.command("sql", "CREATE PROPERTY T.df INTEGER (default 7)").close();
  }

  private Document only() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
      final Document doc = rs.next().getElement().orElseThrow();
      assertThat(rs.hasNext()).isFalse();
      return doc;
    }
  }

  private void run(final String statement) {
    database.transaction(() -> database.command("sql", statement).close());
  }

  @Test
  void insertSetWithExplicitNullStoresNull() {
    run("INSERT INTO T SET id = 1, df = null");

    final Document doc = only();
    assertThat(doc.has("df")).isTrue();
    assertThat(doc.get("df")).isNull();
  }

  @Test
  void insertSetWithTheColumnOmittedStoresTheDefault() {
    run("INSERT INTO T SET id = 1");

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void insertValuesWithExplicitNullStoresNull() {
    run("INSERT INTO T (id, df) VALUES (1, null)");

    final Document doc = only();
    assertThat(doc.has("df")).isTrue();
    assertThat(doc.get("df")).isNull();
  }

  @Test
  void insertValuesWithTheColumnOmittedStoresTheDefault() {
    run("INSERT INTO T (id) VALUES (1)");

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void insertContentWithExplicitNullStoresNull() {
    run("INSERT INTO T CONTENT {\"id\": 1, \"df\": null}");

    final Document doc = only();
    assertThat(doc.has("df")).isTrue();
    assertThat(doc.get("df")).isNull();
  }

  @Test
  void insertContentWithTheKeyOmittedStoresTheDefault() {
    run("INSERT INTO T CONTENT {\"id\": 1}");

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void insertFromSelectOfANullStoresNull() {
    database.command("sql", "CREATE DOCUMENT TYPE Src").close();
    database.transaction(() -> database.newDocument("Src").set("id", 1L).set("df", null).save());

    run("INSERT INTO T FROM SELECT id, df FROM Src");

    final Document doc = only();
    assertThat(doc.has("df")).isTrue();
    assertThat(doc.get("df")).isNull();
  }

  @Test
  void javaApiExplicitNullStoresNull() {
    database.transaction(() -> database.newDocument("T").set("id", 1L).set("df", null).save());

    final Document doc = only();
    assertThat(doc.has("df")).isTrue();
    assertThat(doc.get("df")).isNull();
  }

  @Test
  void javaApiOmittedPropertyStoresTheDefault() {
    database.transaction(() -> database.newDocument("T").set("id", 1L).save());

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void anExplicitNullSurvivesTheReload() {
    run("INSERT INTO T SET id = 1, df = null");

    final Document reloaded = database.lookupByRID(only().getIdentity(), true).asDocument();
    assertThat(reloaded.has("df")).isTrue();
    assertThat(reloaded.get("df")).isNull();
  }

  @Test
  void updateSetToNullStaysNull() {
    run("INSERT INTO T SET id = 1");
    run("UPDATE T SET df = null");

    assertThat(only().get("df")).isNull();
  }

  @Test
  void applyDefaultsStillFillsAnExplicitNull() {
    run("INSERT INTO T SET id = 1, df = null");

    run("UPDATE T SET id = 2 APPLY DEFAULTS");

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void notNullPropertyWithADefaultRefusesAnExplicitNull() {
    database.command("sql", "ALTER PROPERTY T.df NOTNULL true").close();

    assertThatThrownBy(() -> run("INSERT INTO T SET id = 1, df = null")).isInstanceOf(ValidationException.class);
    run("INSERT INTO T SET id = 2");

    assertThat(only().getInteger("df")).isEqualTo(7);
  }

  @Test
  void aRefusedExplicitNullInsideACallerKeptTransactionLeavesItUsable() {
    database.command("sql", "ALTER PROPERTY T.df NOTNULL true").close();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 1").close();
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET id = 2, df = null").close())
          .isInstanceOf(ValidationException.class);
      database.command("sql", "INSERT INTO T SET id = 3").close();
    });

    try (final ResultSet rs = database.query("sql", "SELECT id, df FROM T ORDER BY id")) {
      assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(1L);
      assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(3L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void edgeCreatedWithAnExplicitNullKeepsNull() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.command("sql", "CREATE PROPERTY E.w INTEGER (default 5)").close();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V").close();
      database.command("sql", "CREATE VERTEX V").close();
    });

    run("CREATE EDGE E FROM (SELECT FROM V LIMIT 1) TO (SELECT FROM V SKIP 1 LIMIT 1) SET w = null");
    run("CREATE EDGE E FROM (SELECT FROM V SKIP 1 LIMIT 1) TO (SELECT FROM V LIMIT 1)");

    int nulls = 0;
    int defaults = 0;
    try (final ResultSet rs = database.query("sql", "SELECT FROM E")) {
      while (rs.hasNext()) {
        final Document edge = rs.next().getElement().orElseThrow();
        if (edge.get("w") == null)
          ++nulls;
        else if (edge.getInteger("w") == 5)
          ++defaults;
      }
    }
    assertThat(nulls).isEqualTo(1);
    assertThat(defaults).isEqualTo(1);
  }
}
