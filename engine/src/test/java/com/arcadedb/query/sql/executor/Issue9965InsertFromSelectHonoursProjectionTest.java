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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.graph.Vertex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9965: {@code INSERT INTO <t> FROM SELECT <projection>} copied the RECORD behind each selected row
 * instead of the row, whenever the row had one - which is every {@code SELECT *}-shaped projection, the only ones that can
 * exclude or override a column. {@code SELECT *, !secret} wrote {@code secret} into the target anyway, and an overriding alias
 * was written with the stored value. The kind of the new record still comes from the source record; its properties come from
 * the row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9965InsertFromSelectHonoursProjectionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE VERTEX TYPE SV");
      database.command("sql", "CREATE VERTEX TYPE TV");
      database.command("sql", "INSERT INTO S SET pub = 'p', secret = 'SSS', n = 1");
      database.command("sql", "INSERT INTO SV SET pub = 'p', secret = 'SSS'");
    });
  }

  @Test
  void anExcludedColumnIsNotCopiedIntoADocument() {
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT *, !secret FROM S"));
    final Document copy = single("SELECT FROM T");
    assertThat(copy.getPropertyNames()).containsExactlyInAnyOrder("pub", "n");
    assertThat(copy.getString("pub")).isEqualTo("p");
  }

  @Test
  void anOverridingAliasIsCopiedWithTheRowValue() {
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT *, 'OVERRIDE' AS pub FROM S"));
    final Document copy = single("SELECT FROM T");
    assertThat(copy.getString("pub")).isEqualTo("OVERRIDE");
    assertThat(copy.getString("secret")).isEqualTo("SSS");
  }

  @Test
  void aComputedAliasIsCopiedToo() {
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT *, n + 1 AS z FROM S"));
    final Document copy = single("SELECT FROM T");
    assertThat(copy.getInteger("z")).isEqualTo(2);
    assertThat(copy.getPropertyNames()).containsExactlyInAnyOrder("pub", "secret", "n", "z");
  }

  @Test
  void anExcludedColumnIsNotCopiedIntoAVertex() {
    database.transaction(() -> database.command("sql", "INSERT INTO TV FROM SELECT *, !secret FROM SV"));
    final Document copy = single("SELECT FROM TV");
    assertThat(copy).isInstanceOf(Vertex.class);
    assertThat(copy.getPropertyNames()).containsExactly("pub");
  }

  @Test
  void aWholeRecordIsStillCopiedWholeAndKeepsItsKind() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T FROM SELECT FROM S");
      database.command("sql", "INSERT INTO TV FROM SELECT FROM SV");
    });
    assertThat(single("SELECT FROM T").toMap(false)).containsOnly(Map.entry("pub", "p"), Map.entry("secret", "SSS"),
        Map.entry("n", 1));
    final Document vertex = single("SELECT FROM TV");
    assertThat(vertex).isInstanceOf(Vertex.class);
    assertThat(vertex.toMap(false)).containsOnly(Map.entry("pub", "p"), Map.entry("secret", "SSS"));
  }

  @Test
  void embeddedValuesAreCopiedAsValues() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Emb");
      database.command("sql", "UPDATE S SET e = {\"@type\":\"Emb\",\"x\":1}, le = [{\"@type\":\"Emb\",\"x\":2}, {\"@type\":\"Emb\",\"x\":3}]");
    });
    final Document source = single("SELECT FROM S");
    assertThat(source.get("e")).isInstanceOf(EmbeddedDocument.class);
    final JSONObject sourceJson = source.toJSON(false);

    for (final String projection : List.of("", "*", "*, !secret")) {
      database.transaction(() -> {
        database.command("sql", "DELETE FROM T");
        database.command("sql", "INSERT INTO T FROM SELECT " + projection + " FROM S");
      });
      final Document copy = single("SELECT FROM T");
      // a value of the copy, not the source's own instance: the same shape and content, owned by the new record
      assertThat(copy.get("e")).as(projection).isInstanceOf(EmbeddedDocument.class).isNotSameAs(source.get("e"));
      final JSONObject copyJson = copy.toJSON(false);
      assertThat(copyJson.getJSONObject("e").toMap()).as(projection).isEqualTo(sourceJson.getJSONObject("e").toMap());
      assertThat(copyJson.getJSONArray("le").toList()).as(projection).isEqualTo(sourceJson.getJSONArray("le").toList());
    }
  }

  @Test
  void aQueryVariableIsNotStoredAsAProperty() {
    try (final ResultSet rs = database.query("sql", "SELECT *, $a FROM S LET $a = 5")) {
      assertThat(rs.next().getPropertyNames()).contains("$a");
    }
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT *, $a, $a AS a FROM S LET $a = 5"));
    final Document copy = single("SELECT FROM T");
    assertThat(copy.getPropertyNames()).containsExactlyInAnyOrder("pub", "secret", "n", "a");
    assertThat(copy.getInteger("a")).isEqualTo(5);
  }

  @Test
  void aStoredDollarPropertyIsCopiedLikeAnyOther() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM S");
      database.newDocument("S").set("$foo", 7).set("pub", "p").save();
    });
    for (final String projection : List.of("", "*")) {
      database.transaction(() -> {
        database.command("sql", "DELETE FROM T");
        database.command("sql", "INSERT INTO T FROM SELECT " + projection + " FROM S");
      });
      assertThat(single("SELECT FROM T").toMap(false)).as(projection).containsOnly(Map.entry("$foo", 7), Map.entry("pub", "p"));
    }
  }

  @Test
  void aContentOnlyRowIsCopiedAsBefore() {
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT pub FROM S"));
    assertThat(single("SELECT FROM T").toMap(false)).containsOnly(Map.entry("pub", "p"));
  }

  /**
   * The guarantee over the statement rather than the column kind: the record written has exactly the source row's properties,
   * metadata aside, each with the value the row answers for it.
   */
  @Test
  void theWrittenRecordIsTheSelectedRow() {
    for (final String projection : List.of("*", "*, !secret", "*, !n, 'x' AS pub", "*, !@rid", "*, n * 10 AS n", "pub, n")) {
      final Map<String, Object> expected = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT " + projection + " FROM S")) {
        final Result row = rs.next();
        for (final String name : row.getPropertyNames())
          if (!name.startsWith("@"))
            expected.put(name, row.getProperty(name));
      }

      database.transaction(() -> {
        database.command("sql", "DELETE FROM T");
        database.command("sql", "INSERT INTO T FROM SELECT " + projection + " FROM S");
      });
      assertThat(single("SELECT FROM T").toMap(false)).as(projection).containsExactlyInAnyOrderEntriesOf(expected);
    }
  }

  private Document single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      final Document document = rs.next().toElement();
      assertThat(rs.hasNext()).as(sql).isFalse();
      return document;
    }
  }
}
