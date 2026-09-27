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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A FULL_TEXT index answers a lookup by analyzer tokens OR-ed together, which is the right answer for
 * {@code CONTAINSTEXT} and {@code SEARCH_INDEX()} but not for {@code =}: the planner used to hand an equality (and
 * every other predicate it considers index-aware - {@code IN}, {@code CONTAINS}...) to a FULL_TEXT index and drop it
 * from the residual filter, so {@code WHERE name = 'x'} returned every row sharing a single token with 'x' (issue #8435).
 */
class Issue8435FullTextIndexEqualityTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Entity");
      database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
      database.command("sql", "CREATE PROPERTY Entity.name STRING");
      database.command("sql", "CREATE PROPERTY Entity.entity_type STRING");
      database.command("sql", "CREATE INDEX ON Entity (uuid) UNIQUE");
      database.command("sql", "CREATE INDEX ON Entity (entity_type) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Entity (name) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Entity SET uuid = 'act',  name = 'Актёр маркер-записи', entity_type = 'PERSON'");
      database.command("sql", "INSERT INTO Entity SET uuid = 'tgt',  name = 'Цель маркер-записи',  entity_type = 'ORG'");
      database.command("sql", "INSERT INTO Entity SET uuid = 'act2', name = 'Другой актёр',        entity_type = 'PERSON'");
      database.command("sql", "UPDATE Entity SET name = 'Актёр маркер-записи II' WHERE uuid = 'act'");
    });
  }

  @Test
  void equalityIsExactNotTokenMatching() {
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Цель маркер-записи'")).containsExactly("tgt");
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'актёр'")).isEmpty();
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Актёр маркер-записи'")).isEmpty();
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Актёр маркер-записи II'")).containsExactly("act");
  }

  @Test
  void equalityWithParameterIsExact() {
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = ?", "Цель маркер-записи")).containsExactly("tgt");
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = :n", Map.of("n", "Другой актёр"))).containsExactly("act2");
  }

  @Test
  void equalityAndedWithAnotherIndexedPropertyIsExact() {
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Другой актёр' AND entity_type = 'PERSON'")).containsExactly("act2");
    assertThat(uuids("SELECT uuid FROM Entity WHERE entity_type = 'PERSON' AND name = 'Цель маркер-записи'")).isEmpty();
  }

  @Test
  void orOfEqualitiesIsExact() {
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Цель маркер-записи' OR name = 'актёр'")).containsExactly("tgt");
  }

  @Test
  void sqlMatchEqualityIsExact() {
    assertThat(uuids("MATCH {type: Entity, as: e, where: (name = 'Цель маркер-записи')} RETURN e.uuid AS uuid"))
        .containsExactly("tgt");
  }

  @Test
  void inIsExact() {
    assertThat(uuids("SELECT uuid FROM Entity WHERE name IN ['Цель маркер-записи', 'актёр']")).containsExactly("tgt");
  }

  @Test
  void updateAndDeleteByEqualityTouchOnlyTheExactRow() {
    database.transaction(() -> {
      final ResultSet updated = database.command("sql",
          "UPDATE Entity SET entity_type = 'RENAMED' WHERE name = 'Цель маркер-записи'");
      assertThat(updated.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
    });
    assertThat(uuids("SELECT uuid FROM Entity WHERE entity_type = 'RENAMED'")).containsExactly("tgt");

    database.transaction(() -> {
      final ResultSet deleted = database.command("sql", "DELETE FROM Entity WHERE name = 'Другой актёр'");
      assertThat(deleted.next().<Number>getProperty("count").longValue()).isEqualTo(1L);
    });
    assertThat(uuids("SELECT uuid FROM Entity")).containsExactlyInAnyOrder("act", "tgt");
  }

  @Test
  void containsTextStillUsesTheFullTextIndex() {
    assertThat(explain("SELECT uuid FROM Entity WHERE name CONTAINSTEXT 'маркер'")).contains("FETCH FROM INDEX Entity[name]");
    assertThat(uuids("SELECT uuid FROM Entity WHERE name CONTAINSTEXT 'маркер'")).containsExactlyInAnyOrder("act", "tgt");
    assertThat(uuids("SELECT uuid FROM Entity WHERE search_index('Entity[name]', 'маркер') = true"))
        .containsExactlyInAnyOrder("act", "tgt");
  }

  @Test
  void equalityDoesNotFetchFromTheFullTextIndex() {
    assertThat(explain("SELECT uuid FROM Entity WHERE name = 'Цель маркер-записи'")).doesNotContain("FETCH FROM INDEX Entity[name]");
  }

  @Test
  void cypherPropertyEqualityIsExact() {
    assertThat(cypherUuids("MATCH (n:Entity) WHERE n.name = 'Цель маркер-записи' RETURN n.uuid AS uuid")).containsExactly("tgt");
    assertThat(cypherUuids("MATCH (n:Entity {name: 'Цель маркер-записи'}) RETURN n.uuid AS uuid")).containsExactly("tgt");
    assertThat(cypherUuids("MATCH (n:Entity {name: 'актёр'}) RETURN n.uuid AS uuid")).isEmpty();
  }

  @Test
  void cypherMergeOnFullTextPropertyCreatesWhenNoExactMatch() {
    database.transaction(() -> database.command("opencypher", "MERGE (n:Entity {name: 'Цель'}) ON CREATE SET n.uuid = 'new'"));
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Цель'")).containsExactly("new");
    assertThat(uuids("SELECT uuid FROM Entity WHERE name = 'Цель маркер-записи'")).containsExactly("tgt");
  }

  @Test
  void javaSelectApiEqualityIsExact() {
    final List<String> found = new ArrayList<>();
    final Iterator<? extends Document> it = database.select().fromType("Entity").where().property("name").eq()
        .value("Цель маркер-записи").documents();
    while (it.hasNext())
      found.add(it.next().getString("uuid"));
    assertThat(found).containsExactly("tgt");
  }

  private List<String> uuids(final String sql, final Object... args) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("uuid"));
    }
    return result;
  }

  private List<String> uuids(final String sql, final Map<String, Object> args) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("uuid"));
    }
    return result;
  }

  private List<String> cypherUuids(final String cypher) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.add(r.getProperty("uuid"));
      }
    }
    return result;
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
