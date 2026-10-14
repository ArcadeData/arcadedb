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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9973: a BSON field name is DERIVED from a map key, it is not the key. A {@code MAP} property keeps its keys with their own
 * class (an Integer, a Long, a Date, a {@code null}) after a reopen, and the MongoDB result conversion cast every key to String: a
 * find over such a record failed the whole query with a {@code ClassCastException}, and a {@code null} key produced a null field
 * name that failed later, when the response was encoded. The key's text is the field name, as the shared {@code JSONObject(Map)}
 * constructor takes it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9973MongoMapKeyTest {
  private static final String DB_PATH = "./target/databases/issue9973";
  private static final Date   DATE    = new Date(1_700_000_000_000L);

  private Database database;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Coll").createProperty("m", Type.MAP);
    database.transaction(() -> {
      final Map<Object, Object> map = new LinkedHashMap<>();
      map.put(1, "one");
      map.put(2L, "two");
      map.put(null, "nothing");
      map.put(DATE, "date");
      map.put("text", Map.of(3, "nested"));
      database.newDocument("Coll").set("_id", "a").set("m", map).save();
    });
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      while (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void aMapKeyedByAnythingButAStringConvertsToFieldsNamedByTheKeyText() {
    assertFieldsNamedByKeyText();
  }

  @Test
  void aMapKeyedByAnythingButAStringConvertsToFieldsNamedByTheKeyTextAfterAReopen() {
    database.close();
    database = new DatabaseFactory(DB_PATH).open();

    // the reopen is what hands the keys back with their own class: the claim is pinned on that, not on the in-memory map
    try (final ResultSet rs = database.query("sql", "SELECT m FROM Coll")) {
      final Map<?, ?> stored = rs.next().getProperty("m");
      assertThat(stored.keySet()).anyMatch(Integer.class::isInstance).anyMatch(Long.class::isInstance);
    }
    assertFieldsNamedByKeyText();
  }

  @Test
  void anArbitraryObjectKeyIsNamedByItsText() {
    final Object key = new Object() {
      @Override
      public String toString() {
        return "custom";
      }
    };
    final Map<Object, Object> map = new HashMap<>();
    map.put(key, 7);
    final ResultInternal row = new ResultInternal(Map.of("_id", "b", "m", map));

    final Document converted = MongoDBToSqlTranslator.convertDocumentToMongoDB(row);
    assertThat(((Document) converted.get("m")).get("custom")).isEqualTo(7);
  }

  @Test
  void theMongoQueryLanguageReturnsTheRecord() {
    try (final ResultSet rs = database.query("mongo", "{ collection: 'Coll', query: { _id: 'a' } }")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("m")).isNotNull();
    }
  }

  @Test
  void aDottedSetUpdatesTheStoredKeyItNamesAndKeepsItsClass() {
    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(database, null, null);
    final Document reply = wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "a")).append("u", new Document("$set", new Document("m.1", "uno"))))), null, null);
    assertThat(reply.get("n")).isEqualTo(1);

    try (final ResultSet rs = database.query("sql", "SELECT m FROM Coll")) {
      final Map<?, ?> stored = rs.next().getProperty("m");
      assertThat(stored.get(1)).isEqualTo("uno");
      assertThat(stored.containsKey("1")).as("the key named by the path is updated, not shadowed by a second one").isFalse();
    }
  }

  /** When an Integer key and a String key have the same text, the path names the String one, whatever the map's order. */
  @Test
  void aDottedSetNamesTheStringKeyWhenTwoKeysShareItsText() {
    database.transaction(() -> {
      final Map<Object, Object> map = new LinkedHashMap<>();
      map.put(1, "integer");
      map.put("1", "string");
      database.newDocument("Coll").set("_id", "b").set("m", map).save();
    });

    final MongoDBDatabaseWrapper wrapper = new MongoDBDatabaseWrapper(database, null, null);
    wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", new Document("_id", "b")).append("u", new Document("$set", new Document("m.1", "changed"))))), null, null);

    try (final ResultSet rs = database.query("sql", "SELECT m FROM Coll WHERE _id = 'b'")) {
      final Map<?, ?> stored = rs.next().getProperty("m");
      assertThat(stored.get("1")).isEqualTo("changed");
      assertThat(stored.get(1)).isEqualTo("integer");
    }
  }

  private void assertFieldsNamedByKeyText() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Coll")) {
      final Document converted = MongoDBToSqlTranslator.convertDocumentToMongoDB(rs.next().toElement());
      final Document m = (Document) converted.get("m");
      assertThat(m.get("1")).isEqualTo("one");
      assertThat(m.get("2")).isEqualTo("two");
      assertThat(m.get("null")).isEqualTo("nothing");
      assertThat(m.containsKey(null)).isFalse();
      assertThat(m.keySet()).allMatch(String.class::isInstance);
      assertThat(m.values()).contains("date");
      assertThat(((Document) m.get("text")).get("3")).isEqualTo("nested");
    }
  }
}
