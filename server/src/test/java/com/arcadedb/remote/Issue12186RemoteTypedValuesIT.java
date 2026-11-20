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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #12186 and #12187: a value of an undeclared property saved with save(), or bound as a parameter, kept its Java type
 * only as far as plain JSON could carry it. A link became text, a record an embedded copy, a decimal a double, binary a list
 * and every date-time a number. The stored type is read on the server with {@code <property>.type()}.
 */
class Issue12186RemoteTypedValuesIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-12186";

  private RemoteDatabase remote;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  private static Map<String, Object> values(final RID target) {
    final Map<String, Object> values = new LinkedHashMap<>();
    values.put("link", target);
    values.put("decimal", new BigDecimal("0.10"));
    values.put("binary", new byte[] { 1, 2, 3 });
    values.put("day", LocalDate.of(2024, 2, 29));
    values.put("localDateTime", LocalDateTime.of(2024, 2, 29, 13, 14, 15));
    values.put("javaDate", new Date(1700000000123L));
    values.put("instant", Instant.ofEpochMilli(1700000000123L));
    values.put("zoned", ZonedDateTime.of(2024, 1, 1, 10, 0, 0, 0, ZoneId.of("Europe/Rome")));
    return values;
  }

  private static final Map<String, String> STORED_TYPES = Map.of("link", "LINK", "decimal", "DECIMAL", "binary", "BINARY", "day",
      "DATE", "localDateTime", "DATETIME", "javaDate", "DATETIME", "instant", "DATETIME", "zoned", "ZONED_DATETIME");

  @Test
  void saveKeepsTheTypeOfAnUndeclaredProperty() {
    final RemoteDatabase db = database();
    final RID target = target(db);
    final Map<String, Object> values = values(target);

    final MutableDocument doc = db.newDocument("Doc");
    values.forEach(doc::set);
    doc.save();

    assertStored(db, doc.getIdentity(), values);
    assertThat(db.query("sql", "SELECT link.name AS n FROM " + doc.getIdentity()).next().<String>getProperty("n")).isEqualTo("target");
  }

  @Test
  void updateKeepsTheTypeOfAnUndeclaredProperty() {
    final RemoteDatabase db = database();
    final RID target = target(db);
    final Map<String, Object> values = values(target);

    final MutableDocument doc = db.newDocument("Doc");
    doc.set("name", "first");
    doc.save();
    final MutableDocument modified = db.lookupByRID(doc.getIdentity(), true).asDocument().modify();
    values.forEach(modified::set);
    modified.save();

    assertStored(db, doc.getIdentity(), values);
  }

  @Test
  void aRecordSetAsAValueIsStoredAsALink() {
    final RemoteDatabase db = database();
    final RID target = target(db);
    final MutableDocument doc = db.newDocument("Doc");
    doc.set("owner", db.lookupByRID(target, true));
    doc.set("owners", List.of(target));
    doc.save();

    final Result row = db.query("sql",
        "SELECT owner.type() AS t, owner.name AS n, owners[0].type() AS lt FROM " + doc.getIdentity()).next();
    assertThat(row.<String>getProperty("t")).isEqualTo("LINK");
    assertThat(row.<String>getProperty("n")).isEqualTo("target");
    assertThat(row.<String>getProperty("lt")).isEqualTo("LINK");
  }

  @Test
  void aSavedValueIsFoundByAParameterOfTheSameJavaValue() {
    final RemoteDatabase db = database();
    final Map<String, Object> values = values(target(db));
    final MutableDocument doc = db.newDocument("Doc");
    values.forEach(doc::set);
    doc.save();

    for (final Map.Entry<String, Object> e : values.entrySet())
      assertThat(db.query("sql", "SELECT count(*) AS c FROM Doc WHERE " + e.getKey() + " = :v", Map.of("v", e.getValue())).next()
          .<Long>getProperty("c")).as(e.getKey()).isEqualTo(1L);
  }

  @Test
  void collectionsNullsAndUnusualNamesKeepTheirShape() {
    final RemoteDatabase db = database();
    final MutableDocument doc = db.newDocument("Doc");
    doc.set("numbers", List.of(1, 2, 3));
    doc.set("words", List.of("a", "b"));
    doc.set("nested", Map.of("inner", List.of(1, 2)));
    doc.set("nothing", null);
    doc.set("we\"ird \\ name\n", "x");
    doc.save();

    final Result row = db.query("sql", "SELECT numbers.type() AS n, words.type() AS w, nested.type() AS m, nested.inner.type() AS mi, "
        + "(nothing IS DEFINED) AS d FROM " + doc.getIdentity()).next();
    assertThat(row.<String>getProperty("n")).isEqualTo("LIST");
    assertThat(row.<String>getProperty("w")).isEqualTo("LIST");
    assertThat(row.<String>getProperty("m")).isEqualTo("MAP");
    assertThat(row.<String>getProperty("mi")).isEqualTo("LIST");
    assertThat(row.<Boolean>getProperty("d")).isTrue();
    final Document read = db.lookupByRID(doc.getIdentity(), true).asDocument();
    assertThat(read.getPropertyNames()).contains("we\"ird \\ name\n", "nothing");
    assertThat(read.getString("we\"ird \\ name\n")).isEqualTo("x");
  }

  @Test
  void parametersAreBoundWithTheirJavaType() {
    final RemoteDatabase db = database();
    final Map<String, Object> values = values(target(db));
    for (final Map.Entry<String, Object> e : values.entrySet()) {
      final RID rid = db.command("sql", "INSERT INTO Doc SET v = :v", Map.of("v", e.getValue())).next().getIdentity().get();
      assertThat(db.query("sql", "SELECT v.type() AS t FROM " + rid).next().<String>getProperty("t")).as(e.getKey())
          .isEqualTo(STORED_TYPES.get(e.getKey()));
    }
  }

  @Test
  void decimalParametersKeepTheirPrecisionAndScale() {
    final RemoteDatabase db = database();
    assertThat(db.query("sql", "SELECT :a + :b AS s", Map.of("a", new BigDecimal("0.1"), "b", new BigDecimal("0.2"))).next()
        .<Object>getProperty("s")).isEqualTo(new BigDecimal("0.3"));

    db.command("sql", "CREATE PROPERTY Doc.price DECIMAL");
    final RID rid = db.command("sql", "INSERT INTO Doc SET price = ?", new BigDecimal("19.90")).next().getIdentity().get();
    assertThat(db.query("sql", "SELECT price.asString() AS p FROM " + rid).next().<String>getProperty("p")).isEqualTo("19.90");
  }

  /**
   * An embedded document used to travel inside the JSON text of the CONTENT clause; it is a parameter now, a map with its
   * {@code @type}. A record read, modified in one property and saved again writes every other property back, the embedded
   * documents among them.
   */
  @Test
  void aSavedRecordKeepsItsEmbeddedDocuments() {
    final RemoteDatabase db = database();
    db.command("sql", "CREATE DOCUMENT TYPE Emb");
    final RID rid = db.command("sql", "INSERT INTO Doc CONTENT {\"name\": \"first\", \"emb\": {\"@type\": \"Emb\", \"x\": 1, \"s\": \"a\"}, "
        + "\"embs\": [{\"@type\": \"Emb\", \"x\": 2}, {\"@type\": \"Emb\", \"x\": 3}]}").next().getIdentity().get();

    final String projection = "SELECT name, emb.type() AS et, emb.x AS ex, emb.s AS es, emb.@type AS ety, embs.type() AS lt, "
        + "embs[0].@type AS e0ty, embs[1].x AS e1x FROM " + rid;
    final Map<String, Object> before = db.query("sql", projection).next().toMap();

    final MutableDocument modified = db.lookupByRID(rid, true).asDocument().modify();
    modified.set("name", "second");
    modified.save();

    final Map<String, Object> after = db.query("sql", projection).next().toMap();
    assertThat(after.get("name")).isEqualTo("second");
    assertThat(after.get("et")).isEqualTo("EMBEDDED");
    assertThat(((Number) after.get("ex")).intValue()).isEqualTo(1);
    assertThat(after.get("es")).isEqualTo("a");
    assertThat(after.get("ety")).isEqualTo("Emb");
    assertThat(after.get("lt")).isEqualTo("LIST");
    assertThat(after.get("e0ty")).isEqualTo("Emb");
    assertThat(((Number) after.get("e1x")).intValue()).isEqualTo(3);
    // whatever the server says about the record, it says it as before the save, apart from the property that was modified
    before.put("name", "second");
    assertThat(after).isEqualTo(before);
  }

  /**
   * A new embedded document built on the client, alone and in a list, travels as a parameter too (a document without an
   * identity is encoded as a map of its properties with its {@code @type}).
   */
  @Test
  void aNewEmbeddedDocumentIsStoredAsEmbedded() {
    final RemoteDatabase db = database();
    db.command("sql", "CREATE DOCUMENT TYPE Emb");
    final RID rid = db.command("sql", "INSERT INTO Doc CONTENT {\"name\": \"first\", \"emb\": {\"@type\": \"Emb\", \"x\": 1}}").next()
        .getIdentity().get();
    final Document read = db.lookupByRID(rid, true).asDocument();

    final MutableDocument doc = db.newDocument("Doc");
    doc.set("copy", read.get("emb"));
    doc.set("copies", List.of(read.get("emb"), read.get("emb")));
    doc.save();

    final Result row = db.query("sql", "SELECT copy.type() AS t, copy.x AS x, copies.type() AS lt, copies[1].x AS lx FROM " + doc.getIdentity())
        .next();
    assertThat(row.<String>getProperty("t")).isEqualTo("EMBEDDED");
    assertThat(row.<Number>getProperty("x").intValue()).isEqualTo(1);
    assertThat(row.<String>getProperty("lt")).isEqualTo("LIST");
    assertThat(row.<Number>getProperty("lx").intValue()).isEqualTo(1);
  }

  @Test
  void aVertexSaveKeepsTheTypeOfAnUndeclaredProperty() {
    final RemoteDatabase db = database();
    db.command("sql", "CREATE VERTEX TYPE Node");
    final Map<String, Object> values = values(target(db));

    final MutableVertex vertex = db.newVertex("Node");
    values.forEach(vertex::set);
    vertex.save();

    assertStored(db, vertex.getIdentity(), values);
  }

  /** A marker that does not decode is a client error, and a decimal the JSON number parser would refuse is one too. */
  @Test
  void aHostileDecimalIsRefusedByTheServer() {
    final RemoteDatabase db = database();
    for (final String text : List.of("1E+999999999", "1E-999999999"))
      assertThatThrownBy(() -> db.query("sql", "SELECT :p + 1 AS s", Map.of("p", new BigDecimal(text))).next())
          .as(text)
          .hasMessageContaining("$decimal");
    // the connection is still usable
    assertThat(db.query("sql", "SELECT :p + 1 AS s", Map.of("p", new BigDecimal("1.5"))).next().<Object>getProperty("s"))
        .isEqualTo(new BigDecimal("2.5"));
  }

  private void assertStored(final RemoteDatabase db, final RID rid, final Map<String, Object> values) {
    for (final String name : values.keySet())
      assertThat(db.query("sql", "SELECT " + name + ".type() AS t FROM " + rid).next().<String>getProperty("t")).as(name)
          .isEqualTo(STORED_TYPES.get(name));
    assertThat(db.query("sql", "SELECT decimal.asString() AS d FROM " + rid).next().<String>getProperty("d")).isEqualTo("0.10");
  }

  private static RID target(final RemoteDatabase db) {
    return db.command("sql", "INSERT INTO Doc SET name = 'target'").next().getIdentity().get();
  }

  private RemoteDatabase database() {
    remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root", DEFAULT_PASSWORD_FOR_TESTS);
    remote.command("sql", "CREATE DOCUMENT TYPE Doc");
    return remote;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    if (remote != null)
      remote.close();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
