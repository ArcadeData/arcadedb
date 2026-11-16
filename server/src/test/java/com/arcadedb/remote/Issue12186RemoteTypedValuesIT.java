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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.server.BaseGraphServerTest;
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

/**
 * Issues #12186 and #12187: a value of an undeclared property saved with save(), or bound as a parameter, kept its Java type
 * only as far as plain JSON could carry it. A link became text, a record an embedded copy, a decimal a double, binary a list
 * and every date-time a number. The stored type is read on the server with {@code <property>.type()}.
 */
class Issue12186RemoteTypedValuesIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-12186";

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
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    server.create(DATABASE_NAME);
    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sql", "CREATE DOCUMENT TYPE Doc");
    return db;
  }
}
