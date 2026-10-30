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

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10865: the remote Java client sent a date as epoch MILLISECONDS, which the server reads as epoch DAYS for a DATE, when
 * the DATE property was declared on a supertype of the record's type, and whenever the date was bound as a query parameter.
 * 26.10.2 refused the write with "Invalid value for EpochDay"; earlier versions stored null.
 */
class Issue10865RemoteInheritedDateAndDateParameterIT extends BaseGraphServerTest {
  private static final String    DATABASE_NAME = "remote-database-10865";
  private static final LocalDate DAY           = LocalDate.of(2024, 1, 15);

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void dateDeclaredOnTheTypeItself() {
    final RemoteDatabase db = schema();
    db.transaction(() -> db.newDocument("Evento").set("fecha", DAY).save());
    assertThat(dates(db, "Evento")).containsExactly(DAY);
  }

  @Test
  void localDateForADateInheritedFromASupertype() {
    final RemoteDatabase db = schema();
    db.transaction(() -> {
      db.newVertex("Hijo").set("fecha", DAY).save();
      db.newVertex("Nieto").set("fecha", DAY).save();
    });
    // SELECT FROM Hijo is polymorphic: it reads the Nieto record too
    assertThat(dates(db, "Hijo")).containsExactly(DAY, DAY);
    assertThat(dates(db, "Nieto")).containsExactly(DAY);
  }

  @Test
  void javaUtilDateForADateInheritedFromASupertype() {
    final RemoteDatabase db = schema();
    final Date afternoon = Date.from(DAY.atTime(15, 30).toInstant(ZoneOffset.UTC));
    db.transaction(() -> {
      db.newVertex("Hijo").set("fecha", afternoon).save();
      db.newVertex("Nieto").set("fecha", afternoon).save();
    });
    // SELECT FROM Hijo is polymorphic: it reads the Nieto record too
    assertThat(dates(db, "Hijo")).containsExactly(DAY, DAY);
    assertThat(dates(db, "Nieto")).containsExactly(DAY);
  }

  @Test
  void updateOfAnInheritedDate() {
    final RemoteDatabase db = schema();
    db.transaction(() -> db.newVertex("Nieto").set("fecha", DAY).save());
    final Date nextDay = Date.from(DAY.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC));
    db.transaction(() -> {
      try (final ResultSet rs = db.query("sql", "SELECT FROM Nieto")) {
        rs.next().toElement().asVertex().modify().set("fecha", nextDay).save();
      }
    });
    assertThat(dates(db, "Nieto")).containsExactly(DAY.plusDays(1));
  }

  @Test
  void schemaSeesAPropertyDeclaredTwoLevelsUp() {
    final RemoteDatabase db = schema();
    assertThat(db.getSchema().getType("Hijo").existsPolymorphicProperty("fecha")).isTrue();
    assertThat(db.getSchema().getType("Nieto").existsPolymorphicProperty("fecha")).isTrue();
    assertThat(db.getSchema().getType("Nieto").existsPolymorphicProperty("missing")).isFalse();
  }

  @Test
  void localDateBoundAsAPositionalParameter() {
    final RemoteDatabase db = schema();
    db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY);
    db.command("sql", "INSERT INTO Hijo SET fecha = ?", DAY);
    assertThat(dates(db, "Evento")).containsExactly(DAY);
    assertThat(dates(db, "Hijo")).containsExactly(DAY);
  }

  @Test
  void localDateBoundAsANamedParameter() {
    final RemoteDatabase db = schema();
    db.command("sql", "INSERT INTO Evento SET fecha = :fecha", Map.of("fecha", DAY));
    assertThat(dates(db, "Evento")).containsExactly(DAY);
  }

  @Test
  void localDatesInAListParameter() {
    final RemoteDatabase db = schema();
    db.command("sql", "INSERT INTO Evento SET fechas = ?", List.of(DAY, DAY.plusDays(1)));
    try (final ResultSet rs = db.query("sql", "SELECT fechas FROM Evento")) {
      final List<Object> read = rs.next().getProperty("fechas");
      // compared as text: the remote client reads the items of a LIST OF DATE back as their text, a read-side gap of its own
      assertThat(read).extracting(Object::toString).containsExactly(DAY.toString(), DAY.plusDays(1).toString());
    }
  }

  /**
   * Issue #10908: a comparison reads a number as epoch days, so the epoch milliseconds the client used to send for a LocalDate never
   * matched. The date goes as a typed marker now.
   */
  @Test
  void localDateInAComparison() {
    final RemoteDatabase db = schema();
    db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY);
    db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY.plusDays(1));
    try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha = ?", DAY)) {
      assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha = :d", Map.of("d", DAY))) {
      assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha IN ?", List.of(DAY, DAY.plusDays(1)))) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
    try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha > ?", DAY)) {
      assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY.plusDays(1));
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void midnightMillisInARecordContent() {
    final RemoteDatabase db = schema();
    final long millis = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    db.command("sql", "INSERT INTO Evento CONTENT {\"fecha\": " + millis + "}");
    db.command("sql", "INSERT INTO Evento CONTENT {\"fecha\": " + DAY.toEpochDay() + "}");
    assertThat(dates(db, "Evento")).containsExactly(DAY, DAY);
  }

  private RemoteDatabase schema() {
    final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sqlscript", """
        CREATE DOCUMENT TYPE Evento;
        CREATE PROPERTY Evento.fecha DATE;
        CREATE PROPERTY Evento.fechas LIST OF DATE;
        CREATE VERTEX TYPE Base;
        CREATE PROPERTY Base.fecha DATE;
        CREATE VERTEX TYPE Hijo EXTENDS Base;
        CREATE VERTEX TYPE Nieto EXTENDS Hijo;
        """);
    return db;
  }

  private static List<Object> dates(final RemoteDatabase db, final String typeName) {
    final List<Object> result = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", "SELECT fecha FROM " + typeName)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("fecha"));
    }
    return result;
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
