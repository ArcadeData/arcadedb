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

import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #9111 (merged #9022, #9025): the element validation of {@code LIST OF} / {@code MAP OF} refused every
 * value of a date type, and let anything through when the declared element type is a document type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9111ListOfDateAndDocumentTypesTest extends TestHelper {
  private static final LocalDateTime LDT = LocalDateTime.of(2026, 10, 3, 10, 20, 30, 123456000);

  private Object read(final RID rid, final String property) {
    return database.lookupByRID(rid, true).asDocument().get(property);
  }

  @Test
  void listOfDateTimePrecisionsAcceptsEveryDateValue() {
    int i = 0;
    for (final String declared : new String[] { "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS" }) {
      final String t = "L" + (i++);
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".p LIST OF " + declared);
      for (final Object v : new Object[] { LDT, LDT.toLocalDate(), Date.from(LDT.toInstant(ZoneOffset.UTC)),
          "2026-10-03 10:20:30.123456" }) {
        final RID[] rid = new RID[1];
        database.transaction(() -> rid[0] = database.newDocument(t).set("p", new ArrayList<>(List.of(v))).save().getIdentity());
        assertThat((List<?>) read(rid[0], "p")).as(declared + " " + v.getClass().getSimpleName()).hasSize(1);
      }
    }
  }

  @Test
  void listOfDateIsStoredAsDate() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.p LIST OF DATE");
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newDocument("D").set("p", new ArrayList<>(List.of(LDT.toLocalDate(), "2026-10-04"))).save()
            .getIdentity());
    final List<?> stored = (List<?>) read(rid[0], "p");
    assertThat(stored).hasSize(2);
    assertThat(stored.get(0)).isEqualTo(LocalDate.of(2026, 10, 3));
    assertThat(stored.get(1)).isEqualTo(LocalDate.of(2026, 10, 4));
  }

  @Test
  void listOfDatetimeMicrosKeepsMicrosecondPrecision() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    database.command("sql", "CREATE PROPERTY M.p LIST OF DATETIME_MICROS");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("M").set("p", new ArrayList<>(List.of(LDT))).save().getIdentity());
    assertThat(((List<?>) read(rid[0], "p")).get(0)).isEqualTo(LDT);
  }

  @Test
  void mapOfDateTypesAcceptsDateValues() {
    database.command("sql", "CREATE DOCUMENT TYPE MM");
    database.command("sql", "CREATE PROPERTY MM.a MAP OF DATETIME_MICROS");
    database.command("sql", "CREATE PROPERTY MM.b MAP OF DATE");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("MM").set("a", new LinkedHashMap<>(Map.of("k", LDT)))
        .set("b", new LinkedHashMap<>(Map.of("k", "2026-10-03"))).save().getIdentity());
    assertThat(((Map<?, ?>) read(rid[0], "a")).get("k")).isEqualTo(LDT);
    assertThat(((Map<?, ?>) read(rid[0], "b")).get("k")).isEqualTo(LocalDate.of(2026, 10, 3));
  }

  @Test
  void sqlInsertIntoListOfDatetimeMicros() {
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.p LIST OF DATETIME_MICROS");
    database.transaction(() -> database.command("sql", "INSERT INTO S SET p = ['2026-10-03 10:20:30.123456']").close());
    assertThat(database.countType("S", true)).isEqualTo(1);
  }

  @Test
  void listOfDateStillRefusesNonDates() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.p LIST OF DATETIME_MICROS");
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("R").set("p", new ArrayList<>(List.of("not a date"))).save())).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("'p'");
  }

  private void declareAddress() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.addresses LIST OF Address");
  }

  @Test
  void listOfDocumentTypeRefusesScalars() {
    declareAddress();
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(1, "two"))).save())).isInstanceOf(
        ValidationException.class).hasMessageContaining("LIST of 'Address'");
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("sql", "INSERT INTO Person SET addresses = [1, 'two']").close()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void listOfDocumentTypeRefusesAMapOfAnotherType() {
    declareAddress();
    final Map<String, Object> other = new LinkedHashMap<>();
    other.put("@type", "Other");
    other.put("city", "Rome");
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(other))).save())).isInstanceOf(
        ValidationException.class).hasMessageContaining("Other");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "INSERT INTO Person SET addresses = [{\"@type\": \"Other\", \"city\": \"Rome\"}]").close())).isInstanceOf(
        ValidationException.class);
  }

  @Test
  void listOfDocumentTypeTurnsAPlainMapIntoAnEmbeddedDocument() {
    declareAddress();
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Person")
        .set("addresses", new ArrayList<>(List.of(new LinkedHashMap<>(Map.of("city", "Rome"))))).save().getIdentity());
    final List<?> stored = (List<?>) read(rid[0], "addresses");
    assertThat(stored).hasSize(1);
    assertThat(stored.get(0)).isInstanceOf(EmbeddedDocument.class);
    assertThat(((EmbeddedDocument) stored.get(0)).getTypeName()).isEqualTo("Address");
    assertThat(((EmbeddedDocument) stored.get(0)).getString("city")).isEqualTo("Rome");
  }

  @Test
  void listOfDocumentTypeAcceptsATypedMapOfTheDeclaredType() {
    declareAddress();
    final Map<String, Object> address = new LinkedHashMap<>();
    address.put("@type", "Address");
    address.put("city", "Rome");
    database.transaction(() -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(address))).save());
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET addresses = [{\"city\": \"Milan\"}]").close());
    assertThat(database.countType("Person", true)).isEqualTo(2);
  }

  @Test
  void mapOfDocumentTypeTurnsPlainMapValuesIntoEmbeddedDocuments() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Person")
        .set("byName", new LinkedHashMap<>(Map.of("home", new LinkedHashMap<>(Map.of("city", "Rome"))))).save().getIdentity());
    final Object home = ((Map<?, ?>) read(rid[0], "byName")).get("home");
    assertThat(home).isInstanceOf(EmbeddedDocument.class);
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Person").set("byName", new LinkedHashMap<>(Map.of("home", 1))).save())).isInstanceOf(
        ValidationException.class);
  }

  @Test
  void listOfDocumentTypeAcceptsNullElement() {
    declareAddress();
    final List<Object> withNull = new ArrayList<>();
    withNull.add(null);
    database.transaction(() -> database.newDocument("Person").set("addresses", withNull).save());
    assertThat(database.countType("Person", true)).isEqualTo(1);
  }

  @Test
  void listOfStringStillRefusesADate() {
    database.command("sql", "CREATE DOCUMENT TYPE LS");
    database.command("sql", "CREATE PROPERTY LS.p LIST OF STRING");
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("LS").set("p", new ArrayList<>(List.of(new Date()))).save())).isInstanceOf(
        ValidationException.class);
  }

  @Test
  void listOfDateAcceptsInstantAndZonedDateTime() {
    database.command("sql", "CREATE DOCUMENT TYPE LZ");
    database.command("sql", "CREATE PROPERTY LZ.p LIST OF DATE");
    database.command("sql", "CREATE PROPERTY LZ.q LIST OF DATETIME_MICROS");
    database.transaction(() -> database.newDocument("LZ")
        .set("p", new ArrayList<>(List.of(java.time.Instant.now(), java.time.ZonedDateTime.now())))
        .set("q", new ArrayList<>(List.of(java.time.Instant.now(), java.time.ZonedDateTime.now()))).save());
    assertThat(database.countType("LZ", true)).isEqualTo(1);
  }

  @Test
  void listOfDocumentTypeStillAcceptsALinkWrittenAsAString() {
    declareAddress();
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Address").set("city", "Rome").save().getIdentity());
    database.transaction(() -> database.newDocument("Person").set("addresses", new ArrayList<>(List.of(rid[0].toString()))).save());
    assertThat(database.countType("Person", true)).isEqualTo(1);
  }

  @Test
  void jsonContentWithAPlainMapInAListOfDocumentType() {
    declareAddress();
    database.transaction(() -> database.command("sql", "INSERT INTO Person CONTENT {\"addresses\": [{\"city\": \"Rome\"}]}").close());
    final Object first = ((List<?>) database.query("sql", "SELECT FROM Person").next().getProperty("addresses")).get(0);
    assertThat(first).isNotNull();
    assertThat(database.countType("Person", true)).isEqualTo(1);
  }
}
