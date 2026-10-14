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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9997: a String read from a record is converted when its property is declared as a native temporal type (issue
 * #8572), and finding that out cost a polymorphic property lookup on every String read of every type. Each type now
 * remembers whether it, or a super type, declares a property of such a type, and the read skips the lookup while it does
 * not. The answer is per type and exact: a temporal property elsewhere in the schema does not make this type pay, and
 * dropping the last one gives the fast path back. These tests pin both sides of that switch, and every schema change that
 * moves it (create, drop, rename, a super type added or removed, a reopen).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9997StoredStringConversionMemoTest extends TestHelper {

  @Test
  void stringsAreReturnedAsTheyAreWhileNoTemporalPropertyIsDeclared() {
    database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
    database.getSchema().getType("Plain").createProperty("n", Type.LONG);
    database.transaction(() -> database.newDocument("Plain").set("code", "P1D").set("free", "10:15:30").set("n", 1L).save());

    assertThat(converts("Plain")).isFalse();
    final Result r = first("SELECT code, free, n FROM Plain");
    assertThat(r.<Object>getProperty("code")).isEqualTo("P1D");
    assertThat(r.<Object>getProperty("free")).isEqualTo("10:15:30");
    assertThat(r.<Object>getProperty("n")).isEqualTo(1L);
  }

  @Test
  void aTemporalPropertyOnAnotherTypeDoesNotTurnThisTypeOn() {
    database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
    database.getSchema().createDocumentType("Timed").createProperty("d", Type.DURATION);

    assertThat(converts("Plain")).isFalse();
    assertThat(converts("Timed")).isTrue();
  }

  @Test
  void declaringATemporalPropertyAfterTheStringWasStoredTurnsTheConversionOnAlsoAfterAReopen() {
    database.getSchema().createDocumentType("Old");
    database.transaction(() -> database.newDocument("Old").set("lt", "10:15:30").set("d", "P1M2DT3S").set("s", "10:15:30").save());
    assertThat(converts("Old")).isFalse();
    assertThat(first("SELECT lt FROM Old").<Object>getProperty("lt")).isEqualTo("10:15:30");

    database.getSchema().getType("Old").createProperty("lt", Type.LOCAL_TIME);
    database.getSchema().getType("Old").createProperty("d", Type.DURATION);

    assertThat(converts("Old")).isTrue();
    assertReadsConverted();
    // a property of another type still reads as the String it holds
    assertThat(first("SELECT s FROM Old").<Object>getProperty("s")).isEqualTo("10:15:30");

    reopenDatabase();

    assertThat(converts("Old")).isTrue();
    assertReadsConverted();
  }

  @Test
  void aReopenedDatabaseWithoutTemporalPropertiesStaysOff() {
    database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
    database.transaction(() -> database.newDocument("Plain").set("code", "P1D").save());

    reopenDatabase();

    assertThat(converts("Plain")).isFalse();
    assertThat(first("SELECT code FROM Plain").<Object>getProperty("code")).isEqualTo("P1D");
  }

  @Test
  void droppingTheLastTemporalPropertyTurnsTheConversionOff() {
    database.getSchema().createDocumentType("Old");
    database.transaction(() -> database.newDocument("Old").set("d", "P1D").save());
    database.getSchema().getType("Old").createProperty("d", Type.DURATION);
    assertThat(converts("Old")).isTrue();
    assertThat(first("SELECT d FROM Old").<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));

    database.getSchema().getType("Old").dropProperty("d");

    assertThat(converts("Old")).isFalse();
    assertThat(first("SELECT d FROM Old").<Object>getProperty("d")).isEqualTo("P1D");
  }

  @Test
  void renamingATemporalPropertyMovesTheConversionToTheNewName() {
    database.getSchema().createDocumentType("Renamed");
    database.transaction(() -> database.newDocument("Renamed").set("a", "P1D").set("b", "P2D").save());
    database.getSchema().getType("Renamed").createProperty("a", Type.DURATION);
    assertThat(first("SELECT a, b FROM Renamed").<Object>getProperty("b")).isEqualTo("P2D");

    database.getSchema().getType("Renamed").renameProperty("a", "b");

    assertThat(converts("Renamed")).isTrue();
    final Result r = first("SELECT a, b FROM Renamed");
    assertThat(r.<Object>getProperty("a")).isEqualTo("P1D");
    assertThat(r.<Object>getProperty("b")).isEqualTo(new CypherDuration(0, 2, 0, 0));
  }

  @Test
  void aSubTypeFollowsTheTemporalPropertiesOfItsSuperTypes() {
    database.getSchema().createDocumentType("Base");
    database.getSchema().createDocumentType("Child");
    database.transaction(() -> database.newDocument("Child").set("lt", "10:15:30").save());
    assertThat(converts("Child")).isFalse();

    // A SUPER TYPE ADDED LATER BRINGS ITS TEMPORAL PROPERTY
    database.getSchema().getType("Base").createProperty("lt", Type.LOCAL_TIME);
    assertThat(converts("Child")).isFalse();
    database.getSchema().getType("Child").addSuperType("Base");
    assertThat(converts("Child")).isTrue();
    assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));

    // ...AND TAKES IT AWAY WHEN IT IS REMOVED
    database.getSchema().getType("Child").removeSuperType("Base");
    assertThat(converts("Child")).isFalse();
    assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo("10:15:30");

    // A PROPERTY DECLARED ON THE SUPER TYPE AFTER THE LINK REACHES THE SUB TYPE TOO
    database.getSchema().getType("Base").dropProperty("lt");
    database.getSchema().getType("Child").addSuperType("Base");
    assertThat(converts("Child")).isFalse();
    database.getSchema().getType("Base").createProperty("lt", Type.LOCAL_TIME);
    assertThat(converts("Child")).isTrue();
    assertThat(first("SELECT lt FROM Child").<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
  }

  @Test
  void droppingTheSuperTypeThatDeclaresTheTemporalPropertyTurnsTheSubTypeOff() {
    database.getSchema().createDocumentType("Top");
    database.getSchema().createDocumentType("Middle").createProperty("lt", Type.LOCAL_TIME);
    database.getSchema().createDocumentType("Leaf");
    database.getSchema().getType("Leaf").addSuperType("Middle");
    database.transaction(() -> database.newDocument("Leaf").set("lt2", "10:15:30").save());
    assertThat(converts("Leaf")).isTrue();

    // A MIDDLE TYPE WITHOUT SUPER TYPES: THE DROP RE-LINKS NOTHING, SO NO addSuperType() MOVES THE SERIAL FOR IT
    database.getSchema().dropType("Middle");

    assertThat(database.getSchema().getType("Leaf").getSuperTypes()).isEmpty();
    assertThat(converts("Leaf")).isFalse();

    // ...AND ONE BETWEEN TWO TYPES: THE SUB TYPE IS RE-LINKED TO THE TOP, WHICH DECLARES NOTHING
    database.getSchema().createDocumentType("Middle2").createProperty("lt", Type.LOCAL_TIME);
    database.getSchema().getType("Middle2").addSuperType("Top");
    database.getSchema().getType("Leaf").addSuperType("Middle2");
    assertThat(converts("Leaf")).isTrue();

    database.getSchema().dropType("Middle2");

    assertThat(database.getSchema().getType("Leaf").getSuperTypes()).extracting("name").containsExactly("Top");
    assertThat(converts("Leaf")).isFalse();
  }

  @Test
  void anEmbeddedDocumentFollowsItsOwnType() {
    database.getSchema().createDocumentType("Slot");
    database.getSchema().createDocumentType("Agenda");
    database.transaction(() -> {
      final var agenda = database.newDocument("Agenda");
      agenda.newEmbeddedDocument("Slot", "slot").set("at", "10:15:30").set("note", "10:15:30");
      agenda.save();
    });
    // DECLARED AFTER THE VALUE WAS STORED AS A String. IT IS THE EMBEDDED DOCUMENT'S TYPE THAT DECIDES, NOT THE PARENT'S,
    // WHICH DECLARES NOTHING
    database.getSchema().getType("Slot").createProperty("at", Type.LOCAL_TIME);
    assertThat(converts("Agenda")).isFalse();
    assertThat(converts("Slot")).isTrue();
    final Result r = first("SELECT slot.at AS at, slot.note AS note FROM Agenda");
    assertThat(r.<Object>getProperty("at")).isEqualTo(LocalTime.of(10, 15, 30));
    assertThat(r.<Object>getProperty("note")).isEqualTo("10:15:30");
  }

  /**
   * deserializeProperties() now tells only a value that can hold an embedded document where it lives: an embedded
   * document nested in a list and in a map, read through the whole-record path (toJSON() and a SELECT of the
   * collections), must still find its owner, and its own temporal property must still be converted.
   */
  @Test
  void embeddedDocumentsInsideAListAndAMapReadThroughTheWholeRecordPath() {
    database.getSchema().createDocumentType("Slot");
    database.getSchema().createDocumentType("Planner");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument planner = database.newDocument("Planner");
      planner.set("title", "10:15:30");
      planner.set("slots", new ArrayList<>());
      planner.newEmbeddedDocument("Slot", "slots").set("at", "10:15:30");
      planner.newEmbeddedDocument("Slot", "byName", "morning").set("at", "11:00");
      rid[0] = planner.save().getIdentity();
    });
    // DECLARED AFTER THE VALUES WERE STORED AS Strings
    database.getSchema().getType("Slot").createProperty("at", Type.LOCAL_TIME);

    final Document reloaded = database.lookupByRID(rid[0], true).asDocument();
    final JSONObject json = reloaded.toJSON();
    assertThat(json.getString("title")).isEqualTo("10:15:30");
    assertThat(json.getJSONArray("slots").getJSONObject(0).getString("@type")).isEqualTo("Slot");
    assertThat(json.getJSONObject("byName").getJSONObject("morning").getString("@type")).isEqualTo("Slot");

    final List<?> slots = reloaded.getList("slots");
    assertThat(((Document) slots.getFirst()).get("at")).isEqualTo(LocalTime.of(10, 15, 30));
    final Map<String, ?> byName = reloaded.getMap("byName");
    assertThat(((Document) byName.get("morning")).get("at")).isEqualTo(LocalTime.of(11, 0));

    final Result r = first("SELECT slots, byName, title FROM Planner");
    assertThat(r.<Object>getProperty("title")).isEqualTo("10:15:30");
    assertThat(((Document) r.<List<?>>getProperty("slots").getFirst()).get("at")).isEqualTo(LocalTime.of(10, 15, 30));
    assertThat(((Document) r.<Map<String, ?>>getProperty("byName").get("morning")).get("at")).isEqualTo(LocalTime.of(11, 0));
  }

  private boolean converts(final String typeName) {
    return ((LocalDocumentType) database.getSchema().getType(typeName)).mayConvertStoredStrings();
  }

  private Result first(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next();
    }
  }

  private void assertReadsConverted() {
    final Result r = first("SELECT lt, d FROM Old");
    assertThat(r.<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
    assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(1, 2, 3, 0));
  }
}
