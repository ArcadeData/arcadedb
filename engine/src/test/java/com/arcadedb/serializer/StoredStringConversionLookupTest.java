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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A String read from a record is converted when its property is declared as a native temporal type (issue #8572). Finding
 * out cost a type and a property lookup on every String read, so the schema now remembers whether a property of such a type
 * was ever declared and the read skips the lookups until one is. These tests pin the two sides of that switch: Strings are
 * returned untouched while it is off, and a declaration made after the Strings were stored turns the conversion on, in the
 * same session and after a reopen.
 */
class StoredStringConversionLookupTest extends TestHelper {

  @Test
  void stringsAreReturnedAsTheyAreWhileNoTemporalPropertyIsDeclared() {
    database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
    database.getSchema().getType("Plain").createProperty("n", Type.LONG);
    database.transaction(() -> database.newDocument("Plain").set("code", "P1D").set("free", "10:15:30").set("n", 1L).save());

    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT code, free, n FROM Plain")) {
      final var r = rs.next();
      assertThat(r.<Object>getProperty("code")).isEqualTo("P1D");
      assertThat(r.<Object>getProperty("free")).isEqualTo("10:15:30");
      assertThat(r.<Object>getProperty("n")).isEqualTo(1L);
    }
  }

  @Test
  void declaringATemporalPropertyAfterTheStringWasStoredTurnsTheConversionOn() {
    database.getSchema().createDocumentType("Old");
    database.transaction(() -> database.newDocument("Old").set("lt", "10:15:30").set("d", "P1M2DT3S").set("s", "10:15:30").save());
    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT lt FROM Old")) {
      assertThat(rs.next().<Object>getProperty("lt")).isEqualTo("10:15:30");
    }

    database.getSchema().getType("Old").createProperty("lt", Type.LOCAL_TIME);
    database.getSchema().getType("Old").createProperty("d", Type.DURATION);

    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isTrue();
    assertReadsConverted();

    // a property of another type still reads as the String it holds
    try (final ResultSet rs = database.query("sql", "SELECT s FROM Old")) {
      assertThat(rs.next().<Object>getProperty("s")).isEqualTo("10:15:30");
    }

    reopenDatabase();

    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isTrue();
    assertReadsConverted();
  }

  @Test
  void aReopenedDatabaseWithoutTemporalPropertiesStaysOff() {
    database.getSchema().createDocumentType("Plain").createProperty("code", Type.STRING);
    database.transaction(() -> database.newDocument("Plain").set("code", "P1D").save());

    reopenDatabase();

    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT code FROM Plain")) {
      assertThat(rs.next().<Object>getProperty("code")).isEqualTo("P1D");
    }
  }

  @Test
  void droppingTheTemporalPropertyLeavesTheSwitchOnAndTheStringReadable() {
    database.getSchema().createDocumentType("Old");
    database.transaction(() -> database.newDocument("Old").set("d", "P1D").save());
    database.getSchema().getType("Old").createProperty("d", Type.DURATION);
    database.getSchema().getType("Old").dropProperty("d");

    // sticky on purpose: it can cost a lookup, never a conversion
    assertThat(database.getSchema().getEmbedded().mayHaveStoredStringConversions()).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT d FROM Old")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
    }
  }

  private void assertReadsConverted() {
    try (final ResultSet rs = database.query("sql", "SELECT lt, d FROM Old")) {
      final var r = rs.next();
      assertThat(r.<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(1, 2, 3, 0));
    }
  }
}
