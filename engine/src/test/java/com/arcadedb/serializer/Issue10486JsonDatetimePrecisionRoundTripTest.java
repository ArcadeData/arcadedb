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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10486: a record's JSON did not read back for {@code DATETIME_SECOND}, {@code DATETIME_MICROS} and {@code DATETIME_NANOS}.
 * {@code toJSON()} wrote the value as a count in the column's own unit and {@code fromJSON()} reads a number as epoch milliseconds,
 * as it does everywhere a number meets a datetime (SQL, the HTTP API, the remote client), so a 2020 timestamp came back in 1970, in
 * the year 51972 or in the year 50004715. A number is milliseconds on the way out too now, which a second-precision column loses
 * nothing to; a column of microseconds or nanoseconds, which milliseconds would truncate, is written as the text the HTTP API
 * answers for it, with all its digits.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10486JsonDatetimePrecisionRoundTripTest extends TestHelper {
  private static final LocalDateTime SECONDS = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
  private static final LocalDateTime MILLIS  = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);
  private static final LocalDateTime MICROS  = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_000);
  private static final LocalDateTime NANOS   = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_789);

  private static final long MILLIS_OF_SECONDS = 1_577_934_245_000L;

  @Test
  void everyPrecisionRoundTripsThroughTheRecordJson() {
    assertRoundTrip(Type.DATETIME, MILLIS);
    assertRoundTrip(Type.DATETIME_SECOND, SECONDS);
    assertRoundTrip(Type.DATETIME_MICROS, MICROS);
    assertRoundTrip(Type.DATETIME_NANOS, NANOS);
  }

  /** The reproduction of the issue: a whole second of 2020 in each unit. */
  @Test
  void aWholeSecondOfEveryPrecisionComesBackInTheSameYear() {
    for (final Type type : new Type[] { Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS })
      assertRoundTrip(type, SECONDS);
  }

  @Test
  void aSecondPrecisionColumnWritesEpochMillisecondsLikeADatetime() {
    final MutableDocument doc = newDocument(Type.DATETIME_SECOND, SECONDS);
    assertThat(doc.toJSON(false).getLong("p")).isEqualTo(MILLIS_OF_SECONDS);
  }

  @Test
  void aMicrosecondOrNanosecondColumnWritesTextWithAllItsDigits() {
    assertThat(newDocument(Type.DATETIME_MICROS, MICROS).toJSON(false).getString("p")).endsWith(".123456");
    assertThat(newDocument(Type.DATETIME_NANOS, NANOS).toJSON(false).getString("p")).endsWith(".123456789");
  }

  @Test
  void aDatetimeColumnKeepsItsEpochMillisecondsNumber() {
    assertThat(newDocument(Type.DATETIME, MILLIS).toJSON(false).getLong("p")).isEqualTo(MILLIS_OF_SECONDS + 123);
  }

  /** The same record read from disk renders like the one that was just built. */
  @Test
  void aPersistedRecordRoundTripsToo() {
    for (final Type type : new Type[] { Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
      final LocalDateTime value = type == Type.DATETIME_SECOND ? SECONDS : type == Type.DATETIME_MICROS ? MICROS : NANOS;
      final DocumentType documentType = documentType(type);
      final AtomicReference<RID> rid = new AtomicReference<>();
      database.transaction(() -> rid.set(database.newDocument(documentType.getName()).set("p", value).save().getIdentity()));

      final Document stored = database.lookupByRID(rid.get(), true).asDocument();
      final MutableDocument back = database.newDocument(documentType.getName());
      back.fromJSON(new JSONObject(stored.toJSON(false).toString()));
      assertThat(back.get("p")).as(type.name()).isEqualTo(value);
    }
  }

  /** What every client already sends - a number of milliseconds - is still read as milliseconds for any precision. */
  @Test
  void aNumberIsStillReadAsEpochMilliseconds() {
    for (final Type type : new Type[] { Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
      final MutableDocument back = database.newDocument(documentType(type).getName());
      back.fromJSON(new JSONObject("{\"p\":" + MILLIS_OF_SECONDS + "}"));
      assertThat(back.get("p")).as(type.name()).isEqualTo(SECONDS);
    }
  }

  /**
   * A database whose datetimes are {@code ZonedDateTime} and whose schema zone is not UTC: the text of a micros/nanos column carries
   * no offset and is read back in the schema zone, so the value has to be rendered in that zone, not in the zone it happens to hold.
   */
  @Test
  void aZonedDatetimeInAnotherZoneThanTheSchemaRoundTripsToTheSameInstant() {
    final ZoneId schemaZone = ZoneId.of("America/New_York");
    database.getSchema().setZoneId(schemaZone);
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);

    for (final Type type : new Type[] { Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
      final ZonedDateTime utc = ZonedDateTime.of(type == Type.DATETIME_MICROS ? MICROS : NANOS, ZoneOffset.UTC);
      final MutableDocument source = database.newDocument(documentType(type).getName()).set("p", utc);

      final MutableDocument back = database.newDocument(source.getTypeName());
      back.fromJSON(new JSONObject(source.toJSON(false).toString()));
      assertThat(((ZonedDateTime) back.get("p")).toInstant()).as(type.name()).isEqualTo(utc.toInstant());
    }
  }

  @Test
  void aNullDatetimeStaysNull() {
    final DocumentType type = documentType(Type.DATETIME_NANOS);
    final MutableDocument doc = database.newDocument(type.getName());
    assertThat(doc.toJSON(false).has("p")).isFalse();
  }

  private void assertRoundTrip(final Type type, final LocalDateTime value) {
    final MutableDocument source = newDocument(type, value);
    final String json = source.toJSON(false).toString();

    final MutableDocument back = database.newDocument(source.getTypeName());
    back.fromJSON(new JSONObject(json));
    assertThat(back.get("p")).as("%s through %s", type, json).isEqualTo(value);
  }

  private MutableDocument newDocument(final Type type, final LocalDateTime value) {
    return database.newDocument(documentType(type).getName()).set("p", value);
  }

  private DocumentType documentType(final Type type) {
    final String name = "D" + type.name();
    if (database.getSchema().existsType(name))
      return database.getSchema().getType(name);
    final DocumentType documentType = database.getSchema().createDocumentType(name);
    documentType.createProperty("p", type);
    return documentType;
  }
}
