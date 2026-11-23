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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A property read that an interrupt stops is not a damaged record. A parallel type scan stopped by its LIMIT interrupts its
 * producers, and every external property they were reading from disk failed with an {@link InterruptedIOException} that the
 * serializer logged as SEVERE "Possible corrupted record", with a stack trace, and answered as {@code null}. The interrupt now
 * reaches the reader, which is what cancels it, and nothing is logged.
 * <p>
 * Every place that used to take any failure for a damaged record is covered: the external value of a record read from disk
 * (the real case), and a record whose header does not decode (the cases that do not need a page to be evicted). A record that
 * is really damaged, read by a thread that is not interrupted, is still logged and answered as it always was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class InterruptedPropertyReadTest extends TestHelper {
  private static final String[] DAMAGED_RECORD_LOGS = { "Possible corrupted record", "Error on loading property",
      "Skipping corrupted property" };
  private static final Object   ABSENT              = new Object();

  /** The ways to read the external property of a record whose external page is not in the cache. */
  private enum ExternalRead {
    GET, GET_IF_PRESENT, TO_MAP, TO_JSON, STRICT, LOCATED
  }

  /** The ways to read a record whose header does not decode. */
  private enum DamagedRead {
    PROPERTY_NAMES, HAS_PROPERTY, PROPERTY, PROPERTIES, LOCATED_PROPERTIES, PROPERTY_AT
  }

  private final float[] vector = new float[768];
  private       RID     rid;

  @Test
  void anInterruptedExternalReadFailsInsteadOfReadingNullAsACorruptedRecord() {
    createRecordWithExternalValue();
    final ImmutableDocument doc = documentWithEvictedExternalValue();

    final List<String> reported = WarningCapture.captureSevere(() -> {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> doc.get("e")).as("an interrupted read must not answer null")
            .hasRootCauseInstanceOf(InterruptedIOException.class);
      } finally {
        Thread.interrupted();
      }
    });
    assertThat(damagedRecordLines(reported)).as("an interrupted read is not a corrupted record").isEmpty();

    // the record is intact: read again without the interrupt, the value is there
    assertThat(rid.asDocument(true).get("e")).isEqualTo(vector);
  }

  @ParameterizedTest
  @EnumSource(ExternalRead.class)
  void everyReadOfAnExternalValueFailsWhenInterruptedInsteadOfLoggingACorruptedRecord(final ExternalRead how) {
    createRecordWithExternalValue();
    final ImmutableDocument doc = documentWithEvictedExternalValue();

    final List<String> reported = WarningCapture.captureWarnings(() -> {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> read(how, doc)).as("an interrupted %s must not answer from what it could read", how)
            .isNotInstanceOf(SerializationException.class).hasRootCauseInstanceOf(InterruptedIOException.class);
        assertThat(Thread.currentThread().isInterrupted()).as("the interrupt reaches the caller of the read").isTrue();
      } finally {
        Thread.interrupted();
      }
    });
    assertThat(damagedRecordLines(reported)).as("an interrupted %s is not a corrupted record", how).isEmpty();

    // the record is intact: read again without the interrupt, the value is there
    assertThat(rid.asDocument(true).get("e")).isEqualTo(vector);
  }

  @ParameterizedTest
  @EnumSource(DamagedRead.class)
  void aDamagedHeaderReadByAnInterruptedThreadIsRethrownInsteadOfLogged(final DamagedRead how) {
    final byte[] record = recordWithInflatedPropertyCount();

    final List<String> reported = WarningCapture.captureWarnings(() -> {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> readDamaged(how, record)).as("an interrupted %s must fail", how)
            .isInstanceOf(RuntimeException.class);
        assertThat(Thread.currentThread().isInterrupted()).as("the interrupt reaches the caller of the read").isTrue();
      } finally {
        Thread.interrupted();
      }
    });
    assertThat(damagedRecordLines(reported)).as("an interrupted %s is not reported as a damaged record", how).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(DamagedRead.class)
  void aDamagedHeaderIsStillLoggedAndAnsweredAsBeforeByAThreadThatIsNotInterrupted(final DamagedRead how) {
    final byte[] record = recordWithInflatedPropertyCount();

    final Object[] answer = new Object[1];
    final List<String> reported = WarningCapture.captureWarnings(() -> answer[0] = readDamaged(how, record));

    assertThat(answer[0]).as("the answer of %s for a damaged record", how).isEqualTo(answerOfADamagedRecord(how));
    if (how == DamagedRead.LOCATED_PROPERTIES)
      // NOT LOGGED THERE: THE CALLER READS EVERY PROPERTY ON ITS OWN, WHICH LOGS THE DAMAGED RECORD
      assertThat(damagedRecordLines(reported)).as("%s leaves the report to the caller", how).isEmpty();
    else
      assertThat(damagedRecordLines(reported)).as("a damaged record read through %s is reported", how).hasSize(1);
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test
  void aStrictReadOfADamagedHeaderStillReportsTheDamageWhenNotInterrupted() {
    final byte[] record = recordWithInflatedPropertyCount();
    final DatabaseInternal db = (DatabaseInternal) database;

    assertThatThrownBy(() -> db.getSerializer().deserializePropertiesStrict(db, bufferPositionedAt(record, 1), null)).isInstanceOf(
        SerializationException.class);
  }

  private Object read(final ExternalRead how, final ImmutableDocument doc) {
    final DatabaseInternal db = (DatabaseInternal) database;
    return switch (how) {
      case GET -> doc.get("e");
      case GET_IF_PRESENT -> doc.getIfPresent("e", null);
      case TO_MAP -> doc.toMap();
      case TO_JSON -> doc.toJSON();
      case STRICT -> db.getSerializer().deserializePropertiesStrict(db, doc.getBuffer(), rid);
      case LOCATED -> {
        // WHAT A SCAN THAT READS SEVERAL PROPERTIES OF A RECORD DOES: FIND THEM IN ONE PASS OVER THE HEADER, THEN READ EACH
        final int nameId = db.getSchema().getDictionary().getIdByName("e", false);
        final int[] slotByNameId = new int[nameId + 1];
        Arrays.fill(slotByNameId, -1);
        slotByNameId[nameId] = 0;
        final int[] positions = new int[1];
        final Binary located = doc.locateProperties(slotByNameId, positions, 1);
        assertThat(located).as("the header of the record reads").isNotNull();
        yield doc.getPropertyAt(located, "e", positions[0], null);
      }
    };
  }

  private Object readDamaged(final DamagedRead how, final byte[] record) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final BinarySerializer serializer = db.getSerializer();
    return switch (how) {
      case PROPERTY_NAMES -> serializer.getPropertyNames(db, bufferPositionedAt(record, 1), rid);
      case HAS_PROPERTY -> serializer.hasProperty(db, bufferPositionedAt(record, 1), "alpha", rid);
      case PROPERTY -> serializer.deserializeProperty(db, bufferPositionedAt(record, 1), null, "alpha", rid, ABSENT);
      case PROPERTIES -> serializer.deserializeProperties(db, bufferPositionedAt(record, 1), null, rid);
      case LOCATED_PROPERTIES -> serializer.locateProperties(bufferPositionedAt(record, 1), new int[] { 0 }, new int[1], 1);
      // A POSITION PAST THE END OF THE CONTENT: THE VALUE CANNOT BE READ
      case PROPERTY_AT -> serializer.deserializePropertyAt(db, bufferPositionedAt(record, 1), null, "alpha", rid,
          record.length + 50);
    };
  }

  private static Object answerOfADamagedRecord(final DamagedRead how) {
    return switch (how) {
      case PROPERTY_NAMES -> Set.of();
      case HAS_PROPERTY, LOCATED_PROPERTIES -> false;
      case PROPERTY -> ABSENT;
      case PROPERTIES -> Map.of();
      case PROPERTY_AT -> null;
    };
  }

  private void createRecordWithExternalValue() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DocumentType type = db.getSchema().createDocumentType("Doc");
    type.createProperty("e", Type.ARRAY_OF_FLOATS).setExternal(true);
    for (int i = 0; i < vector.length; i++)
      vector[i] = i / 768f;
    db.transaction(() -> rid = db.newDocument("Doc").set("k", "a").set("e", vector).save().getIdentity());
  }

  /** The record is read now; its external value stays on disk until a read asks for it. */
  private ImmutableDocument documentWithEvictedExternalValue() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pm = db.getPageManager();
    pm.waitAllPagesOfDatabaseAreFlushed(db);
    pm.removeAllReadPagesOfDatabase(db);
    final ImmutableDocument doc = (ImmutableDocument) rid.asDocument(true);
    pm.removeAllReadPagesOfDatabase(db);
    return doc;
  }

  /** A record whose property count does not fit its header: nothing of it decodes, and no I/O is involved. */
  private byte[] recordWithInflatedPropertyCount() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final byte[][] record = new byte[1][];
    db.transaction(() -> {
      db.getSchema().createDocumentType("Plain");
      final MutableDocument doc = db.newDocument("Plain").set("alpha", "one").set("beta", "two");
      record[0] = db.getSerializer().serialize(db, doc).toByteArray();
    });
    // THE COUNT VARINT FOLLOWS THE RECORD TYPE BYTE AND THE HEADER END OFFSET INT
    record[0][Binary.BYTE_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE] = 10;
    return record[0];
  }

  private static Binary bufferPositionedAt(final byte[] record, final int position) {
    final ByteBuffer dest = ByteBuffer.allocate(record.length);
    dest.put(record);
    dest.flip();
    final Binary buffer = new Binary(dest);
    buffer.position(position);
    return buffer;
  }

  /** Only the lines about a damaged record: another thread of the JVM may log anything else meanwhile. */
  private static List<String> damagedRecordLines(final List<String> reported) {
    return reported.stream().filter(line -> {
      for (final String marker : DAMAGED_RECORD_LOGS)
        if (line.contains(marker))
          return true;
      return false;
    }).toList();
  }
}
