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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11738: {@link ResultInternal#propertiesOf(Result)} answers, for every shape of row, the names
 * {@link Result#getPropertyNames()} lists with the values {@link Result#getProperty(String)} answers, in that order, while
 * reading a row backed by a stored record once rather than once per property.
 */
class ResultInternalPropertiesOfTest extends TestHelper {
  private RID rid;

  @BeforeEach
  void createRecord() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Row");
      final MutableDocument doc = database.newDocument("Row");
      doc.set("a", 1).set("b", "two").set("c", List.of(3, 4)).set("nothing", null);
      rid = doc.save().getIdentity();
    });
  }

  @Test
  void aProjectionRowAnswersItsOwnValuesWithoutReadingTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document = countingDocument();
      final ResultInternal row = new ResultInternal(document);
      row.setProperty("x", 10);
      row.setProperty("y", null);
      row.setProperty("a", "shadows the record");
      document.resetCounters();

      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(properties).containsExactlyEntriesOf(byName(row));
      assertThat(properties.keySet()).containsExactly("x", "y", "a");
      assertThat(document.headerWalks()).as("a projection holds its values: the record behind it is not read").isZero();
    });
  }

  @Test
  void aRowWithoutARecordAnswersItsOwnValues() {
    final ResultInternal row = new ResultInternal(database);
    row.setProperty("k", 1);
    row.setProperty("nested", new ResultInternal(Map.of("n", 2)));

    assertThat(ResultInternal.propertiesOf(row)).containsExactlyEntriesOf(byName(row));
    assertThat(ResultInternal.propertiesOf(new ResultInternal(database))).isEmpty();
  }

  @Test
  void aRecordRowIsReadOnceNotOncePerProperty() {
    database.transaction(() -> {
      final CountingImmutableDocument document = countingDocument();
      final ResultInternal row = new ResultInternal(document);

      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      assertThat(document.headerWalks()).as("passes over the record header: the name list, then the values").isEqualTo(2);
      assertThat(properties.keySet()).containsExactly("a", "b", "c", "nothing");
      assertThat(properties).containsExactlyEntriesOf(byName(row));
    });
  }

  @Test
  void aRemovedPropertyStaysRemoved() {
    database.transaction(() -> {
      final ResultInternal recordRow = new ResultInternal(rid.asDocument(true));
      recordRow.removeProperty("b");
      assertThat(ResultInternal.propertiesOf(recordRow)).containsExactlyEntriesOf(byName(recordRow)).doesNotContainKey("b");

      final ResultInternal projection = new ResultInternal(rid.asDocument(true));
      projection.setProperty("x", 1);
      projection.setProperty("y", 2);
      projection.removeProperty("x");
      assertThat(ResultInternal.propertiesOf(projection)).containsExactlyEntriesOf(byName(projection)).containsOnlyKeys("y");
    });
  }

  @Test
  void aProjectionThatExcludesAPropertyLeavesItOut() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT *, !b FROM Row")) {
        final Result row = rs.next();
        assertThat(ResultInternal.propertiesOf(row)).containsExactlyEntriesOf(byName(row)).doesNotContainKey("b")
            .containsKeys("a", "c", "nothing");
      }
    });
  }

  @Test
  void aSubclassIsReadNameByName() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(rid.asDocument(true)) {
        @Override
        public <T> T getProperty(final String name) {
          return (T) ("answered by the subclass: " + name);
        }
      };

      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(properties).containsExactlyEntriesOf(byName(row));
      assertThat(properties.get("a")).isEqualTo("answered by the subclass: a");
    });
  }

  @Test
  void aValueTheSingleReadLeavesOutIsANullColumnReadOnlyOnce() {
    database.transaction(() -> {
      // A damaged record: the single read skips (and logs) a value it cannot decode, as BinarySerializer.deserializeProperties
      // does for a corrupted property
      final CountingImmutableDocument document = new CountingImmutableDocument(database, type(), rid) {
        @Override
        public Map<String, Object> toMap(final boolean includeMetadata) {
          final Map<String, Object> read = super.toMap(includeMetadata);
          read.remove("b");
          return read;
        }
      };

      final Map<String, Object> properties = ResultInternal.propertiesOf(new ResultInternal(document));

      assertThat(properties.keySet()).as("the names decide what the row holds").containsExactly("a", "b", "c", "nothing");
      assertThat(properties.get("b")).isNull();
      assertThat(properties.get("a")).isEqualTo(1);
      assertThat(document.propertyReads()).as("the value is not read (and its damage logged) a second time by name").isZero();
    });
  }

  private CountingImmutableDocument countingDocument() {
    return new CountingImmutableDocument(database, type(), rid);
  }

  private DocumentType type() {
    return database.getSchema().getType("Row");
  }

  /** What a consumer reading the row name by name builds: the reference every shape is compared with. */
  private static Map<String, Object> byName(final Result row) {
    final Map<String, Object> properties = new LinkedHashMap<>();
    for (final String name : row.getPropertyNames())
      properties.put(name, row.getProperty(name));
    return properties;
  }
}
