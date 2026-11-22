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
package com.arcadedb.postgres;

import com.arcadedb.TestHelper;
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Postgres DataRow writer reads a row's columns in one pass over its record (#12558).
 * <p>
 * The writer asked the row for every column, and for a row backed by a stored record that is a walk of
 * the record header per column, so a record of P columns cost P walks and the time per row grew with
 * P squared. The columns are now read from the row's properties, which the executor reads once.
 * <p>
 * The counting document makes the walks visible: it counts every read that walks the record header.
 */
class PostgresOnePassRowReadTest extends TestHelper {

  private static final int PROPERTIES = 40;

  private RID rid;

  @BeforeEach
  void createRecord() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Wide");
      final MutableDocument doc = database.newDocument("Wide");
      doc.set("nothing", null);
      for (int i = 0; i < PROPERTIES; i++)
        doc.set("p" + i, "value-" + i);
      rid = doc.save().getIdentity();
    });
  }

  // Regression: #12558
  @Test
  void theColumnsOfARecordRowAreReadOnceNotOncePerColumn() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final Result row = new ResultInternal(document);

      // what the DataRow writer does for a row: the names the row lists, with the values the row answers
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(properties).hasSize(PROPERTIES + 1);
      assertThat(properties.keySet()).contains("nothing");
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(properties).as("column p%d", i).containsEntry("p" + i, "value-" + i);
      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      assertThat(document.headerWalks()).as("passes over the record header").isLessThan(PROPERTIES);
    });
  }

  // Regression: #12558
  @Test
  void everyColumnOfARecordRowIsAnsweredWithoutAnotherWalkOverTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final Result row = new ResultInternal(document);

      // what the writer does: the row's properties read once, then one column value per column
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(PostgresNetworkExecutor.columnValue(row, "p" + i, properties)).as("column p%d", i)
            .isEqualTo("value-" + i);

      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      assertThat(document.headerWalks()).as("passes over the record header, once for the row and not once per column")
          .isLessThan(PROPERTIES);
    });
  }

  // Regression: #12558
  @Test
  void aColumnAProjectionRowHoldsAsNullIsNotReadFromTheElement() {
    database.transaction(() -> {
      final Map<String, Object> content = new LinkedHashMap<>();
      content.put("p0", null);
      final ResultInternal row = new ResultInternal(content).setElement(database.lookupByRID(rid, true).asDocument());
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      // the row wrote p0 itself, so the element's "value-0" does not answer for it
      assertThat(properties).containsEntry("p0", null);
      assertThat(PostgresNetworkExecutor.columnValue(row, "p0", properties)).isNull();
    });
  }

  // Regression: #12558
  @Test
  void aMetadataOnlyRowDoesNotDecodeTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final ResultInternal row = new ResultInternal(document);
      final Set<String> excludes = new HashSet<>(document.getPropertyNames());
      excludes.add("@rid");
      excludes.add("@type");
      row.setProjectionExcludes(excludes);
      document.resetCounters();

      // what `SELECT *, !p0, ..., !@rid, !@type` leaves: no visible property, so nothing of the record is decoded
      assertThat(ResultInternal.propertiesOf(row)).isEmpty();
      assertThat(document.wholeReads()).isZero();
      assertThat(document.selectiveReads()).isZero();
      assertThat(PostgresNetworkExecutor.columnValue(row, "@cat", Map.of())).isEqualTo("d");
    });
  }

  // Regression: #13012
  @Test
  void theColumnNamesOfARecordRowAreTheKeysOfItsPropertiesNotAnotherWalkOverTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final ResultInternal row = new ResultInternal(document);
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);
      document.resetCounters();

      final Set<String> names = PostgresNetworkExecutor.columnNamesOf(row, properties);

      assertThat(document.headerWalks()).as("the names are the keys of the properties already read").isZero();
      assertThat(names).containsExactlyElementsOf(document.getPropertyNames()).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void aDamagedRecordRowStillAnnouncesTheNameOfTheValueItCannotDecode() {
    database.transaction(() -> {
      // the single read of the record skips a value it cannot decode: propertiesOf keeps its name, with a null value
      final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid) {
        @Override
        public Map<String, Object> toMap(final boolean includeMetadata) {
          final Map<String, Object> read = super.toMap(includeMetadata);
          read.remove("p3");
          return read;
        }
      };
      final ResultInternal row = new ResultInternal(document);
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      final Set<String> names = PostgresNetworkExecutor.columnNamesOf(row, properties);

      assertThat(properties).containsEntry("p3", null);
      assertThat(names).containsExactlyElementsOf(recordNames()).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void aRowThatHoldsValuesOfItsOwnListsTheRecordNamesAndThenItsOwn() {
    database.transaction(() -> {
      final Map<String, Object> content = new LinkedHashMap<>();
      content.put("extra", 1);
      final ResultInternal row = new ResultInternal(content).setElement(database.lookupByRID(rid, true).asDocument());

      final List<String> expected = new ArrayList<>(recordNames());
      expected.add("extra");
      assertThat(columnNamesOfRow(row)).containsExactlyElementsOf(expected).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void aRemovedPropertyIsStillAnnouncedAsItAlwaysWas() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(database.lookupByRID(rid, true).asDocument());
      row.removeProperty("p1");

      assertThat(ResultInternal.propertiesOf(row)).doesNotContainKey("p1");
      assertThat(columnNamesOfRow(row)).containsExactlyElementsOf(recordNames()).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void anExcludedPropertyIsLeftOutOfTheColumnNames() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(database.lookupByRID(rid, true).asDocument());
      row.setProjectionExcludes(Set.of("p2"));

      final List<String> expected = new ArrayList<>(recordNames());
      expected.remove("p2");
      assertThat(columnNamesOfRow(row)).containsExactlyElementsOf(expected).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void aRowSubclassStillListsTheRecordNamesAndThenTheRow() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(database.lookupByRID(rid, true).asDocument()) {
        @Override
        public Set<String> getPropertyNames() {
          return Set.of("answered by the subclass");
        }
      };

      final List<String> expected = new ArrayList<>(recordNames());
      expected.add("answered by the subclass");
      assertThat(columnNamesOfRow(row)).containsExactlyElementsOf(expected).containsExactlyElementsOf(legacyColumnNamesOf(row));
    });
  }

  // Regression: #13012
  @Test
  void aRowWithNoRecordListsItsOwnNames() {
    final Map<String, Object> content = new LinkedHashMap<>();
    content.put("k", 1);
    content.put("a", 2);
    final ResultInternal row = new ResultInternal(content);

    assertThat(columnNamesOfRow(row)).containsExactly("k", "a").containsExactlyElementsOf(legacyColumnNamesOf(row));
  }

  private List<String> recordNames() {
    return new ArrayList<>(database.lookupByRID(rid, true).asDocument().getPropertyNames());
  }

  private static Set<String> columnNamesOfRow(final Result row) {
    return PostgresNetworkExecutor.columnNamesOf(row, ResultInternal.propertiesOf(row));
  }

  /** What {@code columnNamesOf} listed for every row before #13012: the record's names minus the projection's exclusions, then the row's. */
  private static Set<String> legacyColumnNamesOf(final Result row) {
    final Set<String> names = new LinkedHashSet<>();
    if (row.isElement()) {
      final Set<String> excludes = row instanceof ResultInternal internal ? internal.getProjectionExcludes() : null;
      for (final String name : row.getElement().get().getPropertyNames())
        if (excludes == null || !excludes.contains(name))
          names.add(name);
    }
    names.addAll(row.getPropertyNames());
    return names;
  }
}
