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

import java.util.HashSet;
import java.util.LinkedHashMap;
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
}
