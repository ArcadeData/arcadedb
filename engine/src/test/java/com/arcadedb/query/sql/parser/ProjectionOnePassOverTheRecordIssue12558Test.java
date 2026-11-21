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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code SELECT *} reads the record behind the row once, not once per property (issue #12558).
 * <p>
 * The projection copied every property of the row into the projected row, and asked the row for each
 * one by name: for a row backed by a stored record that is a walk of the record header per property,
 * since {@code ImmutableDocument.get(name)} walks the header from its first byte until it meets the
 * name. A record of P properties cost P walks, and the time per row grew with P squared.
 * <p>
 * The counting document makes the walks visible: it counts every read that walks the record header.
 */
class ProjectionOnePassOverTheRecordIssue12558Test extends TestHelper {

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

  @Test
  void theProjectionOfAWholeRowReadsTheRecordOnce() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final Result row = new ResultInternal(document);

      final ProjectionItem star = new ProjectionItem();
      star.setAll(true);
      final Projection projection = new Projection(List.of(star), false);
      final Result projected = projection.calculateSingle(new BasicCommandContext(), row);

      // the projected row carries every property of the record, and the metadata columns the projection adds
      assertThat(projected.getPropertyNames()).contains("nothing").hasSize(PROPERTIES + 3);
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(projected.<Object>getProperty("p" + i)).as("property p%d", i).isEqualTo("value-" + i);
      assertThat(projected.<Object>getProperty("nothing")).isNull();

      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      // a fixed number of passes, not one per property: the count does not grow with the property count
      assertThat(document.headerWalks()).as("passes over the record header").isLessThan(PROPERTIES);
    });
  }

  @Test
  void thePropertiesOfARecordRowAreReadOnceNotOncePerProperty() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final Result row = new ResultInternal(document);

      // what the projection of `*` does: the row's properties, read once
      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(properties).hasSize(PROPERTIES + 1);
      assertThat(properties.keySet()).contains("nothing");
      for (int i = 0; i < PROPERTIES; i++)
        assertThat(properties).as("property p%d", i).containsEntry("p" + i, "value-" + i);
      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      assertThat(document.headerWalks()).as("passes over the record header").isLessThan(PROPERTIES);
    });
  }

  @Test
  void anExcludedPropertyIsNotReadFromTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final Result row = new ResultInternal(document);

      final Projection projection = ((SelectStatement) new SQLAntlrParser(database).parse("SELECT *, !p0 FROM Wide")).getProjection();
      final Result projected = projection.calculateSingle(new BasicCommandContext(), row);

      assertThat(projected.getPropertyNames()).doesNotContain("p0").contains("nothing").hasSize(PROPERTIES + 2);
      for (int i = 1; i < PROPERTIES; i++)
        assertThat(projected.<Object>getProperty("p" + i)).as("property p%d", i).isEqualTo("value-" + i);

      // the record is read once, for the names the projection keeps: the excluded value is never decoded
      assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
      assertThat(document.wholeReads()).as("reads decoding every value, the excluded one included").isZero();
      assertThat(document.selectiveReads()).as("reads restricted to the kept names").isEqualTo(1);
      assertThat(document.lastSelectedNames()).doesNotContain("p0").hasSize(PROPERTIES);
    });
  }

  /**
   * A filter of more than 8 names is sorted and binary-searched by the serializer instead of scanned. The names come in
   * any order and may include one the dictionary does not know: the answer is exactly the stored names asked for.
   */
  @Test
  void aWideNameFilterWithAnUnknownNameReadsExactlyTheKnownNamesAskedFor() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);

      final String[] wide = { "p31", "neverStored", "p2", "p17", "nothing", "p39", "p0", "p8", "p25", "p11" };
      final Map<String, Object> wideRead = document.propertiesAsMap(wide);
      assertThat(wideRead).containsOnlyKeys("p31", "p2", "p17", "nothing", "p39", "p0", "p8", "p25", "p11");
      for (final String name : wideRead.keySet())
        if (!name.equals("nothing"))
          assertThat(wideRead).containsEntry(name, "value-" + name.substring(1));
      assertThat(wideRead.get("nothing")).isNull();
      // the caller's array is not reordered by the sort the serializer applies to its own ids
      assertThat(wide).containsExactly("p31", "neverStored", "p2", "p17", "nothing", "p39", "p0", "p8", "p25", "p11");

      // the same names through the scanned branch (8 names or fewer) answer the same
      final Map<String, Object> narrowRead = document.propertiesAsMap("p31", "neverStored", "p2", "p17");
      assertThat(narrowRead).containsOnlyKeys("p31", "p2", "p17");
    });
  }

  @Test
  void aRowWithEveryPropertyExcludedDoesNotReadTheRecord() {
    database.transaction(() -> {
      final CountingImmutableDocument document =
          new CountingImmutableDocument(database, database.getSchema().getType("Wide"), rid);
      final ResultInternal row = new ResultInternal(document);
      final Set<String> every = new HashSet<>(document.getPropertyNames());
      document.resetCounters();

      assertThat(ResultInternal.propertiesOf(row, every)).isEmpty();
      assertThat(document.wholeReads()).isZero();
      assertThat(document.selectiveReads()).isZero();
      assertThat(document.propertyReads()).isZero();
    });
  }
}
