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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@code SELECT *} reads the record behind the row ONCE, not once per property (issue #12558).
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
      // THREE WALKS, AND NOT ONE PER PROPERTY: the projection asks the row for its names, the helper asks for them
      // again to check the read against them, and the values come from the one pass over the record. The count does
      // not grow with the property count.
      assertThat(document.headerWalks()).as("passes over the record header").isEqualTo(3);
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
      assertThat(document.headerWalks()).as("passes over the record header").isEqualTo(2);
    });
  }
}
