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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11552: {@code _sourceRID} is a reserved column on a per-record materialized view. A schemaless source record can carry
 * an undeclared field of that name, which a whole-record projection ({@code SELECT *}, {@code SELECT FROM}) copies into the row;
 * the link the refresh adds always wins over it, on the full pass and on the delta pass alike, so every row stays on its own
 * source record. A field holding the RID of ANOTHER source record is the case that matters: had it won, two rows would claim the
 * same source, and the next pass would write one over the other.
 */
class Issue11552PerRecordSourceRidReservedTest extends TestHelper {
  private static final String SOURCE_RID = MaterializedViewPerRecordRefresher.SOURCE_RID;

  @ParameterizedTest
  @ValueSource(strings = { "SELECT * FROM P", "SELECT FROM P", "SELECT *, name AS n FROM P" })
  void fullPassLinkWinsOverAnUndeclaredSourceRidField(final String query) {
    database.getSchema().createDocumentType("P");
    final RID first = insert("a", null);
    insert("b", "junk");
    insert("c", first);

    final MaterializedViewImpl view = createView(query);
    assertThat(view.isPerRecord()).as("per-record: " + query).isTrue();
    assertEveryRowOnItsOwnSource(3);

    // the explicit full pass, over rows that already exist: each one is matched by its link and written in place
    final Map<RID, RID> before = rowBySource();
    database.command("sql", "REFRESH MATERIALIZED VIEW V");
    assertEveryRowOnItsOwnSource(3);
    assertThat(rowBySource()).isEqualTo(before);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SELECT * FROM P", "SELECT FROM P" })
  void deltaPassLinkWinsOverAnUndeclaredSourceRidField(final String query) {
    database.getSchema().createDocumentType("P");
    final RID first = insert("a", null);
    final RID second = insert("b", null);
    createView(query);
    assertEveryRowOnItsOwnSource(2);

    // created with the field: a string, and the RID of another source record that already has its row
    insert("c", "junk");
    insert("d", first);
    assertEveryRowOnItsOwnSource(4);

    // an existing source gains the field on update
    database.transaction(() -> ((Document) database.lookupByRID(second, true)).modify().set(SOURCE_RID, first).save());
    assertEveryRowOnItsOwnSource(4);

    // and the full pass that follows agrees with the delta passes
    database.command("sql", "REFRESH MATERIALIZED VIEW V");
    assertEveryRowOnItsOwnSource(4);
  }

  /**
   * The classifier reads the schema at DDL time only, so a property declared after the view was created is the same case as an
   * undeclared field: the view stays per-record and the link wins.
   */
  @Test
  void propertyDeclaredAfterTheViewIsOverriddenByTheLink() {
    database.getSchema().createDocumentType("P");
    final RID first = insert("a", null);
    createView("SELECT * FROM P");
    assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView("V")).isPerRecord()).isTrue();

    database.command("sql", "CREATE PROPERTY P." + SOURCE_RID + " STRING");
    insert("b", "declared");
    insert("c", first.toString());
    assertEveryRowOnItsOwnSource(3);

    database.command("sql", "REFRESH MATERIALIZED VIEW V");
    assertEveryRowOnItsOwnSource(3);
  }

  private RID insert(final String name, final Object sourceRidField) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("P").set("name", name);
      if (sourceRidField != null)
        doc.set(SOURCE_RID, sourceRidField);
      rid[0] = doc.save().getIdentity();
    });
    return rid[0];
  }

  private MaterializedViewImpl createView(final String query) {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("V")
        .withQuery(query)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());
    return (MaterializedViewImpl) database.getSchema().getMaterializedView("V");
  }

  /** Every source record has exactly one row, linked to it, carrying its name; and the view is VALID. */
  private void assertEveryRowOnItsOwnSource(final int expectedRows) {
    assertThat(database.getSchema().getMaterializedView("V").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    assertThat(database.countType("P", true)).isEqualTo(expectedRows);

    final Map<RID, RID> rows = rowBySource();
    assertThat(rows).hasSize(expectedRows);
    try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object link = row.getProperty(SOURCE_RID);
        assertThat(link).as("the link column holds a RID, not the source's own field: " + row.toJSON()).isInstanceOf(RID.class);
        final Document source = (Document) database.lookupByRID((RID) link, true);
        assertThat(source.getTypeName()).isEqualTo("P");
        assertThat(row.<String>getProperty("name")).as("row " + row.toJSON() + " sits on its own source").isEqualTo(source.getString("name"));
      }
    }
  }

  private Map<RID, RID> rowBySource() {
    final Map<RID, RID> map = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID source = row.getProperty(SOURCE_RID);
        assertThat(map.put(source, row.getIdentity().get())).as("one row per source record").isNull();
      }
    }
    return map;
  }
}
