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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10205: a REFRESH INCREMENTAL view stayed VALID but stale after writes to a subtype of its source type, and after
 * writes to any type its query reads besides the first FROM identifier (a subquery in FROM, a subquery in WHERE or in the
 * projection).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10205IncrementalViewSourcesTest extends TestHelper {

  private static final String COUNT    = "SELECT count(*) AS c FROM Item";
  private static final String GROUPED  = "SELECT grp, count(*) AS c FROM (SELECT grp FROM Item) GROUP BY grp";
  private static final String OWNED    = "SELECT grp FROM Item WHERE grp IN (SELECT name FROM Owner)";
  private static final String PROJECTS = "SELECT grp, (SELECT count(*) AS n FROM Owner)[0].n AS owners FROM Item";

  @BeforeEach
  void setUpTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE DOCUMENT TYPE ItemB EXTENDS Item");
    database.command("sql", "CREATE DOCUMENT TYPE Owner");
    database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Item SET grp = 'x'");
      database.command("sql", "INSERT INTO ItemB SET grp = 'y'");
      database.command("sql", "INSERT INTO Owner SET name = 'x'");
    });
  }

  @Test
  void writesToASubtypeRefreshTheView() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
    assertSameAsQuery("Vc", COUNT);

    database.transaction(() -> database.command("sql", "INSERT INTO ItemB SET grp = 'y'"));
    assertSameAsQuery("Vc", COUNT);
    assertThat(viewRows("Vc")).containsExactly("c=3");

    database.transaction(() -> database.command("sql", "DELETE FROM ItemB WHERE grp = 'y' LIMIT 1"));
    assertThat(viewRows("Vc")).containsExactly("c=2");
    assertThat(statusOf("Vc")).isEqualTo("VALID");
  }

  @Test
  void aSubtypeCreatedAfterTheViewRefreshesItToo() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
    database.command("sql", "CREATE DOCUMENT TYPE ItemC EXTENDS ItemB");

    database.transaction(() -> database.command("sql", "INSERT INTO ItemC SET grp = 'z'"));
    assertThat(viewRows("Vc")).containsExactly("c=3");

    database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'w'"));
    assertSameAsQuery("Vc", COUNT);

    database.transaction(() -> database.command("sql", "DELETE FROM ItemC"));
    assertThat(viewRows("Vc")).containsExactly("c=2");
    assertSameAsQuery("Vc", COUNT);
  }

  @Test
  void aViewGroupingASubtypeSeesItsUpdates() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vg AS " + GROUPED + " REFRESH INCREMENTAL");
    database.command("sql", "CREATE DOCUMENT TYPE ItemC EXTENDS Item");
    database.transaction(() -> database.command("sql", "INSERT INTO ItemC SET grp = 'x'"));
    database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'q'"));
    assertSameAsQuery("Vg", GROUPED);

    reopenDatabase();
    database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'r'"));
    assertSameAsQuery("Vg", GROUPED);
    assertThat(viewRows("Vg")).containsExactly("c=1 grp=r", "c=1 grp=x", "c=1 grp=y");
  }

  @Test
  void aSubqueryInFromIsTracked() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vg AS " + GROUPED + " REFRESH INCREMENTAL");
    assertThat(sourceTypesOf("Vg")).containsExactly("Item");

    database.transaction(() -> database.command("sql", "INSERT INTO Item SET grp = 'z'"));
    assertSameAsQuery("Vg", GROUPED);
    assertThat(viewRows("Vg")).containsExactly("c=1 grp=x", "c=1 grp=y", "c=1 grp=z");
  }

  @Test
  void aSubqueryInWhereIsTracked() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
    assertThat(sourceTypesOf("Vo")).containsExactlyInAnyOrder("Item", "Owner");

    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
    assertSameAsQuery("Vo", OWNED);
    assertThat(viewRows("Vo")).containsExactly("grp=x", "grp=y");
  }

  @Test
  void aSubqueryInTheProjectionIsTracked() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vp AS " + PROJECTS + " REFRESH INCREMENTAL");

    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'q'"));
    assertSameAsQuery("Vp", PROJECTS);
    assertThat(viewRows("Vp")).containsExactly("grp=x owners=2", "grp=y owners=2");
  }

  @Test
  void aBackQuotedSourceAndParentReferencesInANestedSubquery() {
    final String query = "SELECT grp, (SELECT count(*) AS n FROM `Owner` WHERE name = $parent.$current.grp)[0].n AS owners FROM `Item`";
    database.command("sql", "CREATE MATERIALIZED VIEW Vn AS " + query + " REFRESH INCREMENTAL");
    // THE $ VARIABLES ARE NOT SOURCES, THE BACK-QUOTED NAMES ARE
    assertThat(sourceTypesOf("Vn")).containsExactlyInAnyOrder("Item", "Owner");

    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
    assertSameAsQuery("Vn", query);
    assertThat(viewRows("Vn")).containsExactly("grp=x owners=1", "grp=y owners=1");
  }

  /** The count of views on the polymorphic registries follows every lifecycle step, so it never drifts. */
  @Test
  void theListenerCountFollowsCreateAlterReopenAndDrop() {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    assertThat(schema.hasPolymorphicViewListeners()).isFalse();

    database.command("sql", "CREATE MATERIALIZED VIEW Va AS " + COUNT + " REFRESH INCREMENTAL");
    database.command("sql", "CREATE MATERIALIZED VIEW Vb AS " + OWNED + " REFRESH INCREMENTAL");
    assertThat(schema.hasPolymorphicViewListeners()).isTrue();

    database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH MANUAL");
    database.command("sql", "ALTER MATERIALIZED VIEW Vb REFRESH MANUAL");
    assertThat(schema.hasPolymorphicViewListeners()).isFalse();

    database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH INCREMENTAL");
    database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH INCREMENTAL");
    assertThat(schema.hasPolymorphicViewListeners()).isTrue();

    reopenDatabase();
    final LocalSchema reopened = (LocalSchema) database.getSchema();
    assertThat(reopened.hasPolymorphicViewListeners()).isTrue();

    database.command("sql", "DROP MATERIALIZED VIEW Va");
    assertThat(reopened.hasPolymorphicViewListeners()).isFalse();
    database.command("sql", "DROP MATERIALIZED VIEW Vb");
    assertThat(reopened.hasPolymorphicViewListeners()).isFalse();

    // AND THE REMAINING VIEW STILL REFRESHES AFTER ALL OF IT
    database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
    database.transaction(() -> database.command("sql", "INSERT INTO ItemB SET grp = 'z'"));
    assertSameAsQuery("Vc", COUNT);
    database.command("sql", "DROP MATERIALIZED VIEW Vc");
    assertThat(reopened.hasPolymorphicViewListeners()).isFalse();
  }

  /** A source named only inside a subquery is a dependency a rename cannot carry with certainty: refused, not broken. */
  @Test
  void renamingASourceReadOnlyByASubqueryIsRefused() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Owner NAME Owner2"))
        .hasMessageContaining("materialized view 'Vo'");
    assertThat(database.getSchema().existsType("Owner")).isTrue();
    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
    assertSameAsQuery("Vo", OWNED);
  }

  @Test
  void writesToATypeTheQueryDoesNotReadDoNotRefresh() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Vo");
    final long refreshes = view.getRefreshCount();

    database.transaction(() -> database.command("sql", "INSERT INTO Unrelated SET name = 'x'"));
    assertThat(view.getRefreshCount()).isEqualTo(refreshes);
  }

  @Test
  void aTypeReadOnlyByASubqueryCannotBeDroppedUnderTheView() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Owner"))
        .hasMessageContaining("source type for materialized view 'Vo'");
  }

  @Test
  void sourcesThatNoTypeEventReportsAreRefusedForIncremental() {
    for (final String query : new String[] {
        "SELECT FROM (TRAVERSE out() FROM Item)",
        "SELECT FROM bucket:Item",
        "SELECT FROM (MATCH {type: Item, as: i} RETURN i)" }) {
      assertThatThrownBy(() -> database.command("sql", "CREATE MATERIALIZED VIEW Vx AS " + query + " REFRESH INCREMENTAL"))
          .as(query)
          .hasMessageContaining("REFRESH INCREMENTAL");
      assertThat(database.getSchema().existsMaterializedView("Vx")).as(query).isFalse();
      assertThat(database.getSchema().existsType("Vx")).as(query).isFalse();
    }

    // THE SAME QUERY IS FINE FOR A VIEW REFRESHED ON DEMAND, BUT CANNOT BE ALTERED TO INCREMENTAL
    database.command("sql", "CREATE MATERIALIZED VIEW Vt AS SELECT FROM (TRAVERSE out() FROM Item) REFRESH MANUAL");
    assertThatThrownBy(() -> database.command("sql", "ALTER MATERIALIZED VIEW Vt REFRESH INCREMENTAL"))
        .isInstanceOfAny(CommandExecutionException.class, SchemaException.class)
        .hasMessageContaining("REFRESH INCREMENTAL");
    assertThat(database.getSchema().getMaterializedView("Vt").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
  }

  @Test
  void theListenersSurviveAReopen() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
    reopenDatabase();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO ItemB SET grp = 'z'");
      database.command("sql", "INSERT INTO Owner SET name = 'z'");
    });
    assertSameAsQuery("Vc", COUNT);
    assertSameAsQuery("Vo", OWNED);
  }

  /** A view stored before every source was tracked lists only its first FROM identifier: the load completes the list. */
  @Test
  void aViewStoredWithOnlyItsFirstSourceTracksAllAfterLoad() throws Exception {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
    database.close();

    final File schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath()));
    schema.getJSONObject("materializedViews").getJSONObject("Vo").put("sourceTypes", new JSONArray().put("Item"));
    Files.writeString(schemaFile.toPath(), schema.toString());

    database = factory.open();
    assertThat(sourceTypesOf("Vo")).containsExactlyInAnyOrder("Item", "Owner");
    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
    assertSameAsQuery("Vo", OWNED);
  }

  @Test
  void anAlterToIncrementalTracksEverySource() {
    database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH MANUAL");
    database.command("sql", "ALTER MATERIALIZED VIEW Vo REFRESH INCREMENTAL");

    database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
    assertSameAsQuery("Vo", OWNED);
  }

  private void assertSameAsQuery(final String view, final String definingQuery) {
    assertThat(viewRows(view)).as(view).isEqualTo(rows(definingQuery));
    assertThat(statusOf(view)).as(view).isEqualTo("VALID");
  }

  private List<String> viewRows(final String view) {
    return rows("SELECT FROM " + view);
  }

  private List<String> rows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final List<String> values = new ArrayList<>();
        for (final String p : r.getPropertyNames())
          if (!p.startsWith("@"))
            values.add(p + "=" + r.getProperty(p));
        Collections.sort(values);
        rows.add(String.join(" ", values));
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private String statusOf(final String view) {
    return database.getSchema().getMaterializedView(view).getStatus();
  }

  private List<String> sourceTypesOf(final String view) {
    return database.getSchema().getMaterializedView(view).getSourceTypeNames();
  }
}
