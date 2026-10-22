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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Issue #10590: {@code DROP MATERIALIZED VIEW} and {@code DROP CONTINUOUS AGGREGATE} unregistered the view (and cancelled its
 * refresh scheduler, and took its listeners off the source types) BEFORE they dropped the backing type, which is the step that can
 * refuse. When it refused, the statement failed with a message saying "Nothing was changed" and left a view that no longer
 * existed beside a backing type that did, holding the last rows the view computed and never updating again; no statement could
 * repair it but {@code DROP TYPE} of the backing type.
 * <p>
 * A refused schema statement changes nothing (#10421): every refusal the drop of the backing type can give - a file that cannot be
 * deleted, another view or aggregate built from the backing type - is now asked before the first change.
 * <p>
 * The file fault is injected as {@link Issue10421SchemaStatementsAreAtomicTest} does: the file of a bucket of the backing type is
 * moved aside and a non-empty directory takes its path, which {@code Files.delete} refuses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10590DropViewRefusalChangesNothingTest extends TestHelper {

  /** A directory with something in it where a file of the schema is: it cannot be deleted. */
  private record Fault(Path file, Path aside) {
    void remove() throws IOException {
      Files.deleteIfExists(file.resolve("blocker"));
      Files.deleteIfExists(file);
      Files.move(aside, file);
    }
  }

  private Fault injectFault(final String backingTypeName) throws IOException {
    assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
    final Bucket bucket = database.getSchema().getType(backingTypeName).getBuckets(false).getLast();
    final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(bucket.getFileId()).getFilePath());
    final Path aside = file.resolveSibling(file.getFileName() + ".aside");
    Files.move(file, aside);
    Files.createDirectory(file);
    Files.writeString(file.resolve("blocker"), "x");
    return new Fault(file, aside);
  }

  // ---- DROP MATERIALIZED VIEW

  @Test
  void aDropOfAViewWhoseBackingFileCannotBeDeletedIsRefusedBeforeItChangesAnything() throws IOException {
    createSource();
    database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, sum(v) AS s FROM S GROUP BY k");
    final String schemaBefore = schemaWithoutVersion();
    assertThat(viewRows("MV")).isEqualTo(3L);

    final Fault fault = injectFault("MV");
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
      assertThat(database.getSchema().existsType("MV")).isTrue();
      assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);
      assertThat(viewRows("MV")).isEqualTo(3L);
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
    assertThat(viewRows("MV")).isEqualTo(3L);

    // and the statement completes once nothing refuses it
    database.command("sql", "DROP MATERIALIZED VIEW MV");

    assertThat(database.getSchema().existsMaterializedView("MV")).isFalse();
    assertThat(database.getSchema().existsType("MV")).isFalse();
  }

  @Test
  void aRefusedDropOfAnIncrementalViewKeepsItsListenersOnTheSource() throws IOException {
    createSource();
    database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S REFRESH INCREMENTAL");
    assertThat(viewRows("MV")).isEqualTo(10L);

    final Fault fault = injectFault("MV");
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      // the view is still maintained from the source: a refused drop must not leave a view that never updates again
      database.transaction(() -> database.newDocument("S").set("k", "z").set("v", 99).save());
      assertThat(viewRows("MV")).isEqualTo(11L);
    } finally {
      fault.remove();
    }

    database.command("sql", "DROP MATERIALIZED VIEW MV");
    assertThat(database.getSchema().existsType("MV")).isFalse();
  }

  @Test
  void aViewAnotherViewIsBuiltFromCannotBeDroppedAndStaysRegistered() {
    createSource();
    database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S");
    database.command("sql", "CREATE MATERIALIZED VIEW MV2 AS SELECT k, v FROM MV");
    final String schemaBefore = schemaWithoutVersion();

    // the backing type of MV is a source type of MV2: dropping it is refused, and the refusal comes after the view was unregistered
    assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW MV")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("MV2");

    assertThat(database.getSchema().existsMaterializedView("MV")).isTrue();
    assertThat(database.getSchema().existsType("MV")).isTrue();
    assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);

    // dropping the dependant first lets the drop through
    database.command("sql", "DROP MATERIALIZED VIEW MV2");
    database.command("sql", "DROP MATERIALIZED VIEW MV");
    assertThat(database.getSchema().existsType("MV")).isFalse();
  }

  @Test
  void aPlainDropStillRemovesTheViewAndItsBackingType() {
    // the drop of the backing type is not refused by the view being dropped, which still names that type when the drop is asked
    createSource();
    database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT k, v FROM S");

    database.command("sql", "DROP MATERIALIZED VIEW MV");

    assertThat(database.getSchema().existsMaterializedView("MV")).isFalse();
    assertThat(database.getSchema().existsType("MV")).isFalse();
    assertThat(database.getSchema().existsType("S")).isTrue();
  }

  // ---- DROP CONTINUOUS AGGREGATE

  @Test
  void aDropOfAnAggregateWhoseBackingFileCannotBeDeletedIsRefusedBeforeItChangesAnything() throws IOException {
    createTimeSeriesSource();
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM TS GROUP BY h");
    final String schemaBefore = schemaWithoutVersion();
    assertThat(viewRows("CA")).isEqualTo(1L);

    final Fault fault = injectFault("CA");
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP CONTINUOUS AGGREGATE CA")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
      assertThat(database.getSchema().existsType("CA")).isTrue();
      assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);
      assertThat(viewRows("CA")).isEqualTo(1L);
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
    assertThat(viewRows("CA")).isEqualTo(1L);

    // and the statement completes once nothing refuses it
    database.command("sql", "DROP CONTINUOUS AGGREGATE CA");

    assertThat(database.getSchema().existsContinuousAggregate("CA")).isFalse();
    assertThat(database.getSchema().existsType("CA")).isFalse();
  }

  @Test
  void anAggregateAViewIsBuiltFromCannotBeDroppedAndStaysRegistered() {
    createTimeSeriesSource();
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM TS GROUP BY h");
    database.command("sql", "CREATE MATERIALIZED VIEW MVC AS SELECT h, s FROM CA");
    final String schemaBefore = schemaWithoutVersion();

    assertThatThrownBy(() -> database.command("sql", "DROP CONTINUOUS AGGREGATE CA")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("MVC");

    assertThat(database.getSchema().existsContinuousAggregate("CA")).isTrue();
    assertThat(database.getSchema().existsType("CA")).isTrue();
    assertThat(schemaWithoutVersion()).isEqualTo(schemaBefore);

    database.command("sql", "DROP MATERIALIZED VIEW MVC");
    database.command("sql", "DROP CONTINUOUS AGGREGATE CA");
    assertThat(database.getSchema().existsType("CA")).isFalse();
  }

  // ---- helpers

  /** The schema as it is written, without the version it bumps at every save. */
  private String schemaWithoutVersion() {
    final JSONObject json = ((LocalSchema) database.getSchema()).toJSON();
    json.remove("schemaVersion");
    return json.toString();
  }

  private long viewRows(final String name) {
    return database.query("sql", "SELECT FROM " + name).stream().count();
  }

  private void createSource() {
    database.command("sql", "CREATE DOCUMENT TYPE S BUCKETS 2");
    database.command("sql", "CREATE PROPERTY S.k STRING");
    database.command("sql", "CREATE PROPERTY S.v INTEGER");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("S").set("k", "k" + (i % 3)).set("v", i).save();
    });
  }

  private void createTimeSeriesSource() {
    database.command("sql", "CREATE TIMESERIES TYPE TS TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO TS SET ts = ?, sensor = 'a', v = ?", 1_700_000_000_000L + i * 1000L, i * 1.0);
    });
  }
}
