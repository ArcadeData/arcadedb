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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7886, a follow-up to #7032: the JSONL exporter writes the whole {@code LocalSchema.toJSON()} object, which
 * carries {@code triggers}, {@code materializedViews}, {@code continuousAggregates}, {@code functions} and
 * {@code extensions}. {@code JsonlImporterFormat.loadSchema()} read exactly two keys out of it - {@code settings}
 * and {@code types} - so a database restored from a JSONL export came back with none of the five, with no warning,
 * no failed record count, and an import that reported success.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7886JsonlSchemaMembersRoundTripIT {
  private static final String SOURCE_PATH = "target/databases/issue7886-jsonl-source";
  private static final String TARGET_PATH = "target/databases/issue7886-jsonl-target";
  private static final String FILE        = "target/issue7886-jsonl.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void theSchemaMembersTheExportCarriesAreRestored() throws Exception {
    createSourceDatabase();

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    new Importer(("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH
        + " -forceDatabaseCreate true").split(" ")).load();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalSchema schema = target.getSchema().getEmbedded();

      assertThat(schema.existsTrigger("audit"))
          .as("a trigger carried by the export is part of the restored schema")
          .isTrue();
      assertThat(schema.getTrigger("audit").getTypeName()).isEqualTo("Account");

      assertThat(schema.existsMaterializedView("ActiveAccounts"))
          .as("so is a materialized view")
          .isTrue();

      assertThat(schema.existsContinuousAggregate("hourly_temps"))
          .as("and a continuous aggregate")
          .isTrue();

      assertThat(schema.getFunctionLibrary("acct").getFunction("twice"))
          .as("and a DEFINE FUNCTION library")
          .isNotNull();

      final JSONObject extension = schema.getExtension("someModule");
      assertThat(extension).as("and a module's extension configuration").isNotNull();
      assertThat(extension.getString("mode")).isEqualTo("fast");

      // The records still come back too: the restore of the members must not have cost anything the import used to
      // get right.
      assertThat(target.countType("Account", false)).isEqualTo(2);
      assertThat(target.query("sql", "SELECT count(*) AS c FROM SensorReading").next().<Number>getProperty("c")
          .longValue()).isEqualTo(3L);
    }
  }

  /**
   * A trigger must not FIRE during the restore: the source database already applied it and wrote the result into
   * the very export being read back, so a trigger restored alongside the types would apply it a second time.
   */
  @Test
  void aRestoredTriggerDoesNotFireOnTheImportedRecords() throws Exception {
    createSourceDatabase();

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    new Importer(("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH
        + " -forceDatabaseCreate true").split(" ")).load();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      assertThat(target.countType("AuditLog", false))
          .as("the AFTER CREATE trigger did not run for the two accounts the import restored")
          .isZero();

      // And it is live from here on: a record created after the restore does fire it.
      target.transaction(() -> target.command("sql", "INSERT INTO Account SET name = 'carol', active = true"));
      assertThat(target.countType("AuditLog", false))
          .as("the restored trigger is registered, not merely recorded")
          .isEqualTo(1);
    }
  }

  private void createSourceDatabase() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Account");
      source.command("sql", "CREATE PROPERTY Account.name STRING");
      source.command("sql", "CREATE PROPERTY Account.active BOOLEAN");
      source.command("sql", "CREATE DOCUMENT TYPE AuditLog");
      source.command("sql", "CREATE PROPERTY AuditLog.what STRING");

      source.transaction(() -> {
        source.command("sql", "INSERT INTO Account SET name = 'alice', active = true");
        source.command("sql", "INSERT INTO Account SET name = 'bob', active = false");
      });

      // The trigger is created AFTER the two accounts, so the source's own AuditLog is empty too and the assertion
      // on the target is about the restore rather than about what the source happened to contain.
      source.command("sql",
          "CREATE TRIGGER audit AFTER CREATE ON TYPE Account EXECUTE SQL \"INSERT INTO AuditLog SET what = 'created'\"");

      source.command("sql", "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true");

      source.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE) SHARDS 1");
      source.transaction(() -> {
        source.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
        source.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
        source.command("sql", "INSERT INTO SensorReading SET ts = 3000, sensor_id = 'A', temperature = 21.8");
      });

      source.command("sql", """
          CREATE CONTINUOUS AGGREGATE hourly_temps AS \
          SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp \
          FROM SensorReading GROUP BY sensor_id, hour""");

      source.command("sql", "DEFINE FUNCTION acct.twice \"SELECT :a AS result\" PARAMETERS [a] LANGUAGE sql");

      source.getSchema().getEmbedded().setExtension("someModule", new JSONObject().put("mode", "fast"));
    }
  }
}
