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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9949: {@code set database setting} (HTTP and gRPC, both through {@link ServerControlPlane#setDatabaseSetting})
 * stored the value and saved {@code configuration.json}, but never re-derived what the open database derives from its
 * configuration: the serializer's date and datetime implementations and the statement caches. Outside Raft HA nothing
 * else did either, so the change was reported as applied while the open database kept reading DATE values as the
 * previous class until it was reopened. Nor did it run the check {@code ALTER DATABASE} runs before changing the
 * implementation (issue #9976), so a REGEXP on a DATE property could be left matching no value of the new class.
 */
class Issue9949SetDatabaseSettingRederivesTest {
  @RegisterExtension
  final ServedDatabases served = new ServedDatabases();

  private static final String DB_NAME = "set9949db";

  @Test
  void dateImplementationReachesTheOpenDatabaseWithoutReopeningIt() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    database.getSchema().createDocumentType("D").createProperty("d", Type.DATE);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("D").set("d", LocalDate.of(2024, 1, 15)).save().getIdentity());
    assertThat(database.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);

    controlPlane(database).setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date");

    assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
    try (final ResultSet rs = database.query("sql", "SELECT d FROM " + rid[0])) {
      assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(Date.class);
    }
  }

  @Test
  void dateTimeImplementationReachesTheOpenDatabaseWithoutReopeningIt() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);

    controlPlane(database).setDatabaseSetting(DB_NAME, "`arcadedb.dateTimeImplementation`", "'java.util.Date'");

    assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(Date.class);
    assertThat(database.getSerializer().getSubMillisDateTimeImplementation()).isEqualTo(LocalDateTime.class);
  }

  /** The statement caches are re-derived too: a cached statement is never parsed again, so a lowered limit missed it. */
  @Test
  void aLoweredParseLimitReachesAStatementAlreadyCached() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    database.getSchema().createDocumentType("T");
    final String query = "SELECT FROM T WHERE " + "(".repeat(10) + "1 = 1" + ")".repeat(10);
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).isFalse();
    }

    controlPlane(database).setDatabaseSetting(DB_NAME, GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getKey(), "3");

    assertThatThrownBy(() -> database.query("sql", query)).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("maximum allowed depth of 3");
  }

  /** The check ALTER DATABASE runs before changing the implementation (issue #9976) is not bypassed by this path. */
  @Test
  void aChangeThatWouldBreakADateRegexpIsRefusedAndLeavesTheSettingAsItWas() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    database.command("sql", "CREATE DOCUMENT TYPE R1");
    database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
    final ServerControlPlane controlPlane = controlPlane(database);

    assertThatThrownBy(() -> controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date"))
        .hasMessageContaining("R1.d").hasMessageContaining("held as Date");

    assertThat(database.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);
    assertThat(database.getConfiguration().hasValue(GlobalConfiguration.DATE_IMPLEMENTATION.getKey())).isFalse();
    final File configurationFile = new File(database.getDatabasePath(), "configuration.json");
    assertThat(!configurationFile.exists() || !FileUtils.readFileAsString(configurationFile).contains("dateImplementation"))
        .as("a refused change must not reach configuration.json").isTrue();
    database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());

    // once the pattern is gone, the change goes through
    database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
    controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateImplementation", "java.util.Date");
    assertThat(database.getSerializer().getDateImplementation()).isEqualTo(Date.class);
  }

  /** The DATETIME twin of the check above, against the datetime implementation. */
  @Test
  void aChangeThatWouldBreakADateTimeRegexpIsRefusedAndLeavesTheSettingAsItWas() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    database.command("sql", "CREATE DOCUMENT TYPE R2");
    database.command("sql", "CREATE PROPERTY R2.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
    final ServerControlPlane controlPlane = controlPlane(database);

    assertThatThrownBy(() -> controlPlane.setDatabaseSetting(DB_NAME, "arcadedb.dateTimeImplementation", "java.util.Date"))
        .hasMessageContaining("R2.t");

    assertThat(database.getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);
    assertThat(database.getConfiguration().hasValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getKey())).isFalse();
  }

  private static ServerControlPlane controlPlane(final ServerDatabase database) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.on("getDatabase", args -> Objects.equals(args[0], DB_NAME) ? database : null);
    return new ServerControlPlane(server);
  }
}
