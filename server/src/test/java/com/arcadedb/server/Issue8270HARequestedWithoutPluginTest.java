/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8270: a server started with high availability requested opens its databases refusing writes, because the HA
 * plugin that wraps them for replication starts only after the network listeners do. This module has no HA plugin on
 * its classpath, so startup has to conclude that none is coming and let the node run standalone - as its warning
 * says - with every database writable: the one opened at startup, and one created afterwards.
 */
class Issue8270HARequestedWithoutPluginTest extends StaticBaseServerTest {
  private static final String DATABASE = "issue8270";

  private ArcadeDBServer server;

  @AfterEach
  @Override
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    super.endTest();
  }

  @Test
  void aStandaloneFallbackLeavesEveryDatabaseWritable() {
    try (final DatabaseFactory factory = new DatabaseFactory("./target/databases0/" + DATABASE);
        final Database database = factory.create()) {
      database.getSchema().createDocumentType("Doc");
    }

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_NAME, "issue8270");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases0");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    configuration.setValue(GlobalConfiguration.HA_ENABLED, true);
    configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2424");

    server = new ArcadeDBServer(configuration);
    server.start();

    assertThat(server.isHARequested()).isTrue();
    assertThat(server.getHA()).as("no HA plugin in this module").isNull();

    final ServerDatabase loaded = server.getDatabase(DATABASE);
    assertThat(((LocalDatabase) loaded.getWrappedDatabaseInstance()).getWriteRefusal())
        .as("the database opened while the HA plugin was expected accepts writes once startup gave up on it").isNull();
    loaded.transaction(() -> loaded.newDocument("Doc").set("n", 1).save());
    assertThat(loaded.countType("Doc", true)).isEqualTo(1L);

    final ServerDatabase created = server.createDatabase(DATABASE + "_created", ComponentFile.MODE.READ_WRITE);
    assertThat(((LocalDatabase) created.getWrappedDatabaseInstance()).getWriteRefusal()).isNull();
    created.transaction(() -> created.getSchema().createDocumentType("Doc"));
    created.transaction(() -> created.newDocument("Doc").set("n", 1).save());
    assertThat(created.countType("Doc", true)).isEqualTo(1L);
  }
}
