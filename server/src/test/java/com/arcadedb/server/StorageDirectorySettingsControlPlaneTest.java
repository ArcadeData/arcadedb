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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code set database setting} server command (HTTP and gRPC) stores a database's directory settings under the same
 * rule {@code ALTER DATABASE} applies: inside the database's own directory, or inside the directory the server-level value
 * of the same setting names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StorageDirectorySettingsControlPlaneTest {
  @RegisterExtension
  final ServedDatabases served = new ServedDatabases();

  private static final String DB_NAME = "storagedirsdb";

  @TempDir
  Path tempDir;

  @AfterEach
  void resetSettings() {
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.reset();
    GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.reset();
  }

  @Test
  void aDirectoryOutsideTheServerDirectoryIsRefused() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    final ServerControlPlane controlPlane = controlPlane(database);
    final Path outside = tempDir.resolve("outside");

    for (final GlobalConfiguration setting : new GlobalConfiguration[] { GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH,
        GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH }) {
      assertThatThrownBy(() -> controlPlane.setDatabaseSetting(DB_NAME, setting.getKey(), outside.toString()))
          .as(setting.getKey()).isInstanceOf(SecurityException.class).hasMessageContaining(setting.getKey());
      assertThat(database.getConfiguration().hasValue(setting.getKey())).as(setting.getKey()).isFalse();
    }
    assertThat(outside).doesNotExist();
  }

  @Test
  void aSubDirectoryOfTheServerDirectoryIsAccepted() throws Exception {
    final ServerDatabase database = served.open(DB_NAME);
    final Path root = Files.createDirectories(tempDir.resolve("root"));
    GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.setValue(root.toString());

    final String tier = root.resolve("tier").toString();
    controlPlane(database).setDatabaseSetting(DB_NAME, GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH.getKey(), tier);

    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH)).isEqualTo(tier);
  }

  private static ServerControlPlane controlPlane(final ServerDatabase database) {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);
    return new ServerControlPlane(server);
  }
}
