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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7141.
 * <p>
 * Two of the three defects the ticket reports are behavioural and are covered here: {@code ALTER DATABASE}
 * used to throw {@code "Error on saving database configuration"} with the {@link IOException} neither chained
 * nor logged, so a read-only filesystem, a permission problem and a full volume were indistinguishable; and
 * the HA single-bucket warning advised {@code CREATE VERTEX TYPE <name> BUCKETS 16} on types that by
 * definition already exist, which cannot work. The third (two log messages printing {@code null} where the
 * offending value belonged) is guarded by {@code LogMessageArityTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7141DiagnosticsTest extends TestHelper {

  /**
   * The reason the configuration could not be written must survive as the cause and the message must name
   * both the setting and the database, so the operator can tell the failure modes apart.
   */
  @Test
  void alterDatabaseReportsWhySavingTheConfigurationFailed() {
    final File configuration = new File(((DatabaseInternal) database).getDatabasePath() + File.separator
        + "configuration.json");

    // Turning the configuration file into a directory makes the FileOutputStream behind saveConfiguration()
    // fail with an IOException on every platform and for every user, root included.
    assertThat(configuration.delete() || !configuration.exists()).isTrue();
    assertThat(configuration.mkdir()).isTrue();
    try {
      assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.asyncWorkerThreads` 3"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("arcadedb.asyncWorkerThreads")
          .hasMessageContaining(database.getName())
          .hasCauseInstanceOf(IOException.class);
    } finally {
      assertThat(configuration.delete()).isTrue();
    }
  }

  /**
   * The advice the HA plugin now gives has to work on a type that already exists - which every type it warns
   * about does, since it warns while wrapping an existing database for HA.
   */
  @Test
  void theBucketAdviceGivenForSingleBucketTypesActuallyRuns() {
    database.command("sql", "create vertex type Contended buckets 1");
    assertThat(database.getSchema().getType("Contended").getBuckets(false)).hasSize(1);

    // What the warning used to advise: impossible, the type already exists.
    assertThatThrownBy(() -> database.command("sql", "create vertex type Contended buckets 16"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("already exists");

    // What it advises now.
    database.command("sql", "alter type Contended bucket +Contended_1");
    database.command("sql", "alter type Contended bucket +Contended_2");
    database.command("sql", "alter type Contended bucketselectionstrategy `thread`");

    assertThat(database.getSchema().getType("Contended").getBuckets(false)).hasSize(3);
    assertThat(database.getSchema().getType("Contended").getBucketSelectionStrategy().getName()).isEqualTo("thread");
  }
}
