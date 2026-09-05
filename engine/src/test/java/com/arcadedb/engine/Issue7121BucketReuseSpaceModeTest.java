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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7121: {@code arcadedb.bucketReuseSpaceMode} is declared {@code SCOPE.DATABASE} and {@code ALTER DATABASE}
 * persists it per database, but both {@link LocalBucket} constructors read the JVM-global
 * {@link GlobalConfiguration} value. An operator who tuned one database to {@code low} for write throughput saw no
 * change, and the persisted schema recorded a value the engine ignored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7121BucketReuseSpaceModeTest {
  private static final String DB_PATH = "./target/databases/Issue7121ReuseMode";

  @AfterEach
  void cleanUp() {
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void theDatabasesOwnValueWinsOverTheProcessWideOne() {
    // The process-wide default says HIGH; the database asks for LOW through the documented DATABASE scope
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    factory.getContextConfiguration().setValue(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE, "low");

    try (final Database db = factory.create()) {
      db.transaction(() -> db.getSchema().createDocumentType("Reuse"));

      assertThat(bucketOf(db).reuseSpaceModeName())
          .as("the bucket must honour the database's own setting, not the JVM-wide one")
          .isEqualTo("LOW");
    }

    // And again on the reopen path, which is the other constructor
    final DatabaseFactory reopen = new DatabaseFactory(DB_PATH);
    reopen.getContextConfiguration().setValue(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE, "low");
    try (final Database db = reopen.open()) {
      assertThat(bucketOf(db).reuseSpaceModeName())
          .as("the load constructor read the global value too")
          .isEqualTo("LOW");
    }

    assertThat(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString())
        .as("and the per-database value must not leak into the process-wide default")
        .isEqualTo("high");
  }

  /**
   * The scenario #7121 actually describes: an operator tunes ONE database with {@code ALTER DATABASE} and expects it
   * to survive a restart. That exercises the {@code configuration.json} round trip - {@code saveConfiguration()} on
   * the way out, {@code fromJSON()} on the way back in - rather than only the in-memory config object, and it is the
   * path on which the persisted schema used to record a value the engine then ignored.
   */
  @Test
  void aValuePersistedByAlterDatabaseSurvivesAReopen() {
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    try (final Database db = new DatabaseFactory(DB_PATH).create()) {
      db.transaction(() -> db.getSchema().createDocumentType("Reuse"));
      db.command("sql", "ALTER DATABASE `arcadedb.bucketReuseSpaceMode` 'low'");
    }

    // A plain factory: nothing but the persisted configuration can carry the value now
    try (final Database db = new DatabaseFactory(DB_PATH).open()) {
      assertThat(db.getConfiguration().getValueAsString(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE))
          .as("ALTER DATABASE must persist the setting into the database's own configuration")
          .isEqualTo("low");
      assertThat(bucketOf(db).reuseSpaceModeName())
          .as("and the reopened bucket must be built from it, not from the JVM-wide default")
          .isEqualTo("LOW");
    }

    assertThat(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString())
        .as("ALTER DATABASE tunes ONE database: it must not write through to the process-wide default")
        .isEqualTo("high");
  }

  private static LocalBucket bucketOf(final Database db) {
    return (LocalBucket) db.getSchema().getType("Reuse").getBuckets(false).getFirst();
  }
}
