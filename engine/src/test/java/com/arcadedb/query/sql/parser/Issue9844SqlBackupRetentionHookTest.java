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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BackupDirectoryResolver;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The engine half of issue #9844: {@code BACKUP DATABASE} hands the retention of the directory it wrote into to the
 * server through the {@link BackupDirectoryResolver} the server binds to the database, and only AFTER a backup that
 * succeeded. The server half - that the real resolver prunes, and that an unscheduled database stops growing its archive
 * directory - is {@code Issue9844SqlBackupAppliesRetentionIT}.
 * <p>
 * The statement cannot complete here: the integration module is not on the engine's test classpath, so it fails at its
 * reflective boundary with "libs not found in classpath". That is what makes the failure case sharp - a backup that
 * did not happen must prune nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9844SqlBackupRetentionHookTest extends TestHelper {

  private static class RecordingResolver implements BackupDirectoryResolver {
    private final List<String> retentionCalls = new ArrayList<>();

    @Override
    public String resolveBackupDirectory() {
      return "./target/backups-9844";
    }

    @Override
    public int applyRetention(final String backupDirectory, final String databaseName) {
      retentionCalls.add(backupDirectory + "|" + databaseName);
      return 0;
    }
  }

  private final RecordingResolver resolver = new RecordingResolver();

  @AfterEach
  void unbindResolver() {
    ((DatabaseInternal) database).setWrapper(BackupDirectoryResolver.WRAPPER_NAME, null);
  }

  @Test
  void aBackupThatFailedPrunesNothing() {
    ((DatabaseInternal) database).setWrapper(BackupDirectoryResolver.WRAPPER_NAME, resolver);

    assertThatThrownBy(() -> database.command("sql", "BACKUP DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("backup libs not found in classpath");

    assertThat(resolver.retentionCalls).as("retention follows a backup that exists, never one that failed").isEmpty();
  }

  @Test
  void aResolverThatOnlyResolvesPrunesNothingByDefault() {
    final BackupDirectoryResolver onlyResolves = () -> "./target/backups-9844";

    assertThat(onlyResolves.applyRetention("./target/backups-9844", "any")).isZero();
  }

  @Test
  void applyingRetentionForADatabaseWithNoResolverIsANoOp() {
    // an embedded process with no server in it: nothing is bound, nothing is pruned, nothing throws
    BackupDirectoryResolver.applyRetentionAfterBackup(database, "./target/backups-9844");
  }

  @Test
  void applyingRetentionNamesTheDirectoryAndTheDatabase() {
    ((DatabaseInternal) database).setWrapper(BackupDirectoryResolver.WRAPPER_NAME, resolver);

    BackupDirectoryResolver.applyRetentionAfterBackup(database, "./target/backups-9844");

    assertThat(resolver.retentionCalls).containsExactly("./target/backups-9844|" + database.getName());
  }

  @Test
  void aResolverThatFailsToPruneNeverFailsTheCaller() {
    ((DatabaseInternal) database).setWrapper(BackupDirectoryResolver.WRAPPER_NAME, new BackupDirectoryResolver() {
      @Override
      public String resolveBackupDirectory() {
        return "./target/backups-9844";
      }

      @Override
      public int applyRetention(final String backupDirectory, final String databaseName) {
        throw new IllegalStateException("the directory is unreadable");
      }
    });

    // the archive is written and the caller is being told so: a directory that could not be pruned is an operational problem
    BackupDirectoryResolver.applyRetentionAfterBackup(database, "./target/backups-9844");
  }
}
