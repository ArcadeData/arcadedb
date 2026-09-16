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
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7645: unlike {@code BACKUP DATABASE}, {@code EXPORT DATABASE} registered no {@link com.arcadedb.engine.OperationProgress}
 * around its run, so a running export was invisible to the progress endpoint, the console and Studio - the operator
 * refused by a concurrent {@code restore database} (issue #7450 gave the export {@link MaintenanceCoordinator.Operation#EXPORT})
 * had no way to see the export that was refusing them, how far along it was, or when it would let go.
 * <p>
 * Mirrors {@link Issue7443SqlMaintenanceSlotTest}'s assertion for {@code BACKUP DATABASE} and {@code IMPORT DATABASE}:
 * the registry is empty once the statement returns, on the failure path as on the success one, because the statement
 * always fails at the reflective boundary in this module (the integration module is not on the engine's test
 * classpath) - which makes this a sharp test of the RELEASE, exactly as {@code Issue7450SqlExportMaintenanceSlotTest}
 * is for the maintenance slot itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7645SqlExportProgressTest extends TestHelper {

  @Test
  void aFailedSqlExportPublishesNoLingeringProgress() {
    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  @Test
  void aFailedSqlExportWithAnExplicitTargetPublishesNoLingeringProgress() {
    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://export-7645.jsonl.tgz"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  /**
   * A statement refused by the maintenance slot never reaches the progress registration at all - it must not
   * publish an operation it never ran, exactly as {@code BackupDatabaseStatement} does not.
   */
  @Test
  void anExportRefusedByTheSlotPublishesNoProgress() {
    final MaintenanceCoordinator alwaysRestoreInProgress = new MaintenanceCoordinator() {
      @Override
      public Operation begin(final String databaseName, final Operation operation) {
        return Operation.RESTORE;
      }

      @Override
      public void end(final String databaseName, final Operation operation) {
        throw new AssertionError("a refused reservation must never be released");
      }
    };
    ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, alwaysRestoreInProgress);
    try {
      assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
          .isInstanceOf(DatabaseOperationInProgressException.class);

      assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
    } finally {
      ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, null);
    }
  }

  /**
   * A statement rejected by its own validation - before the reservation and before the progress registration - must
   * not have published anything either.
   */
  @Test
  void anExportRejectedByItsOwnValidationPublishesNoProgress() {
    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://..escape-7645.jsonl.tgz"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot contain path change");

    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }
}
