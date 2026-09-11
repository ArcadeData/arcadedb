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
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code BACKUP DATABASE} and {@code IMPORT DATABASE} take the per-database maintenance slot when one is bound to
 * the database, and release it on every way out (issue #7443).
 * <p>
 * The statements are executed by the ENGINE, so before this they could not reach the server's admission policy at
 * all: a SQL backup ran unseen by a concurrent {@code restore database} about to drop the directory out from under
 * it, and a second SQL backup of the same database was not refused either.
 * <p>
 * This is the engine half, and it pins the statements' side of the contract with a coordinator bound by hand: that
 * they reserve before they start and release afterwards, that a refusal reaches the caller as a
 * {@link DatabaseOperationInProgressException} and stops the statement dead, and that a database with NO
 * coordinator - an embedded process with no server in it - is left exactly as it was. The other half, that the
 * server binds its real {@code BackupCoordinator} to every database it opens and that the refusal reaches an HTTP
 * client as a 409, is {@code Issue7443SqlMaintenanceSlotIT} in the server module.
 * <p>
 * Neither statement can complete here: the integration module is not on the engine's test classpath, so both fail
 * at their reflective boundary with "libs not found in classpath". That is what makes this a sharp test of the
 * RELEASE - the reservation has to survive a statement that throws.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7443SqlMaintenanceSlotTest extends TestHelper {

  /**
   * A real coordinator, not a mock: it records the calls in order so the test can assert the reserve/release pair,
   * and can be primed to refuse the way a busy {@code BackupCoordinator} does.
   */
  private static class RecordingCoordinator implements MaintenanceCoordinator {
    private final List<String> calls   = new ArrayList<>();
    private       Operation    refuseWith;

    @Override
    public Operation begin(final String databaseName, final Operation operation) {
      calls.add("begin:" + operation + ":" + databaseName);
      return refuseWith;
    }

    @Override
    public void end(final String databaseName, final Operation operation) {
      calls.add("end:" + operation + ":" + databaseName);
    }
  }

  private final RecordingCoordinator coordinator = new RecordingCoordinator();

  @AfterEach
  void unbindCoordinator() {
    ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, null);
  }

  private void bindCoordinator() {
    ((DatabaseInternal) database).setWrapper(MaintenanceCoordinator.WRAPPER_NAME, coordinator);
  }

  @Test
  void aSqlBackupReservesTheSlotAndReleasesItEvenWhenTheBackupFails() {
    bindCoordinator();

    assertThatThrownBy(() -> database.command("sql", "BACKUP DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("backup libs not found in classpath");

    assertThat(coordinator.calls).containsExactly(
        "begin:BACKUP:" + database.getName(),
        "end:BACKUP:" + database.getName());
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  @Test
  void aSqlImportReservesTheSlotAndReleasesItEvenWhenTheImportFails() {
    bindCoordinator();

    assertThatThrownBy(() -> database.command("sql", "IMPORT DATABASE file:///no/such/source-7443.jsonl"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("importer libs not found in classpath");

    assertThat(coordinator.calls).containsExactly(
        "begin:IMPORT:" + database.getName(),
        "end:IMPORT:" + database.getName());
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  /**
   * A refused statement stops before it starts: nothing is released - the caller never took the slot - and no
   * progress entry is published for an operation that never ran.
   */
  @Test
  void aSqlBackupIsRefusedWhileARestoreHoldsTheSlotAndOwesNoRelease() {
    bindCoordinator();
    coordinator.refuseWith = Operation.RESTORE;

    assertThatThrownBy(() -> database.command("sql", "BACKUP DATABASE"))
        .isInstanceOf(DatabaseOperationInProgressException.class)
        .hasMessage("Cannot back up database '" + database.getName() + "': a restore of it is already in progress");

    assertThat(coordinator.calls).containsExactly("begin:BACKUP:" + database.getName());
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  /** The same for an import, and the verb is the one a human would write rather than the enum constant. */
  @Test
  void aSqlImportIsRefusedWhileARestoreHoldsTheSlotAndOwesNoRelease() {
    bindCoordinator();
    coordinator.refuseWith = Operation.RESTORE;

    assertThatThrownBy(() -> database.command("sql", "IMPORT DATABASE file:///no/such/source-7443.jsonl"))
        .isInstanceOf(DatabaseOperationInProgressException.class)
        .hasMessage("Cannot import database '" + database.getName() + "': a restore of it is already in progress");

    assertThat(coordinator.calls).containsExactly("begin:IMPORT:" + database.getName());
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  /** A second backup is refused by the slot the same way a restore refuses one - #6753 on the SQL path. */
  @Test
  void aSqlBackupIsRefusedWhileAnotherBackupHoldsTheSlot() {
    bindCoordinator();
    coordinator.refuseWith = Operation.BACKUP;

    assertThatThrownBy(() -> database.command("sql", "BACKUP DATABASE"))
        .isInstanceOf(DatabaseOperationInProgressException.class)
        .hasMessage("Cannot back up database '" + database.getName() + "': a backup of it is already in progress");
  }

  /**
   * An embedded process with no server in it has no coordinator bound, and both statements have to behave exactly
   * as they did before #7443: straight through to their reflective boundary, reserving nothing.
   */
  @Test
  void withNoCoordinatorBoundBothStatementsRunExactlyAsBefore() {
    assertThat(MaintenanceCoordinator.boundTo(database)).isNull();

    assertThatThrownBy(() -> database.command("sql", "BACKUP DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("backup libs not found in classpath");
    assertThatThrownBy(() -> database.command("sql", "IMPORT DATABASE file:///no/such/source-7443.jsonl"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("importer libs not found in classpath");

    assertThat(coordinator.calls).isEmpty();
    assertThat(OperationProgressRegistry.instance().getOperations(database.getName())).isEmpty();
  }

  /** The lookup follows the wrapper chain, which is what lets a statement find it through whichever layer it holds. */
  @Test
  void theCoordinatorIsFoundThroughTheDatabaseTheStatementHolds() {
    bindCoordinator();
    assertThat(MaintenanceCoordinator.boundTo(database)).isSameAs(coordinator);
    assertThat(MaintenanceCoordinator.boundTo(((DatabaseInternal) database).getEmbedded())).isSameAs(coordinator);
  }
}
