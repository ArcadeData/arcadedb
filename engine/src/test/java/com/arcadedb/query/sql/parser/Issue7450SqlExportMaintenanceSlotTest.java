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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code EXPORT DATABASE} takes the per-database maintenance slot when one is bound to the database, and releases
 * it on every way out (issue #7450).
 * <p>
 * It is the third statement of the shape #7443 gave {@code BACKUP DATABASE} and {@code IMPORT DATABASE}, and it was
 * left out of that one because it is the first holder that does NOT exclude a second of its own kind: an export
 * reads the whole database off disk and writes an archive exactly as a backup does, but two exports of one database
 * write two different files and have no reason to refuse each other. What they must not run through is a
 * {@code restore database}, which drops and replaces the directory the export is reading.
 * <p>
 * This is the engine half, and it pins the statement's side of the contract with a coordinator bound by hand: that
 * it reserves {@link Operation#EXPORT} before it starts and releases it afterwards, that a refusal reaches the
 * caller as a {@link DatabaseOperationInProgressException} and stops the statement dead, and that a database with
 * NO coordinator - an embedded process with no server in it - is left exactly as it was. The admission policy
 * itself ({@code EXPORT} excludes only {@code RESTORE}, and two exports coexist) is
 * {@code Issue7450ExportAdmissionTest}, and the server binding is {@code Issue7450SqlExportMaintenanceSlotIT},
 * both in the server module.
 * <p>
 * The statement cannot complete here: the integration module is not on the engine's test classpath, so it fails at
 * its reflective boundary with "libs not found in classpath". That is what makes this a sharp test of the RELEASE -
 * the reservation has to survive a statement that throws.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7450SqlExportMaintenanceSlotTest extends TestHelper {

  /**
   * A real coordinator, not a mock: it records the calls in order so the test can assert the reserve/release pair,
   * and can be primed to refuse the way a busy {@code BackupCoordinator} does.
   */
  private static class RecordingCoordinator implements MaintenanceCoordinator {
    private final List<String> calls = new ArrayList<>();
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
  void aSqlExportReservesTheSlotAndReleasesItEvenWhenTheExportFails() {
    bindCoordinator();

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    assertThat(coordinator.calls).containsExactly(
        "begin:EXPORT:" + database.getName(),
        "end:EXPORT:" + database.getName());
  }

  /**
   * A refused statement stops before it starts: nothing is released, because the caller never took the slot, and
   * the verb in the message is the one a human would write rather than the enum constant.
   */
  @Test
  void aSqlExportIsRefusedWhileARestoreHoldsTheSlotAndOwesNoRelease() {
    bindCoordinator();
    coordinator.refuseWith = Operation.RESTORE;

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(DatabaseOperationInProgressException.class)
        .hasMessage("Cannot export database '" + database.getName() + "': a restore of it is already in progress");

    assertThat(coordinator.calls).containsExactly("begin:EXPORT:" + database.getName());
  }

  /**
   * The reservation is taken for the export's own kind, not borrowed from the backup's. A coordinator that refuses
   * everything still has to be ASKED for {@link Operation#EXPORT}, because that is the constant the admission
   * policy reasons about - asking for {@code BACKUP} here would make two exports refuse each other.
   */
  @Test
  void theSlotIsAskedForTheExportsOwnOperation() {
    bindCoordinator();
    coordinator.refuseWith = Operation.EXPORT;

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(DatabaseOperationInProgressException.class)
        .hasMessage("Cannot export database '" + database.getName() + "': an export of it is already in progress");

    assertThat(coordinator.calls).containsExactly("begin:EXPORT:" + database.getName());
  }

  /**
   * An export named explicitly reserves the same slot as the default-named one: the reservation is per database,
   * not per target file, so the URL must not change whether it is taken.
   */
  @Test
  void anExportWithAnExplicitTargetReservesTheSameSlot() {
    bindCoordinator();

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://export-7450.jsonl.tgz"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    assertThat(coordinator.calls).containsExactly(
        "begin:EXPORT:" + database.getName(),
        "end:EXPORT:" + database.getName());
  }

  /**
   * A statement rejected before it reaches the exporter must not have reserved anything: a path-changing target is
   * refused by the statement's own validation, and a reservation taken before that check would leak on every
   * malformed export.
   */
  @Test
  void anExportRejectedByItsOwnValidationReservesNothing() {
    bindCoordinator();

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE file://..escape-7450.jsonl.tgz"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot contain path change");

    assertThat(coordinator.calls).isEmpty();
  }

  /**
   * The admission policy's justification for admitting two exports at once is that they name two different targets.
   * That only holds if a default-named export resolves a NEW name on every run, and it did not: the parsed
   * statement is held in {@link com.arcadedb.query.sql.parser.StatementCache} and the same instance is handed to
   * every later execution of the same text, so writing the resolved default back into {@code url} froze the first
   * run's timestamp for the life of the database - and two concurrent executions raced on that one field.
   * <p>
   * Asserting on the cached instance rather than on two exports' file names is what makes this independent of the
   * integration module, which is not on the engine's test classpath. {@code Issue7450SqlExportMaintenanceSlotIT}
   * asserts the visible consequence - two successive default-named exports produce two different archives.
   */
  @Test
  void executingADefaultNamedExportDoesNotMutateTheCachedStatement() {
    bindCoordinator();
    final DatabaseInternal internal = (DatabaseInternal) database;

    final Statement cached = internal.getStatementCache().get("EXPORT DATABASE");
    assertThat(cached).isInstanceOf(ExportDatabaseStatement.class);
    assertThat(((ExportDatabaseStatement) cached).url).isNull();

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    // THE SAME INSTANCE EVERY LATER EXECUTION OF THIS TEXT WILL GET, STILL CARRYING NO TARGET
    assertThat(internal.getStatementCache().get("EXPORT DATABASE")).isSameAs(cached);
    assertThat(((ExportDatabaseStatement) cached).url)
        .as("a resolved default name written back here would be reused by every later export")
        .isNull();
  }

  /**
   * An embedded process with no server in it has no coordinator bound, and the statement has to behave exactly as
   * it did before #7450: straight through to its reflective boundary, reserving nothing.
   */
  @Test
  void withNoCoordinatorBoundTheExportRunsExactlyAsBefore() {
    assertThat(MaintenanceCoordinator.boundTo(database)).isNull();

    assertThatThrownBy(() -> database.command("sql", "EXPORT DATABASE"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("exporter libs not found in classpath");

    assertThat(coordinator.calls).isEmpty();
  }
}
