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
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BACKUP, IMPORT and EXPORT DATABASE statements: maintenance slot, progress, default names and retention hook.
 * <p>
 * A container for the regression suites of the feature; each nested class keeps its own fixture.
 */
class SqlMaintenanceStatementTest {
  /**
   * Regression for #7443: BACKUP DATABASE and IMPORT DATABASE take the per-database maintenance slot when one is bound to the database,...
   * <p>
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
  @Nested
  class SqlMaintenanceSlot extends TestHelper {

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

  /**
   * Regression for #7450: EXPORT DATABASE takes the per-database maintenance slot when one is bound to the database, and releases it on...
   * <p>
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
  @Nested
  class SqlExportMaintenanceSlot extends TestHelper {

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
     * The premise `Operation.EXPORT` rests on, tested where it can actually be forced. Two exports of one database
     * are admitted together because they write two different files; a default name built from the clock alone breaks
     * that for two that start within the same millisecond, and the exporter formats check `file.exists()` and then
     * create, so a shared name means two writers of one archive rather than one clean refusal.
     * <p>
     * A thousand names in a tight loop is what makes this deterministic rather than a race the test hopes to lose:
     * the run is asserted to have produced at least one repeated timestamp, so the same-millisecond case was
     * genuinely exercised, and every name is still distinct.
     */
    @Test
    void aDefaultExportNameIsDistinctEvenWithinOneMillisecond() {
      final int names = 1000;
      final Set<String> distinct = new HashSet<>();
      final Set<String> timestamps = new HashSet<>();

      for (int i = 0; i < names; i++) {
        final String name = ExportDatabaseStatement.defaultTargetName("db7450", "jsonl");
        distinct.add(name);
        // '<db>-export-<yyyyMMdd-HHmmssSSS>-<random>.<format>.tgz' - the timestamp is the 3rd and 4th '-' segments
        final String[] parts = name.split("-");
        timestamps.add(parts[2] + "-" + parts[3]);
      }

      assertThat(timestamps)
          .as("the loop has to have produced two names in one millisecond, or it proves nothing")
          .hasSizeLessThan(names);
      assertThat(distinct).as("every default export name is its own archive").hasSize(names);
    }

    /** The timestamp stays first, so the archives of one database still sort chronologically. */
    @Test
    void aDefaultExportNameKeepsItsConvention() {
      final String name = ExportDatabaseStatement.defaultTargetName("db7450", "jsonl");

      assertThat(name).startsWith("db7450-export-").endsWith(".jsonl.tgz");
      assertThat(name).matches("db7450-export-\\d{8}-\\d{9}-[0-9a-f-]{36}\\.jsonl\\.tgz");
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

  /**
   * Regression for #7645: unlike BACKUP DATABASE, EXPORT DATABASE registered no com.arcadedb.engine.OperationProgress around its run,...
   * <p>
   * Issue #7645: unlike {@code BACKUP DATABASE}, {@code EXPORT DATABASE} registered no {@link com.arcadedb.engine.OperationProgress}
   * around its run, so a running export was invisible to the progress endpoint, the console and Studio - the operator
   * refused by a concurrent {@code restore database} (issue #7450 gave the export {@link MaintenanceCoordinator.Operation#EXPORT})
   * had no way to see the export that was refusing them, how far along it was, or when it would let go.
   * <p>
   * Mirrors {@link SqlMaintenanceStatementTest.SqlMaintenanceSlot}'s assertion for {@code BACKUP DATABASE} and {@code IMPORT DATABASE}:
   * the registry is empty once the statement returns, on the failure path as on the success one, because the statement
   * always fails at the reflective boundary in this module (the integration module is not on the engine's test
   * classpath) - which makes this a sharp test of the RELEASE, exactly as {@code SqlMaintenanceStatementTest.SqlExportMaintenanceSlot}
   * is for the maintenance slot itself.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlExportProgress extends TestHelper {

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

  /**
   * Regression for #8301: the default name of a SQL EXPORT DATABASE carries a timestamp that must not depend on the JVM's default...
   * <p>
   * Issue #8301: the default name of a SQL {@code EXPORT DATABASE} carries a timestamp that must not depend on the JVM's
   * default locale (calendar and digits), like every other generated file name.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExportDefaultTargetName {

    @Test
    void defaultTargetNameIsLocaleIndependent() {
      final Locale saved = Locale.getDefault();
      try {
        for (final String tag : new String[] { "th-TH", "ar-EG", "ja-JP-u-ca-japanese", "en-US" }) {
          Locale.setDefault(Locale.forLanguageTag(tag));
          final String name = ExportDatabaseStatement.defaultTargetName("db8301", "jsonl");
          assertThat(name).as("locale %s", tag)
              .matches("db8301-export-\\d{8}-\\d{9}-[0-9a-f-]{36}\\.jsonl\\.tgz")
              .startsWith("db8301-export-" + LocalDate.now().getYear());
        }
      } finally {
        Locale.setDefault(saved);
      }
    }
  }

  /**
   * Regression for #9844: The engine half of issue #9844:
   * <p>
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
  @Nested
  class SqlBackupRetentionHook extends TestHelper {

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
}
