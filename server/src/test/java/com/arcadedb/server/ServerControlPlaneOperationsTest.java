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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MaintenanceCoordinator.Operation;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Operator commands of {@code ServerControlPlane} at the layer every transport shares: the exclusive-operation slot of
 * {@code close database} and {@code drop database}, {@code list backups}, and the profiler start response.
 */
class ServerControlPlaneOperationsTest {
  // Regression: #7469 close database takes the per-database exclusive-operation slot
  /**
   * Issue #7469, the residue left over when {@code create database} and {@code drop database} were enrolled in the
   * per-database exclusive-operation slot (#7441 and #7641). {@code ServerControlPlane.closeDatabase} took neither the
   * slot nor the restore name claim, so it could close the open instance out from under a backup, an export or an
   * import of the same database - every one of which reads or writes THROUGH that instance. The archive is half
   * written and the operation that was writing it loses its database, with nothing said to either caller: the same
   * silent failure the drop used to be, by another route.
   * <p>
   * Fixed by adding {@link Operation#CLOSE} to the enum and taking the slot in {@code closeDatabase}. It conflicts
   * with everything, as {@link Operation#DROP} does.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CloseDatabaseSlot {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

    private static final String DB_NAME = "close7469db";

    @Test
    void closeDatabaseIsRefusedWhileABackupIsRunning() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.returns("getBackupCoordinator", coordinator);

      assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
      try {
        final ServerControlPlane controlPlane = new ServerControlPlane(server);

        assertThatThrownBy(() -> controlPlane.closeDatabase(DB_NAME))
            .as("closing the instance a backup is streaming from must not be admitted")
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessageContaining(DB_NAME);

        // NOT EVEN LOOKED UP: THE REFUSAL HAPPENS BEFORE ANYTHING IS TOUCHED
        assertThat(server.calls("getDatabaseForOperator")).doesNotContain(List.of(DB_NAME));
        assertThat(server.calls("removeDatabase")).doesNotContain(List.of(DB_NAME));
      } finally {
        coordinator.end(DB_NAME, Operation.BACKUP);
      }
    }

    @Test
    void closeDatabaseIsRefusedWhileAnExportOrAnImportIsRunning() {
      for (final Operation running : new Operation[] { Operation.EXPORT, Operation.IMPORT, Operation.RESTORE }) {
        final BackupCoordinator coordinator = new BackupCoordinator();
        final FakeArcadeDBServer server = FakeArcadeDBServer.create();
        server.returns("getBackupCoordinator", coordinator);

        assertThat(coordinator.begin(DB_NAME, running)).isNull();
        try {
          assertThatThrownBy(() -> new ServerControlPlane(server).closeDatabase(DB_NAME))
              .as("a close must be refused while %s of the same database is running", running.phrase())
              .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
        } finally {
          coordinator.end(DB_NAME, running);
        }
      }
    }

    /**
     * The other direction, which is the half that matters for the backup: a backup started while a close of the same
     * database is in flight must be refused too, or the slot would only be a one-way check.
     */
    @Test
    void anOperationIsRefusedWhileACloseHoldsTheSlot() {
      final BackupCoordinator coordinator = new BackupCoordinator();

      assertThat(coordinator.begin(DB_NAME, Operation.CLOSE)).isNull();
      assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isEqualTo(Operation.CLOSE);
      assertThat(coordinator.begin(DB_NAME, Operation.EXPORT)).isEqualTo(Operation.CLOSE);
      assertThat(coordinator.begin(DB_NAME, Operation.CLOSE)).isEqualTo(Operation.CLOSE);

      coordinator.end(DB_NAME, Operation.CLOSE);
      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }

    /** The success path: the close proceeds and hands the slot back, so the database is genuinely free afterwards. */
    @Test
    void closeDatabaseSucceedsAndReleasesItsSlotAfterwards() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      final ServerDatabase database = SERVED.open(DB_NAME);

      server.returns("getBackupCoordinator", coordinator);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);

      new ServerControlPlane(server).closeDatabase(DB_NAME);

      assertThat(database.isOpen()).as("the close reached the database").isFalse();
      assertThat(server.calls("removeDatabase")).containsOnlyOnce(List.of(DB_NAME));

      assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
      coordinator.end(DB_NAME, Operation.BACKUP);
      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }

    /**
     * A close that throws - most commonly because no database by that name is open - must still release its slot, or
     * one failed admin command would block every later backup, restore, import and export of that database until the
     * server restarts.
     */
    @Test
    void aFailedCloseStillReleasesItsSlot() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      server.returns("getBackupCoordinator", coordinator);
      server.on("getDatabaseForOperator", args -> {
        if (Objects.equals(args[0], DB_NAME))
          throw new IllegalArgumentException("Database '" + DB_NAME + "' not found");
        return null;
      });

      assertThatThrownBy(() -> new ServerControlPlane(server).closeDatabase(DB_NAME))
          .isInstanceOf(IllegalArgumentException.class);

      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }
  }

  // Regression: #7641 drop database takes the per-database exclusive-operation slot
  /**
   * Issue #7641: {@code drop database} used to delete a whole database directory without consulting
   * {@link BackupCoordinator} at all - the one whole-database delete left unslotted after issue #7384 gave a
   * restore the same per-database reservation. A {@code trigger backup}, {@code restore database} or
   * {@code import database} running on the same database was no obstacle: the drop deleted the directory out from
   * under whichever of those was reading or writing it.
   * <p>
   * The fix gives {@code dropDatabase} the slot as the new {@link Operation#DROP}, which - like {@link Operation#RESTORE}
   * - conflicts with every other kind, because a drop destroys the directory unconditionally and with no replacement.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DropDatabaseSlot {
    @RegisterExtension
    static final ServedDatabases SERVED = new ServedDatabases();

    private static final String DB_NAME = "drop7641db";

    @Test
    void dropDatabaseIsRefusedWhileABackupIsRunning() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
      server.returns("getBackupCoordinator", coordinator);

      assertThat(coordinator.begin(DB_NAME, Operation.BACKUP)).isNull();
      try {
        final ServerControlPlane controlPlane = new ServerControlPlane(server);

        assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
            .as("a drop must not be admitted while a backup of the same database is running")
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class)
            .hasMessageContaining(DB_NAME);

        // NOTHING WAS TOUCHED: THE DATABASE WAS NEVER EVEN LOOKED UP TO BE DROPPED
        assertThat(server.calls("getDatabaseForOperator")).doesNotContain(List.of(DB_NAME));
        assertThat(server.calls("removeDatabase")).doesNotContain(List.of(DB_NAME));
      } finally {
        coordinator.end(DB_NAME, Operation.BACKUP);
      }
    }

    @Test
    void dropDatabaseIsRefusedWhileARestoreIsRunning() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
      server.returns("getBackupCoordinator", coordinator);

      assertThat(coordinator.begin(DB_NAME, Operation.RESTORE)).isNull();
      try {
        final ServerControlPlane controlPlane = new ServerControlPlane(server);

        assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class);

        assertThat(server.calls("getDatabaseForOperator")).doesNotContain(List.of(DB_NAME));
      } finally {
        coordinator.end(DB_NAME, Operation.RESTORE);
      }
    }

    @Test
    void dropDatabaseIsRefusedWhileAnExportIsRunning() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
      server.returns("getBackupCoordinator", coordinator);

      // EXPORT IS THE ONE KIND ADMITTED WITHOUT LIMIT (issue #7450) - IT MUST STILL REFUSE A DROP
      assertThat(coordinator.begin(DB_NAME, Operation.EXPORT)).isNull();
      try {
        final ServerControlPlane controlPlane = new ServerControlPlane(server);

        assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME))
            .isInstanceOf(ServerControlPlane.OperationInProgressException.class);
      } finally {
        coordinator.end(DB_NAME, Operation.EXPORT);
      }
    }

    /**
     * A drop conflicts with everything, so it is also refused when nothing but ANOTHER drop of the same database is
     * mid-flight - two concurrent drops racing each other are exactly the same hazard as a restore racing a drop.
     */
    @Test
    void twoConcurrentDropsOfOneDatabaseExcludeEachOther() {
      final BackupCoordinator coordinator = new BackupCoordinator();

      assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
      assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isEqualTo(Operation.DROP);

      coordinator.end(DB_NAME, Operation.DROP);
      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }

    /**
     * The success path: nothing else is running, so the drop proceeds and releases its own reservation afterwards -
     * the database is genuinely free again, not merely reported free.
     */
    @Test
    void dropDatabaseSucceedsAndReleasesItsSlotAfterwards() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      // A real database, NOT AN HAReplicatedDatabase, SO dropDatabaseClusterWide TAKES THE NON-HA, LOCAL-DELETE BRANCH
      final ServerDatabase database = SERVED.open(DB_NAME);
      final File directory = new File(database.getDatabasePath());

      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
      server.returns("getBackupCoordinator", coordinator);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);

      final ServerControlPlane controlPlane = new ServerControlPlane(server);
      controlPlane.dropDatabase(DB_NAME);

      assertThat(database.isOpen()).isFalse();
      assertThat(directory).as("the drop deleted the database's files").doesNotExist();
      assertThat(server.calls("removeDatabase")).containsOnlyOnce(List.of(DB_NAME));

      // THE SLOT WAS RELEASED: A FRESH RESERVATION OF ANY KIND IS ADMITTED RIGHT AFTER
      assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
      coordinator.end(DB_NAME, Operation.DROP);
      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }

    /**
     * A drop that throws while dropping must still release its slot - the failure is the caller's to see, not a
     * reservation leaked for the life of the server.
     */
    @Test
    void aFailedDropStillReleasesItsSlot() {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      // The drop itself is scripted on the embedded database; the server-side handle around it is the real one
      final DatabaseInternal embedded = mock(DatabaseInternal.class);
      when(embedded.getEmbedded()).thenReturn(embedded);
      final ServerDatabase database = new ServerDatabase(null, embedded);

      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME));
      server.returns("getBackupCoordinator", coordinator);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);
      doThrow(new RuntimeException("boom")).when(embedded).drop();

      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      assertThatThrownBy(() -> controlPlane.dropDatabase(DB_NAME)).hasMessageContaining("boom");

      assertThat(coordinator.begin(DB_NAME, Operation.DROP)).isNull();
      coordinator.end(DB_NAME, Operation.DROP);
    }

    /**
     * Two real threads calling {@code dropDatabase} on one database at the same instant: exactly one delete happens,
     * the loser is told why rather than silently re-dropping, and neither leaks the slot. Either refusal is correct -
     * the loser may arrive while the winner still holds the slot
     * ({@link ServerControlPlane.OperationInProgressException}) or after it released ({@link IllegalArgumentException},
     * "does not exist").
     * <p>
     * WHAT THIS DOES NOT PIN, so nobody later reads more into a green run than is there: it does not discriminate the
     * existence check being INSIDE the reservation rather than before it (review of PR #7649). It was checked against
     * both orderings and passes on both. The interleaving only the reordering rules out - the loser reads
     * {@code existsDatabase() == true}, the winner then takes the slot, deletes and releases, and the loser goes on to
     * take the freed slot and drop an already-dropped database through a stale {@link ServerDatabase} handle - needs
     * the loser suspended between its check and its {@code beginExclusive}, and that point exists only in the code the
     * fix removed. Reaching it from a test means latching on {@code server.getBackupCoordinator()} and giving the
     * fixed ordering a timed escape so it does not simply deadlock, which pins this method's internal call sequence
     * rather than its behaviour. The reordering is argued at the call site instead; this test guards the invariants
     * above, which is what a later change to the slot or the check would break.
     */
    @Test
    @Timeout(30)
    void twoConcurrentDropDatabaseCallsDeleteItExactlyOnce() throws Exception {
      final BackupCoordinator coordinator = new BackupCoordinator();
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      // The drop itself is scripted on the embedded database; the server-side handle around it is the real one
      final DatabaseInternal embedded = mock(DatabaseInternal.class);
      when(embedded.getEmbedded()).thenReturn(embedded);
      final ServerDatabase database = new ServerDatabase(null, embedded);

      // THE DATABASE STOPS EXISTING THE MOMENT IT IS DROPPED, WHICH IS WHAT THE LOSER MUST BE ABLE TO OBSERVE
      final AtomicBoolean exists = new AtomicBoolean(true);
      server.on("existsDatabase", args -> Objects.equals(args[0], DB_NAME) ? exists.get() : false);
      server.returns("getBackupCoordinator", coordinator);
      server.on("getDatabaseForOperator", args -> Objects.equals(args[0], DB_NAME) ? database : null);
      doAnswer(invocation -> {
        assertThat(exists.getAndSet(false)).as("the directory was deleted twice").isTrue();
        return null;
      }).when(embedded).drop();

      final ServerControlPlane controlPlane = new ServerControlPlane(server);

      final CyclicBarrier startTogether = new CyclicBarrier(2);
      final List<Throwable> failures = new ArrayList<>();
      final List<Thread> droppers = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        final Thread dropper = new Thread(() -> {
          try {
            startTogether.await();
            controlPlane.dropDatabase(DB_NAME);
          } catch (final Throwable t) {
            synchronized (failures) {
              failures.add(t);
            }
          }
        }, "drop-racer-7641-" + i);
        droppers.add(dropper);
        dropper.start();
      }
      for (final Thread dropper : droppers)
        dropper.join(20_000);

      // EXACTLY ONE DELETE, AND THE OTHER CALLER WAS TOLD WHY RATHER THAN SILENTLY RE-DROPPING
      verify(embedded, times(1)).drop();
      assertThat(server.calls("removeDatabase")).containsOnlyOnce(List.of(DB_NAME));
      assertThat(failures).hasSize(1);
      assertThat(failures.get(0)).isInstanceOfAny(ServerControlPlane.OperationInProgressException.class,
          IllegalArgumentException.class);

      // AND NO SLOT WAS LEAKED BY EITHER OF THEM
      assertThat(coordinator.isInProgress(DB_NAME)).isFalse();
    }
  }

  // Regression: #10880 list backups published an unreadable archive as a zero-byte backup dated 1970
  /**
   * Issue #10880: {@code list backups} published an archive it could not stat as {@code "size":0,"lastModified":0}, a zero-byte
   * backup dated 1970, and logged nothing. The common cause is the retention sweep deleting the archive under the listing, which a
   * dangling symbolic link reproduces deterministically: the name is listed, the stat fails with {@code NoSuchFileException}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ListBackupsUnreadableArchive extends BaseGraphServerTest {
    private Path                     backupDir;
    // the permissions the shared backup root had before the test, given back as they were
    private Set<PosixFilePermission> rootPermissions;

    @BeforeEach
    void prepareBackupDirectory() throws IOException {
      backupDir = new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY),
          getDatabaseName()).toPath();
      FileUtils.deleteRecursively(backupDir.toFile());
      Files.createDirectories(backupDir);
      if (isPosix())
        rootPermissions = Files.getPosixFilePermissions(backupDir.getParent());
    }

    @AfterEach
    void removeBackupDirectory() throws IOException {
      if (backupDir != null) {
        // the backup root is shared with every other backup test of the module: give it back the access it had, and the
        // test's own directory the access its deletion needs, or what is left behind fails the next test
        if (rootPermissions != null) {
          Files.setPosixFilePermissions(backupDir.getParent(), rootPermissions);
          if (Files.exists(backupDir, LinkOption.NOFOLLOW_LINKS))
            Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("rwx------"));
        }
        FileUtils.deleteRecursively(backupDir.toFile());
        assertThat(backupDir).as("the test leaves nothing behind").doesNotExist();
      }
    }

    @Test
    void onlyRegularFilesAreListedWithTheNumbersReadFromThem() throws IOException {
      final Path real = writeArchive("120000");

      // A directory with an archive's name is not an archive anybody can restore or delete
      Files.createDirectories(backupDir.resolve(getDatabaseName() + "-backup-20261028-140000.zip"));

      final JSONObject listing = listBackups();
      assertOnlyListed(listing, real);
    }

    @Test
    void anArchiveRemovedUnderTheListingIsNotListedWithMadeUpNumbers() throws IOException {
      final Path real = writeArchive("120000");

      // What an archive removed by the retention sweep under the listing looks like to the stat: listed, then gone
      final Path removed = backupDir.resolve(getDatabaseName() + "-backup-20261028-130000.zip");
      boolean linked;
      try {
        Files.createSymbolicLink(removed, backupDir.resolve("does-not-exist.zip"));
        linked = true;
      } catch (final UnsupportedOperationException | IOException | SecurityException e) {
        // Windows without the symbolic link privilege: the case cannot be built here
        linked = false;
      }
      assumeThat(linked).as("symbolic links can be created").isTrue();

      try {
        assertOnlyListed(listBackups(), real);
      } finally {
        // a recursive delete that asks File.exists() never sees a dangling link
        Files.deleteIfExists(removed);
      }
    }

    @Test
    void aDatabaseWithNoBackupDirectoryHasNoBackups() {
      FileUtils.deleteRecursively(backupDir.toFile());

      final JSONObject listing = listBackups();
      assertThat(listing.getJSONArray("backups").length()).isZero();
      assertThat(listing.getLong("totalCount")).isZero();
      assertThat(listing.getLong("totalSize")).isZero();
    }

    @Test
    void aDirectoryThatCannotBeListedIsATypedCommandFailure() throws IOException {
      writeArchive("120000");
      assumePosix();
      Files.setPosixFilePermissions(backupDir, PosixFilePermissions.fromString("---------"));
      // A superuser lists it anyway: nothing to test there
      assumeThat(Files.isReadable(backupDir)).isFalse();

      assertListingFails();
    }

    /** Files.isDirectory() answers false for a directory the server cannot reach, which used to read as "no backups". */
    @Test
    void aDirectoryThatCannotBeReachedIsATypedCommandFailureNotAnEmptyList() throws IOException {
      writeArchive("120000");
      assumePosix();
      Files.setPosixFilePermissions(backupDir.getParent(), PosixFilePermissions.fromString("---------"));
      assumeThat(Files.isDirectory(backupDir)).as("the directory is out of reach").isFalse();

      assertListingFails();
    }

    private Path writeArchive(final String time) throws IOException {
      final Path archive = backupDir.resolve(getDatabaseName() + "-backup-20261028-" + time + ".zip");
      Files.write(archive, new byte[4096]);
      return archive;
    }

    private JSONObject listBackups() {
      return new ServerControlPlane(getServer(0)).listBackups(getDatabaseName());
    }

    private void assertOnlyListed(final JSONObject listing, final Path archive) throws IOException {
      final JSONArray backups = listing.getJSONArray("backups");
      assertThat(backups.length()).as("only the archive whose attributes were read is listed").isEqualTo(1);
      final JSONObject backup = backups.getJSONObject(0);
      assertThat(backup.getString("fileName")).isEqualTo(archive.getFileName().toString());
      assertThat(backup.getLong("size")).isEqualTo(4096L);
      assertThat(backup.getLong("lastModified")).isEqualTo(Files.getLastModifiedTime(archive).toMillis());
      assertThat(backup.getString("timestamp")).isEqualTo("2026-10-28T12:00");

      assertThat(listing.getLong("totalCount")).isEqualTo(1L);
      assertThat(listing.getLong("totalSize")).isEqualTo(4096L);
    }

    private void assertListingFails() {
      assertThatThrownBy(this::listBackups)
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Error listing backups for database '" + getDatabaseName() + "'");
    }

    private void assumePosix() {
      assumeThat(isPosix()).as("POSIX permissions").isTrue();
    }

    private boolean isPosix() {
      return backupDir.getFileSystem().supportedFileAttributeViews().contains("posix");
    }
  }

  // Regression: #7394 profiler start reports the timeout that actually applies
  /**
   * Issue #7394 item 4: a recording started with no timeout was documented as open-ended and was not.
   * <p>
   * Three surfaces said that a {@code timeoutSeconds} of zero records until the profiler is stopped - this
   * class's javadoc, {@code ProfilerStartRequest.timeout_seconds} in the proto, and
   * {@code RemoteGrpcServer.profilerStart}. {@link ServerQueryProfiler#start(int)} does
   * {@code timeoutSec > 0 ? timeoutSec : DEFAULT_TIMEOUT_SECONDS}, so both spellings of "no timeout" get 60
   * seconds and an auto-stop timer, and a caller planning a long capture found its recording already stopped.
   * <p>
   * The bounded default stays - an unattended recording keeps every server query on the
   * {@code ProfilingResultSet} wrapping path, so "records forever unless someone remembers" is not the safer
   * behaviour - and is reported instead. The start response now carries the timeout that will actually apply,
   * so a caller can tell when its recording ends without having to know the default.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
   */
  @Nested
  class ProfilerStartTimeout {
    private FakeArcadeDBServer  server;
    private ServerQueryProfiler profiler;
    private ServerControlPlane  controlPlane;

    @BeforeEach
    void setup(@TempDir final Path rootPath) {
      // stop() persists the run under <rootPath>/profiler, so the server is rooted in the test's own directory
      server = FakeArcadeDBServer.create(rootPath, new ContextConfiguration());
      profiler = new ServerQueryProfiler(server);
      server.queryProfiler(profiler);
      controlPlane = new ServerControlPlane(server);
    }

    @AfterEach
    void teardown() {
      if (profiler != null && profiler.isRecording())
        profiler.stop();
    }

    @Test
    void aZeroTimeoutReportsTheDefaultItActuallyGets() {
      final JSONObject response = controlPlane.profilerStart(0);

      assertThat(response.getBoolean("recording")).isTrue();
      assertThat(response.getInt("timeoutSeconds")).isEqualTo(60);
      assertThat(profiler.getTimeoutSeconds()).isEqualTo(60);
    }

    @Test
    void aNegativeTimeoutReportsTheSameDefault() {
      final JSONObject response = controlPlane.profilerStart(-1);

      assertThat(response.getInt("timeoutSeconds")).isEqualTo(60);
    }

    @Test
    void anExplicitTimeoutIsReportedBackUnchanged() {
      final JSONObject response = controlPlane.profilerStart(300);

      assertThat(response.getInt("timeoutSeconds")).isEqualTo(300);
      assertThat(profiler.getTimeoutSeconds()).isEqualTo(300);
    }

    /**
     * Starting an already-recording profiler is a no-op, so the reported timeout has to be the one the live
     * recording is running under - not the one this call asked for and did not get.
     */
    @Test
    void aSecondStartReportsTheTimeoutOfTheRecordingThatIsActuallyRunning() {
      controlPlane.profilerStart(300);

      final JSONObject response = controlPlane.profilerStart(5);

      assertThat(response.getInt("timeoutSeconds")).isEqualTo(300);
      assertThat(profiler.getTimeoutSeconds()).isEqualTo(300);
    }
  }
}
