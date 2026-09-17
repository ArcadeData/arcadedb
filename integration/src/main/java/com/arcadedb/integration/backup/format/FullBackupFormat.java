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
package com.arcadedb.integration.backup.format;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.timeseries.TimeSeriesCompactionPause;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.integration.backup.BackupException;
import com.arcadedb.integration.backup.BackupSettings;
import com.arcadedb.integration.backup.IoThrottler;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FullBackupFormat extends AbstractBackupFormat {
  /**
   * How long the backup waits to pause TimeSeries compaction before giving up. A tripwire, not a latency bound:
   * the compaction write lock is only ever held across one brief transaction, so a budget this size expiring
   * means something is wrong rather than merely slow. Expiring FAILS the backup - proceeding unpaused would risk
   * the one outcome worth failing over, an archive that restores with duplicated samples and reports success.
   */
  private static final long COMPACTION_PAUSE_TIMEOUT_MS = 60_000L;

  /**
   * Whether this attempt has written a non-empty {@code schema.json} entry, set by the two methods that append
   * entries ({@link #compressEntry} and {@link #compressFile}) and checked once by {@link #writeArchive} before
   * the central directory is written. See {@link #checkArchiveCarriesTheSchema()} for why the archive is asked
   * rather than the filesystem, and {@link #writeArchive} for why it is reset per attempt.
   */
  private boolean schemaArchived;

  private interface BackupCallback {
    void backup(BackupArchiveWriter archive) throws Exception;
  }

  public FullBackupFormat(final DatabaseInternal database, final BackupSettings settings, final ConsoleLogger logger) {
    super(database, settings, logger);
  }

  @Override
  public void backupDatabase() throws Exception {
    settings.validateSettings();

    String fileName;
    if (settings.file.startsWith("file://"))
      fileName = settings.file.substring("file://".length());
    else
      fileName = settings.file;

    if (settings.directory != null)
      fileName = settings.directory + File.separator + fileName;

    final File backupFile = new File(fileName);

    if (backupFile.getParentFile() != null) {
      // createDirectories, not mkdirs: it is idempotent and does not report "already there" as a failure, so a
      // concurrent backup creating the same directory first is not mistaken for one that could not be created
      try {
        Files.createDirectories(backupFile.getParentFile().toPath());
      } catch (final IOException e) {
        throw new BackupException("The backup file '%s' cannot be created".formatted(backupFile), e);
      }
    }

    if (database.isTransactionActive() && database.getTransaction().hasChanges())
      throw new BackupException("Transaction in progress found");

    final int compressionLevel = resolveSetting(settings.compressionLevel, GlobalConfiguration.BACKUP_COMPRESSION_LEVEL);
    final int compressionThreads = resolveThreads();
    final int maxMBPerSecond = resolveSetting(settings.maxMBPerSecond, GlobalConfiguration.BACKUP_MAX_MB_PER_SECOND);

    // LAST THING BEFORE THE WRITE, SO THAT A BACKUP REJECTED BY ONE OF THE CHECKS ABOVE LEAVES NO EMPTY ARCHIVE FOR
    // RETENTION TO COUNT AND FOR AN OPERATOR TO MISTAKE FOR A BACKUP
    claimBackupFile(backupFile);

    logger.logLine(0, "Executing full backup of database to '%s' (compression level %d, %s%s)...", backupFile,
        compressionLevel, compressionThreads > 0 ? compressionThreads + " threads" : "single threaded",
        maxMBPerSecond > 0 ? ", throttled at " + maxMBPerSecond + "MB/s" : "");

    final long beginTime = System.currentTimeMillis();
    final AtomicLong databaseOrigSize = new AtomicLong();

    // #6075: READ THE PAGE FILES THROUGH A POINT-IN-TIME SNAPSHOT INSTEAD OF FREEZING THEM. THE OLD PATH SUSPENDED
    // PAGE FLUSHING FOR THE WHOLE BACKUP, SO DIRTY PAGES PILED UP IN RAM UNTIL FLUSH_SUSPEND_MAX_DEFERRED_RAM AND
    // COMMITTING THREADS WERE THROTTLED - AND LSM/VECTOR INDEX COMPACTION WAS POSTPONED WITH THEM. IT IS KEPT AS A
    // FALLBACK, SELECTED BY CONFIGURATION OR AUTOMATICALLY WHEN THE SHADOW BREACHES ITS CAP
    boolean useSnapshot = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.PAGE_SNAPSHOT_ENABLED);

    while (true) {
      // PageManager.suspendFlushAndExecute RUNS ITS CALLBACK THROUGH CodeUtils.executeIgnoringExceptions, WHICH LOGS AND
      // SWALLOWS. WITHOUT CARRYING THE FAILURE OUT BY HAND, A BACKUP THAT DIED HALFWAY WOULD STILL GET ITS CENTRAL
      // DIRECTORY WRITTEN AND BE REPORTED AS SUCCESSFUL - A TRUNCATED ARCHIVE THAT LOOKS VALID IS THE WORST POSSIBLE
      // FAILURE MODE FOR A BACKUP
      final AtomicReference<Exception> failure = new AtomicReference<>();
      final boolean snapshotAttempt = useSnapshot;

      try (final TimeSeriesCompactionPause pause = pauseCompaction()) {
        writeArchive(backupFile, compressionLevel, compressionThreads, maxMBPerSecond, failure, archive -> {
          if (snapshotAttempt)
            // #6114: NO DATABASE READ LOCK. The lock this path used to hold for its WHOLE duration existed only to
            // keep the two configuration files consistent with the page files, by excluding the DDL that rewrites
            // them - so a backup blocked CREATE TYPE / DROP TYPE / CREATE INDEX for as long as it ran. The window
            // now carries those files itself, captured at t0 together with the page list (see
            // PageSnapshot.getConfigurationFiles), so there is nothing left for the lock to protect
            databaseOrigSize.set(backupFromSnapshot(archive, pause));
          else
            // THE FALLBACK STILL TAKES IT. Here the page image is the LIVE on-disk one, held still by the flush
            // suspension rather than by a point in time, so the configuration files have to be pinned by a lock the
            // way they always were - there is no t0 to capture them at
            database.executeInReadLock(() -> {
              // FORCE FLUSHING BEFORE THE BACKUP AND AVOID FLUSHING OF DATA PAGES TO DISK
              database.getPageManager().suspendFlushAndExecute(database, () -> {
                try {
                  databaseOrigSize.set(backupFromFrozenFiles(archive));
                } catch (final Exception e) {
                  failure.set(e);
                  throw e;
                }
              });
              return null;
            });
        });
        // NO SECOND CHECK OF failure HERE: writeArchive RETHROWS IT FROM INSIDE ITS OWN CALLBACK, WHICH IT HAS TO DO SO
        // THE STREAM-CLOSING THERE KNOWS THE BACKUP FAILED. A CHECK AT THIS POINT WOULD BE UNREACHABLE, AND UNREACHABLE
        // SAFETY NETS ONLY TEACH THE NEXT READER THAT THE THROW ABOVE MIGHT NOT HAPPEN
        break;
      } catch (final PageSnapshotException e) {
        if (!snapshotAttempt || e.getReason() == PageSnapshotException.Reason.CLOSING) {
          // A PARTIAL ARCHIVE MUST NOT SURVIVE: LEAVING ONE BEHIND INVITES A RESTORE FROM IT.
          // #7458: A WINDOW REFUSED BECAUSE THE DATABASE IS CLOSED, OR BECAUSE A CLOSE IS WAITING FOR THE WINDOWS
          // ALREADY OPEN, IS NOT THE TRANSIENT SHADOW PROBLEM THE RETRY BELOW EXISTS FOR - THE DATABASE IS GOING
          // AWAY, AND A FROZEN-FILES RETRY WOULD ONLY RACE ITS TEARDOWN. FAIL NOW, AND SAY WHY
          backupFile.delete();
          throw e;
        }
        // ON THE WAY TO A RETRY THE PARTIAL ARCHIVE IS KEPT, NOT DELETED: DELETING IT WOULD HAND THE PATH BACK TO
        // ANYBODY ELSE RACING FOR IT (SEE claimBackupFile), AND THE RETRY OPENS THE SAME FILE WITH TRUNCATE, SO THE
        // PARTIAL CONTENT IS GONE THE MOMENT THE SECOND ATTEMPT STARTS WRITING. A RETRY THAT FAILS IN TURN LANDS IN
        // ONE OF THE TWO BRANCHES THAT DO DELETE, SO A PARTIAL ARCHIVE STILL NEVER SURVIVES THE CALL
        //
        // THE WINDOW COULD NOT HOLD THE POINT IN TIME (THE SHADOW BREACHED ITS CAP, OR A PRE-IMAGE COULD NOT BE READ).
        // A STREAMED ARCHIVE CANNOT BE REPAIRED IN PLACE, SO THE WHOLE BACKUP RESTARTS ON THE PATH THAT ALWAYS
        // COMPLETES - AT THE COST OF THROTTLING WRITERS, WHICH IS STILL BETTER THAN NOT HAVING A BACKUP
        logger.logLine(0, "Point-in-time snapshot unusable (%s): retrying with page flushing suspended...", e.getMessage());
        useSnapshot = false;
      } catch (final Exception e) {
        backupFile.delete();
        throw e;
      }
    }

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;
    final long origSize = databaseOrigSize.get();
    final long databaseCompressedSize = backupFile.length();

    logger.logLine(0, "Full backup completed in %d seconds %s -> %s (%,d%% compressed)", elapsedInSecs,
        FileUtils.getSizeAsString(origSize), FileUtils.getSizeAsString(databaseCompressedSize),
        origSize > 0 ? (origSize - databaseCompressedSize) * 100 / origSize : 0);
  }

  /**
   * Archives the two configuration files and the TimeSeries sealed stores, plus every PAGE file as it stood at the
   * snapshot's t0 (issue #6075).
   * <p>
   * The configuration files come FROM THE WINDOW since issue #6114, as bytes captured inside the t0 barrier, rather
   * than being read off the filesystem after it. That is what removes the database read lock from this path: the
   * lock's only remaining job was to exclude the DDL that rewrites those two files while they were read, and a
   * configuration pinned at t0 needs no exclusion - it is captured in the same file-set-locked region that lists
   * the pages (see {@code PageManager.captureConfigurationFiles}, and #7457 for the one ordering window that
   * region does not cover and the read lock did not either). Files
   * created after t0 are absent from the snapshot by construction, which is correct: they did not exist at the
   * point in time being archived. Files DROPPED after t0 are still readable, because their physical deletion is
   * deferred until the window closes.
   * <p>
   * <b>The TimeSeries sealed stores ARE listed under the read lock, together with the window (issue #7705).</b>
   * This javadoc used to say they never were and did not need to be, which was the same reasoning issue #7671
   * corrected on the HA snapshot ship. The window cannot carry them - {@code TimeSeriesSealedStore} opens
   * {@code <base>.ts.sealed} with raw {@code FileChannel} I/O and never registers it with the {@code FileManager},
   * so it is in neither {@code getFiles()} nor the deferred-deletion protection a dropped page file gets - and the
   * compaction pause excludes a compaction, not schema DDL. So a {@code DROP TYPE} of a TIMESERIES type landing
   * between t0 and a LIVE listing produced an archive whose {@code schema.json}, captured at t0, declares the type
   * while its sealed segments are absent: the #6356 / #6839 "type whose sealed store fails to load" state, in a
   * backup rather than on a follower.
   * <p>
   * The lock is held for the barrier plus one {@code File.listFiles}, and released before a byte of the archive is
   * written, so the availability issue #6114 bought is untouched: DDL still runs alongside the backup. The
   * caller's compaction pause is still held until the listed stores have been READ, which is a different
   * guarantee (#7280) - the pairing with {@code schema.json} is what the lock buys, and a compaction landing
   * whole inside the span is what the pause buys.
   */
  private long backupFromSnapshot(final BackupArchiveWriter archive, final TimeSeriesCompactionPause pause)
      throws Exception {
    long origSize = 0L;
    // ONE READ-LOCKED FRAME AROUND BOTH HALVES OF THE CAPTURE, AND NOTHING ELSE (issue #7705, after #7671). A
    // window that never reaches the try-with-resources below is never closed by it, so a listing that throws
    // closes it here and suppresses a close failure rather than replacing the exception that says what went wrong.
    final SnapshotImage image = database.executeInReadLock(() -> {
      final PageSnapshot window = database.getPageManager().openSnapshot(database);
      try {
        return new SnapshotImage(window, listSealedStoresOrFail());
      } catch (final RuntimeException e) {
        try {
          window.close();
        } catch (final RuntimeException closeFailure) {
          e.addSuppressed(closeFailure);
        }
        throw e;
      }
    });

    try (final PageSnapshot snapshot = image.snapshot()) {
      for (final PageSnapshot.SnapshotConfigFile config : snapshot.getConfigurationFiles())
        origSize += compressEntry(archive, config.fileName(), config.lastModified(), config.newInputStream());
      origSize += compressSealedStores(archive, image.sealedFiles());
      // RELEASED HERE AND NOT AT THE END: THE SPAN THAT HAS TO EXCLUDE A COMPACTION ENDS WITH THE LAST SEALED
      // BYTE READ, BECAUSE THE PAGE IMAGE IS ALREADY FIXED AT THE WINDOW'S t0 NO MATTER WHEN ITS BYTES ARE
      // STREAMED. HOLDING IT FOR THE WHOLE BACKUP WOULD POSTPONE TIMESERIES COMPACTION FOR THE WHOLE BACKUP,
      // WHICH IS EXACTLY THE COST #6075 REMOVED FOR EVERY OTHER BACKGROUND JOB. addFile/addEntry READ THEIR
      // INPUT TO THE END BEFORE RETURNING - THE WORKER THREADS ONLY COMPRESS - SO NOTHING IS STILL READING A
      // SEALED FILE AT THIS POINT
      pause.close();

      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        origSize += compressEntry(archive, file.fileName(), file.lastModified(), snapshot.newInputStream(file.fileId()));

      // ANY PAGE READ ABOVE COULD HAVE BEEN THE ONE THAT BREACHED THE SHADOW CAP, AND A STREAM THAT ALREADY FAILED
      // WOULD HAVE THROWN - BUT RE-CHECKING HERE ALSO CATCHES A WINDOW INVALIDATED AFTER ITS LAST BYTE WAS READ,
      // WHICH WOULD OTHERWISE PRODUCE AN ARCHIVE NOBODY EVER VERIFIED
      snapshot.checkValid();

      logger.logLine(2, "- Snapshot at txId=%d shadowed %d page(s), %s (%s spilled to disk)", snapshot.getLastTxId(),
          snapshot.getShadowedPages(), FileUtils.getSizeAsString(snapshot.getShadowSizeInBytes()),
          FileUtils.getSizeAsString(snapshot.getShadowSpilledBytes()));
    }
    return origSize;
  }

  /** The historical path: the data files are frozen by suspending page flushing and copied straight off the disk. */
  private long backupFromFrozenFiles(final BackupArchiveWriter archive) throws IOException {
    long origSize = 0L;
    origSize += compressFile(archive, ((LocalDatabase) database.getEmbedded()).getConfigurationFile());
    final File schemaFile = ((LocalSchema) database.getSchema()).getConfigurationFile();
    origSize += compressFile(archive, schemaFile);
    // schema.prev.json, the copy LocalSchema.readConfiguration() falls back to when schema.json is missing,
    // zero-length or unparseable, and the second arm of DatabaseFactory.exists(). Archived so a restored database
    // keeps the corruption fallback the database it was copied from had, instead of having none until its first
    // schema save recreates one (issue #7637). BOTH paths carry it, or the archive's file set would depend on
    // which one happened to run. Absent is legitimate and compressFile skips it: a database whose schema has never
    // been re-saved has no previous copy - which is also why it is deliberately NOT part of what
    // checkArchiveCarriesTheSchema() requires.
    origSize += compressFile(archive, new File(schemaFile.getParentFile(), LocalSchema.SCHEMA_PREV_FILE_NAME));
    // THE CALLER'S PAUSE IS NOT RELEASED EARLY ON THIS PATH, UNLIKE THE SNAPSHOT ONE: HERE THE PAGE IMAGE IS THE
    // ON-DISK ONE THE FLUSH SUSPENSION IS FREEZING, SO IT IS ONLY FIXED FOR AS LONG AS THE SUSPENSION LASTS AND
    // THE PAUSE HAS TO SPAN THE WHOLE CALLBACK - WHICH THIS PATH ALREADY THROTTLES WRITERS FOR ANYWAY
    // The fallback lists live, under the read lock the callback already holds - which is the same "one
    // observation" guarantee the window path gets from its own frame (issue #7705). There is no t0 here: the page
    // image is the on-disk one the flush suspension is freezing, so the listing is coherent with it for as long as
    // both are held, and both are.
    origSize += compressSealedStores(archive, listSealedStoresOrFail());

    final Collection<ComponentFile> files = database.getFileManager().getFiles();

    for (final ComponentFile file : new ArrayList<>(files))
      if (file != null)
        origSize += compressFile(archive, file.getOSFile());

    return origSize;
  }

  /**
   * Archives the TimeSeries sealed stores, which neither backup path can reach through its own file
   * enumeration: {@code TimeSeriesSealedStore} opens {@code <base>.ts.sealed} with raw {@code FileChannel} I/O
   * and never registers it as a {@code ComponentFile}, so it is absent from {@code FileManager.getFiles()} and
   * from the page snapshot alike. Without this a restored backup returns the schema, the graph, the tag
   * dictionary and whatever samples had not been compacted yet, and reports success (issue #7280).
   * <p>
   * The restore side needs no counterpart: {@code FullRestoreFormat} extracts every archive entry by name into
   * the database directory, so a {@code .ts.sealed} entry lands where the reopened database looks for it.
   * <p>
   * {@code sealedFiles} is the set CAPTURED WITH the page image rather than one listed here, and a name in it that
   * cannot be read fails the backup - see the catch below for both, and {@link #backupFromSnapshot} for the frame
   * that captures them (issue #7705).
   */
  private long compressSealedStores(final BackupArchiveWriter archive, final List<File> sealedFiles)
      throws IOException {
    long origSize = 0L;
    for (final File sealedFile : sealedFiles)
      try {
        // NO exists() PRE-CHECK: the open inside compressFile IS the check, so there is no window between asking
        // and reading in which the file can still go away silently.
        origSize += compressFile(archive, sealedFile, false);
      } catch (final FileNotFoundException e) {
        // THE LISTING AND THE READ ARE NOT ONE OPERATION, AND A STORE THAT WENT AWAY BETWEEN THEM NOW FAILS THE
        // BACKUP (issue #7705). Neither a compaction nor retention nor downsampling can replace the file here -
        // the pause is held, and runSealedMaintenanceReplicated takes the same shard write lock a compaction does
        // (verified on PR #7474). What is left is a store that went away ENTIRELY: a DROP TYPE that landed after
        // the listing, or a repair of a type whose engine never loaded, which takes no shard lock (#7475).
        //
        // This used to be skipped with a log line, on the argument that an operator can always move a file and
        // that it is not worth failing a backup over. That argument does not survive the listing moving inside the
        // read lock. Every name in `sealedFiles` belonged to a type the schema.json IN THIS VERY ARCHIVE declares,
        // captured in the same frame; skipping one therefore produces an archive that contradicts its own schema -
        // the #6356 / #6839 state, arriving by construction rather than by corruption - and reports success, so
        // the operator learns of it from a restore rather than from the tool. The ship reached the same conclusion
        // for the same reason in #7671.
        //
        // The cost is one failed backup in a rare race, which is visible and repeatable: a re-run opens its window
        // AFTER the drop, so the second attempt is coherent. The alternative cost is a backup nobody can trust.
        // The partial archive does not survive either - backupDatabase deletes it on the way out.
        //
        // ONLY FileNotFoundException, and that is not a gap: it is what BOTH archive writers raise for a name they
        // cannot open, because each reaches the file through `new FileInputStream` (ZipStreamArchiveWriter.addFile,
        // ParallelZipArchiveWriter.addFile) - a missing file, a directory wearing the name, and a file another
        // process holds open on Windows all arrive here. Any OTHER IOException is a real I/O failure, and it
        // already fails the backup by propagating out of this method - just without the sentence below saying
        // which store it was (claude-review on PR #7746).
        throw new BackupException("TimeSeries sealed store '" + sealedFile.getName()
            + "' could not be read after being listed for this backup: the archive would declare its type without "
            + "its data (" + e.getMessage() + ")", e);
      }
    return origSize;
  }

  /**
   * The sealed-store listing both backup paths use, which refuses to answer "this database has none" for a
   * directory it could NOT read (issue #7705, the same refusal #7671 gave the ship).
   * <p>
   * {@link TimeSeriesSealedStore#listSealedFiles(File)} maps an unreadable directory to an EMPTY array, which is
   * the right answer for a caller that only wants to iterate whatever is there and the wrong one here: a listing
   * that failed - a permission change, a transient I/O error - would let the backup archive a {@code schema.json}
   * declaring a TIMESERIES type with no sealed-store entry beside it, and report success. Refusing costs one
   * re-run; answering "none" costs a restore that cannot open the type.
   */
  private List<File> listSealedStoresOrFail() {
    final File[] sealedFiles = TimeSeriesSealedStore.listSealedFilesOrNull(new File(database.getDatabasePath()));
    if (sealedFiles == null)
      throw new BackupException("Cannot list the directory of database '" + database.getName()
          + "' to archive its TimeSeries sealed stores: a backup taken on the assumption that there are none would "
          + "declare every TIMESERIES type without its data");
    return List.of(sealedFiles);
  }

  /**
   * The page window and the TimeSeries sealed-store listing as ONE observation (issue #7705).
   * <p>
   * They are one value because {@code schema.json} comes from the window's t0 barrier and a {@code .ts.sealed} set
   * listed at any later moment can disagree with it - see {@link #backupFromSnapshot} for what listing them
   * together buys and what it does not. {@code sealedFiles} is immutable and never {@code null}; it is empty for a
   * database with no TimeSeries type.
   */
  private record SnapshotImage(PageSnapshot snapshot, List<File> sealedFiles) {
  }

  /**
   * Holds TimeSeries compaction back while the sealed stores are paired with the page image. See
   * {@link TimeSeriesCompactionPause} for why a whole compaction landing inside that span - and only a whole one
   * - would restore as duplicated samples.
   * <p>
   * Called OUTSIDE {@code executeInReadLock} and outside the flush suspension, and both matter.
   * <p>
   * Outside the read lock - which the fallback takes for its whole callback and which the window path takes for
   * the capture alone since #7705, so this argument constrains both again - because compaction's own order is
   * compaction-write-lock first and database read lock second (Phase 0/4a/4c take the write lock and then commit,
   * and {@code LocalDatabase.commit()} runs under the
   * database READ lock). Taking them the other way round here would close a cycle with any waiting DDL: the
   * database lock is a {@code ReentrantReadWriteLock}, so a queued writer stops new readers from barging, which
   * would leave the compaction unable to commit, this backup unable to take the compaction lock it is waiting
   * for, and the DDL unable to take the write lock this backup's read lock is holding. Acquiring the pause first
   * puts both this backup and compaction in the same order and there is no cycle to close.
   * <p>
   * Outside the flush suspension, because a compaction already in its Phase 4c holds the compaction write lock
   * while it commits, and a commit throttled by a suspension this thread has already taken would never release
   * it. Taken first, this waits for that compaction and only then closes the door.
   */
  private TimeSeriesCompactionPause pauseCompaction() {
    return TimeSeriesCompactionPause.acquire(database, COMPACTION_PAUSE_TIMEOUT_MS);
  }

  private long compressEntry(final BackupArchiveWriter archive, final String name, final long lastModified,
      final InputStream input) throws IOException {
    final String entryName = archiveEntryName(name);
    if (!entryName.equals(name))
      // A DIRECTORY-BEARING NAME HERE IS ALREADY A BUG UPSTREAM (issue #7586) - LOGGED RATHER THAN SILENTLY
      // ACCEPTED, SO THE ROOT CAUSE STAYS VISIBLE EVEN THOUGH THE ARCHIVE ITSELF IS STILL SAVED CORRECTLY
      logger.logLine(0, "- WARNING: archive entry name '%s' carried a directory prefix, stored as '%s' instead", name,
          entryName);

    logger.log(2, "- File '%s'...", entryName);
    final BackupArchiveWriter.EntryStats stats = archive.addEntry(entryName, lastModified, input);
    final long origSize = stats.uncompressedSize();
    final long compressedSize = stats.compressedSize();

    logger.logLine(2, " %s -> %s (%,d%% compressed)", FileUtils.getSizeAsString(origSize),
        FileUtils.getSizeAsString(compressedSize), origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
    recordArchivedEntry(entryName, origSize);
    return origSize;
  }

  /**
   * Every archive entry must sit at the archive root: {@code FullRestoreFormat} extracts each one under its own
   * name straight into the database directory, so any directory component would nest it where the reopened
   * database cannot find it (issue #7586). {@code compressFile}/{@code addFile} already guarantee this by deriving
   * the entry name from {@link File#getName()}; this is the same guarantee for a name handed in directly rather
   * than read off a {@link File}, so an upstream separator mismatch can never reach the archive uncaught - see
   * {@link FileUtils#lastIndexOfSeparator(String)} for why a lookup keyed on this JVM's own {@link File#separator}
   * would not be enough.
   */
  static String archiveEntryName(final String name) {
    return FileUtils.getFileNameFromPath(name);
  }

  private long compressFile(final BackupArchiveWriter archive, final File inputFile) throws IOException {
    return compressFile(archive, inputFile, true);
  }

  /**
   * The same, with the SKIP made a caller's decision rather than this method's (issue #7705).
   * <p>
   * {@code skippable} is what the page files and the configuration want: a file that is not there is simply left
   * out of the archive - {@code configuration.json} only exists once a setting has been persisted, and a component
   * file can be dropped while the enumeration runs. It is not what a TimeSeries sealed store wants, whose absence
   * the archive's own {@code schema.json} contradicts; {@link #compressSealedStores} passes {@code false} and says
   * why there.
   * <p>
   * With {@code skippable} false there is no {@code exists()} PRE-CHECK at all: the open inside
   * {@link BackupArchiveWriter#addFile} is the check, so there is no window between asking and reading in which
   * the file can go away silently, and a name that is present but unopenable fails here rather than being reported
   * as "not found".
   */
  private long compressFile(final BackupArchiveWriter archive, final File inputFile, final boolean skippable)
      throws IOException {
    logger.log(2, "- File '%s'...", inputFile.getName());
    if (!skippable || inputFile.exists()) {
      final BackupArchiveWriter.EntryStats stats = archive.addFile(inputFile);
      final long origSize = stats.uncompressedSize();
      final long compressedSize = stats.compressedSize();

      logger.logLine(2, " %s -> %s (%,d%% compressed)", FileUtils.getSizeAsString(origSize),
          FileUtils.getSizeAsString(compressedSize), origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
      recordArchivedEntry(inputFile.getName(), origSize);
      return origSize;
    }

    // STILL NOT AN ERROR HERE. A FILE THAT IS NOT THERE IS SKIPPED EXACTLY AS BEFORE - configuration.json ONLY
    // EXISTS ONCE A SETTING HAS BEEN PERSISTED, AND A COMPONENT FILE CAN BE DROPPED WHILE THE ENUMERATION RUNS.
    // WHAT CHANGED IN #7464 IS THAT THE SET OF ENTRIES IS CHECKED AFTERWARDS, ONCE, IN writeArchive
    logger.logLine(2, " not found");
    return 0;
  }

  /**
   * Notes an entry that made it into the archive, for {@link #checkArchiveCarriesTheSchema()} to read.
   * <p>
   * NON-EMPTY, NOT MERELY PRESENT. {@code LocalSchema.readConfiguration()} treats a zero-length
   * {@code schema.json} exactly as it treats a missing one - so a zero-byte entry restores to a database that
   * opens cleanly with an EMPTY schema and reports no error anywhere. That is the quieter half of the same
   * defect, and a presence check would let it through.
   * <p>
   * ONLY {@code schema.json} COUNTS, still, although the archive does now carry {@code schema.prev.json} too
   * (issue #7637). The previous copy is legitimately absent on a database whose schema has never been re-saved,
   * so requiring it would refuse backups of perfectly good databases; and a restore that had only the fallback
   * would open at the generation BEFORE the one the pages belong to. It is shipped so the restored database
   * keeps the corruption fallback its source had, not as a substitute for the primary.
   */
  private void recordArchivedEntry(final String entryName, final long origSize) {
    if (origSize > 0 && LocalSchema.SCHEMA_FILE_NAME.equals(entryName))
      schemaArchived = true;
  }

  /**
   * Refuses an archive that would not restore to a database, which is what issue #7464 reported: both paths
   * treated an absent {@code schema.json} as "nothing to archive" and completed, and the archive they produced
   * satisfied neither arm of {@code DatabaseFactory.exists()} - it looks for {@code schema.json} and then for
   * {@code schema.prev.json}, and at the time neither path archived the latter either (both do since #7637, but
   * it is optional, so it does not make this check redundant). The restored directory was therefore not
   * recognised as a database at all, from a backup that had printed "Full backup completed".
   * <p>
   * HERE AND NOT AT EACH READER, for the reason the issue gives. Failing inside
   * {@code PageManager.captureConfigurationFiles} does not work: a raw {@code IOException} there bypasses the
   * {@code PageSnapshotException} retry, and raising a {@code PageSnapshotException} instead just retries onto
   * {@code backupFromFrozenFiles}, which reads the same missing file and skips it just as quietly. Asking the
   * archive what it actually contains covers both paths with one check, and covers them at the only moment at
   * which the answer is final.
   */
  private void checkArchiveCarriesTheSchema() {
    if (!schemaArchived)
      throw new BackupException(
          ("Backup of database '%s' aborted: the archive would carry no '%s' (missing or empty in '%s'), and a "
              + "database restored without it is not recognised as a database at all").formatted(database.getName(),
              LocalSchema.SCHEMA_FILE_NAME, database.getDatabasePath()));
  }

  private int resolveSetting(final Integer explicitValue, final GlobalConfiguration fallback) {
    return explicitValue != null ? explicitValue : database.getConfiguration().getValueAsInteger(fallback);
  }

  private int resolveThreads() {
    final int configured = resolveSetting(settings.compressionThreads, GlobalConfiguration.BACKUP_COMPRESSION_THREADS);
    if (configured >= 0)
      return configured;
    return autoCompressionThreads(Runtime.getRuntime().availableProcessors());
  }

  /**
   * The automatic thread count: half the available processors, capped at 8, never below 1. A backup runs alongside the
   * live workload it is already throttling through the flush suspension, so claiming every core would buy the backup's
   * own speed with the writers' CPU. The cap matters because scaling is close to linear - without it a 64-core machine
   * would put 32 threads on a job that saturates the disk long before that.
   * <p>
   * Package-private and taking the core count as an argument so the boundaries can be pinned by a test rather than
   * depending on whatever the machine running the suite happens to have.
   */
  static int autoCompressionThreads(final int availableProcessors) {
    return Math.max(1, Math.min(availableProcessors / 2, 8));
  }

  private void writeArchive(final File backupFile, final int compressionLevel, final int compressionThreads,
      final int maxMBPerSecond, final AtomicReference<Exception> failure, final BackupCallback callback) throws Exception {
    // PER ATTEMPT, NOT PER BACKUP. A snapshot attempt that archived schema.json and then lost its window retries
    // on the frozen-files path (see the PageSnapshotException branch in backupDatabase), and that second attempt
    // writes a brand new archive from scratch - so what the abandoned one contained says nothing about it
    schemaArchived = false;

    encryptFile(backupFile, out -> {
      final IoThrottler throttler = new IoThrottler(maxMBPerSecond);
      final BackupArchiveWriter archive = compressionThreads > 0 ?
          new ParallelZipArchiveWriter(out, compressionLevel, compressionThreads, throttler) :
          new ZipStreamArchiveWriter(out, compressionLevel, throttler);

      // NOT try-WITH-RESOURCES: A BACKUP THAT FAILED INSIDE suspendFlushAndExecute REACHES HERE NORMALLY (THAT METHOD
      // SWALLOWS ITS CALLBACK'S EXCEPTION), SO 'RETURNED WITHOUT THROWING' IS NOT THE SAME AS 'SUCCEEDED'. ONLY A
      // BACKUP THAT ACTUALLY SUCCEEDED EARNS A CENTRAL DIRECTORY; ANY OTHER OUTCOME ABORTS, WHICH RELEASES THE
      // RESOURCES WITHOUT TERMINATING THE ARCHIVE AND CANNOT THROW OVER THE FAILURE ALREADY IN FLIGHT.
      //
      // THE CLOSE ALSO SITS DELIBERATELY OUTSIDE THE CALLBACK, SO THE CENTRAL DIRECTORY IS WRITTEN AFTER THE READ LOCK
      // AND THE FLUSH SUSPENSION HAVE BEEN RELEASED - THE OLD CODE CLOSED THE ZIP INSIDE THEM. IT IS BUILT ENTIRELY
      // FROM THE PER-ENTRY RECORDS COLLECTED WHILE THE FILES WERE READ AND TOUCHES NO DATABASE STATE, SO NOTHING
      // REQUIRES THE SUSPENSION, AND KEEPING IT INSIDE WOULD HOLD WRITERS THROTTLED FOR ONE MORE WRITE PER FILE
      boolean terminated = false;
      try {
        callback.backup(archive);
        if (failure.get() != null)
          // SURFACE IT HERE RATHER THAN ONLY AFTER writeArchive RETURNS, SO THE STREAM-CLOSING BELOW KNOWS THE BACKUP
          // FAILED AND CANNOT LET ITS OWN close() FAILURE TAKE THE ROOT CAUSE'S PLACE
          throw failure.get();

        // AFTER THE FAILURE CHECK ABOVE, NOT BEFORE: A BACKUP THAT DIED HALFWAY IS ALSO MISSING ENTRIES, AND ITS
        // ROOT CAUSE IS THE ONE WORTH REPORTING. ONLY A RUN THAT OTHERWISE SUCCEEDED GETS TOLD ITS ARCHIVE WOULD
        // NOT RESTORE. THROWN HERE SO THE abort() BELOW RUNS: NO CENTRAL DIRECTORY IS WRITTEN, SO EVEN IF THE
        // CALLER'S delete() OF THE PARTIAL FILE FAILS, NOTHING WILL RESTORE FROM WHAT IS LEFT
        checkArchiveCarriesTheSchema();

        archive.close();
        terminated = true;
      } finally {
        if (!terminated)
          archive.abort();
      }
    });
  }

  /**
   * Takes ownership of the target path before a single byte is written, by creating the file itself rather than by
   * asking whether it exists. The check-then-create it replaces was a TOCTOU: two backups of the same database landing
   * on the same name - the scheduler's periodic tick and an operator's "backup now", two nodes writing to a shared
   * directory - both saw a free path, both opened their own stream on it, and interleaved their output into one
   * unreadable archive that each of them reported as a success (issue #6753). Whoever loses the create now fails
   * before it can touch the winner's file, including its delete-on-failure cleanup.
   * <p>
   * With {@code -o} the caller has asked for the existing file to be replaced, so there is nothing to claim and the
   * write below truncates whatever is there.
   */
  private void claimBackupFile(final File backupFile) {
    if (settings.overwriteFile)
      return;

    try {
      Files.createFile(backupFile.toPath());
    } catch (final FileAlreadyExistsException e) {
      // THE RESOLVED PATH, NOT settings.file: THE POINT OF THIS MESSAGE IS TO TELL AN OPERATOR WHICH ARCHIVE IS IN
      // THE WAY, AND THE NAME THE CALLER TYPED IS RELATIVE TO A DIRECTORY THEY DID NOT NECESSARILY CHOOSE
      throw new BackupException("The backup file '%s' already exist and '-o' setting is false".formatted(backupFile));
    } catch (final IOException e) {
      throw new BackupException("The backup file '%s' cannot be created".formatted(backupFile), e);
    }
  }

  private interface StreamCallback {
    void write(OutputStream out) throws Exception;
  }

  private void encryptFile(final File backupFile, final StreamCallback callback) throws Exception {
    try (final FileOutputStream fos = new FileOutputStream(backupFile)) {
      final OutputStream archiveStream;
      if (settings.encryptionKey != null) {
        // Generate a random salt (e.g., 16 bytes)
        final byte[] salt = new byte[16];

        new SecureRandom().nextBytes(salt);
        // Store this salt at the beginning of the backup file, similar to the IV.

        fos.write(salt);

        final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        // Iteration count should be high, e.g., 65536 or more
        final KeySpec spec = new PBEKeySpec(settings.encryptionKey.toCharArray(), salt, 65536, 256); // 256-bit key
        final SecretKey tmp = factory.generateSecret(spec);
        final byte[] derivedKeyBytes = tmp.getEncoded();
        final SecretKey secretKey = new SecretKeySpec(derivedKeyBytes, settings.encryptionAlgorithm);

        // Initialize cipher
        final Cipher cipher = Cipher.getInstance(settings.encryptionAlgorithm + "/CTR/NoPadding");
        final byte[] iv = new byte[16];
        new SecureRandom().nextBytes(iv);
        final IvParameterSpec ivSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);

        // Write IV at the beginning of the file
        fos.write(iv);

        // Wrap the output stream with CipherOutputStream
        archiveStream = new CipherOutputStream(fos, cipher);
      } else
        archiveStream = fos;

      try {
        callback.write(archiveStream);
        // ON THE ENCRYPTED PATH THIS FINALISES THE CIPHER; ON THE PLAIN ONE IT IS THE SAME STREAM THE try-WITH-RESOURCES
        // ALREADY OWNS, AND CLOSING A FileOutputStream TWICE IS A NO-OP. ON THE SUCCESS PATH A FAILURE TO CLOSE IS THE
        // BACKUP'S FAILURE - THE LAST BYTES MAY NOT HAVE REACHED THE DISK - SO IT PROPAGATES
        archiveStream.close();
      } catch (final Exception e) {
        try {
          archiveStream.close();
        } catch (final Exception closeError) {
          // THE BACKUP HAD ALREADY FAILED. THE CLOSE FAILURE IS A CONSEQUENCE, NOT THE CAUSE, SO IT IS RECORDED
          // ALONGSIDE INSTEAD OF REPLACING THE ONE THAT EXPLAINS WHAT WENT WRONG
          e.addSuppressed(closeError);
        }
        throw e;
      }
    }
  }
}
