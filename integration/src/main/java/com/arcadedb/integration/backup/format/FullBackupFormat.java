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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FullBackupFormat extends AbstractBackupFormat {
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

    if (backupFile.exists() && !settings.overwriteFile)
      throw new BackupException("The backup file '%s' already exist and '-o' setting is false".formatted(settings.file));

    if (backupFile.getParentFile() != null && !backupFile.getParentFile().exists()) {
      if (!backupFile.getParentFile().mkdirs())
        throw new BackupException("The backup file '%s' cannot be created".formatted(backupFile));
    }

    if (database.isTransactionActive() && database.getTransaction().hasChanges())
      throw new BackupException("Transaction in progress found");

    final int compressionLevel = resolveSetting(settings.compressionLevel, GlobalConfiguration.BACKUP_COMPRESSION_LEVEL);
    final int compressionThreads = resolveThreads();
    final int maxMBPerSecond = resolveSetting(settings.maxMBPerSecond, GlobalConfiguration.BACKUP_MAX_MB_PER_SECOND);

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

      try {
        writeArchive(backupFile, compressionLevel, compressionThreads, maxMBPerSecond, failure, archive ->
          // ACQUIRE A READ LOCK. TRANSACTION CAN STILL RUN, BUT CREATION OF NEW FILES (BUCKETS, TYPES, INDEXES) WILL BE PUT ON PAUSE UNTIL THIS LOCK IS RELEASED
          database.executeInReadLock(() -> {
            if (snapshotAttempt)
              databaseOrigSize.set(backupFromSnapshot(archive));
            else
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
          }));
        // NO SECOND CHECK OF failure HERE: writeArchive RETHROWS IT FROM INSIDE ITS OWN CALLBACK, WHICH IT HAS TO DO SO
        // THE STREAM-CLOSING THERE KNOWS THE BACKUP FAILED. A CHECK AT THIS POINT WOULD BE UNREACHABLE, AND UNREACHABLE
        // SAFETY NETS ONLY TEACH THE NEXT READER THAT THE THROW ABOVE MIGHT NOT HAPPEN
        break;
      } catch (final PageSnapshotException e) {
        // A PARTIAL ARCHIVE MUST NOT SURVIVE: LEAVING ONE BEHIND INVITES A RESTORE FROM IT
        backupFile.delete();
        if (!snapshotAttempt)
          throw e;
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
   * Archives the two configuration files plus every PAGE file as it stood at the snapshot's t0 (issue #6075).
   * <p>
   * The configuration files are still read straight off the filesystem: they are not page files, so the snapshot
   * does not cover them, and the database read lock this runs under is what keeps them consistent with the page
   * files - it excludes the DDL that rewrites them. Files created after t0 are absent from the snapshot by
   * construction, which is correct: they did not exist at the point in time being archived. Files DROPPED after t0
   * are still readable, because their physical deletion is deferred until the window closes.
   */
  private long backupFromSnapshot(final BackupArchiveWriter archive) throws Exception {
    long origSize = 0L;
    try (final PageSnapshot snapshot = database.getPageManager().openSnapshot(database)) {
      origSize += compressFile(archive, ((LocalDatabase) database.getEmbedded()).getConfigurationFile());
      origSize += compressFile(archive, ((LocalSchema) database.getSchema()).getConfigurationFile());

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
    origSize += compressFile(archive, ((LocalSchema) database.getSchema()).getConfigurationFile());

    final Collection<ComponentFile> files = database.getFileManager().getFiles();

    for (final ComponentFile file : new ArrayList<>(files))
      if (file != null)
        origSize += compressFile(archive, file.getOSFile());

    return origSize;
  }

  private long compressEntry(final BackupArchiveWriter archive, final String name, final long lastModified,
      final InputStream input) throws IOException {
    logger.log(2, "- File '%s'...", name);
    final BackupArchiveWriter.EntryStats stats = archive.addEntry(name, lastModified, input);
    final long origSize = stats.uncompressedSize();
    final long compressedSize = stats.compressedSize();

    logger.logLine(2, " %s -> %s (%,d%% compressed)", FileUtils.getSizeAsString(origSize),
        FileUtils.getSizeAsString(compressedSize), origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
    return origSize;
  }

  private long compressFile(final BackupArchiveWriter archive, final File inputFile) throws IOException {
    logger.log(2, "- File '%s'...", inputFile.getName());
    if (inputFile.exists()) {
      final BackupArchiveWriter.EntryStats stats = archive.addFile(inputFile);
      final long origSize = stats.uncompressedSize();
      final long compressedSize = stats.compressedSize();

      logger.logLine(2, " %s -> %s (%,d%% compressed)", FileUtils.getSizeAsString(origSize),
          FileUtils.getSizeAsString(compressedSize), origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
      return origSize;
    }

    logger.logLine(2, " not found");
    return 0;
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

        archive.close();
        terminated = true;
      } finally {
        if (!terminated)
          archive.abort();
      }
    });
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
