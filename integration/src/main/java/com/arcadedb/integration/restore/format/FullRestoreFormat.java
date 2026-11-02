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
package com.arcadedb.integration.restore.format;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreException;
import com.arcadedb.integration.restore.RestoreSettings;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IPAddressBlocklist;
import com.arcadedb.utility.SafeHttpFetcher;

import java.io.*;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.security.spec.KeySpec;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FullRestoreFormat extends AbstractRestoreFormat {
  // SHARED WITH PostServerCommandHandler.isBlockedHost AND ImportSecurityValidator.isBlockedAddress: SEE
  // IPAddressBlocklist FOR WHY THIS MUST NOT BE A PER-CALLER COPY (GHSA-67m7-7w7g-mpmh).
  private static final IPAddressBlocklist RESERVED_ADDRESSES = IPAddressBlocklist.defaultReservedRanges();

  private final byte[] BUFFER = new byte[ParallelZipExtractor.BUFFER_SIZE];

  /** The salt and the IV an encrypted archive opens with are 16 bytes each, written by {@code FullBackupFormat.encryptFile}. */
  private static final int ENCRYPTION_HEADER_FIELD_SIZE = 16;

  private interface RestoreCallback {
    ParallelZipExtractor.ExtractStats restore(ZipInputStream zipFile) throws Exception;
  }

  /**
   * Where the archive is, and how it can be read. Exactly one of the two is set: a local {@code File}, or an already
   * opened remote stream. The local stream is opened lazily, because the parallel path never wants one.
   */
  private record RestoreInputSource(File localFile, InputStream remoteStream, long fileSize) {
    InputStream openStream() throws IOException {
      return remoteStream != null ? remoteStream : new FileInputStream(localFile);
    }

    /**
     * The archive as something that can be opened for random access, or {@code null} when it cannot be - which is
     * the precondition of the parallel path. A directory or a named pipe passes {@code exists()} but is not a plain
     * file, so it keeps the sequential path and fails there the way it always did, rather than failing differently
     * inside {@code ZipFile}.
     */
    File randomAccessArchive() {
      return remoteStream == null && localFile.isFile() ? localFile : null;
    }

    /**
     * Releases the remote stream, which is opened before the target directory is checked: a restore refused there, or one
     * that failed before reading the archive to its end, used to leave the connection open.
     */
    void close() {
      if (remoteStream != null)
        try {
          remoteStream.close();
        } catch (final IOException e) {
          // NOTHING LEFT TO READ FROM IT: THE RESTORE HAS ALREADY ENDED, ONE WAY OR THE OTHER
        }
    }
  }

  public FullRestoreFormat(final DatabaseInternal database, final RestoreSettings settings, final ConsoleLogger logger) {
    super(database, settings, logger);
  }

  @Override
  public void restoreDatabase() throws Exception {
    settings.validate();

    final RestoreInputSource inputSource = openInputFile();
    try {
      restoreFrom(inputSource);
    } finally {
      inputSource.close();
    }
  }

  /**
   * Extracts into a STAGING directory, a hidden sibling of the target, and moves it into place only once the archive was
   * extracted and checked (issue #11058). Before, the archive was extracted straight into the target, so a restore that failed
   * part way left a half-written directory there that nothing removed: every later restore into that path, the retry of the
   * same command with a good archive included, was refused with "already exist", and with {@code -o} the database being
   * replaced had already been deleted before a byte was read. Now a failed restore leaves the target exactly as it found it -
   * absent, or the database {@code -o} would have replaced - and the retry is the same command.
   * <p>
   * The server's restore ({@code ServerControlPlane.performRestore}) already followed this policy around the call (issue
   * #5027); it lives here now, in the code that makes the mess, so the CLI and embedded callers get it too. The staging
   * directory is a sibling so the final move is a rename on one filesystem, and dot-prefixed so a server scanning the parent
   * never takes it for a database ({@code ArcadeDBServer.RESERVED_DATABASE_PREFIX}). The price is disk: with {@code -o} the
   * replaced database and the restored copy coexist until the swap, as they already did on the server.
   */
  private void restoreFrom(final RestoreInputSource inputSource) throws Exception {
    final File databaseDirectory = new File(settings.databaseDirectory).getAbsoluteFile();
    // AN EARLY ANSWER ONLY, SO A RESTORE THAT CANNOT LAND IS NOT EXTRACTED FIRST: THE MOVE IN moveIntoPlace IS THE CHECK
    // THAT COUNTS, BECAUSE A DIRECTORY CAN APPEAR BETWEEN THIS LINE AND THAT ONE
    if (databaseDirectory.exists() && !settings.overwriteDestination)
      throw new RestoreException(
          "The database directory '%s' already exist and '-o' setting is false".formatted(settings.databaseDirectory));

    final File parentDirectory = databaseDirectory.getParentFile();
    final File stagingDirectory = new File(parentDirectory, "." + databaseDirectory.getName() + ".restore-" + System.nanoTime());
    try {
      Files.createDirectories(parentDirectory.toPath());
      Files.createDirectory(stagingDirectory.toPath());
    } catch (final IOException e) {
      throw new RestoreException(
          "Error on restoring database: the database directory '%s' cannot be created".formatted(settings.databaseDirectory), e);
    }

    try {
      final long beginTime = System.currentTimeMillis();
      final ParallelZipExtractor.ExtractStats stats = extractAndCheck(inputSource, stagingDirectory);
      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

      // THE FILES WERE SYNCED ONE BY ONE AS THEY WERE EXTRACTED; THEIR NAMES LIVE IN THE DIRECTORY, AND THE DIRECTORY ITSELF
      // IS RENAMED INTO ITS PARENT, SO BOTH ARE FORCED TOO OR A POWER LOSS CAN STILL LOSE THE RESTORE (ISSUE #8626)
      FileUtils.forceDirectory(stagingDirectory.toPath());
      moveIntoPlace(stagingDirectory, databaseDirectory);
      FileUtils.forceDirectory(parentDirectory.toPath());

      logger.logLine(0, "Full restore completed in %d seconds %s -> %s (%,d%% compression)", elapsedInSecs,
          FileUtils.getSizeAsString(inputSource.fileSize()), FileUtils.getSizeAsString(stats.uncompressedSize()),
          stats.uncompressedSize() > 0 ? (stats.uncompressedSize() - inputSource.fileSize()) * 100 / stats.uncompressedSize() : 0);
    } catch (final Throwable t) {
      // Throwable: AN OutOfMemoryError HALFWAY THROUGH A LARGE ARCHIVE LEAVES THE SAME HALF-WRITTEN DIRECTORY AS AN EXCEPTION
      FileUtils.deleteRecursively(stagingDirectory);
      throw t;
    }
  }

  /**
   * Moves the staged restore to the target. Without {@code -o} it is one rename that fails if the target appeared while the
   * archive was extracted, rather than replacing it: this restore was accepted because there was nothing there. With {@code -o}
   * the database being replaced is renamed aside first and deleted only after the restored copy took its name, and put back
   * if that rename fails, so there is no moment at which neither exists on disk.
   */
  private void moveIntoPlace(final File stagingDirectory, final File databaseDirectory) throws IOException {
    if (settings.overwriteDestination && databaseDirectory.exists()) {
      final File replaced = new File(databaseDirectory.getParentFile(),
          "." + databaseDirectory.getName() + ".replaced-" + System.nanoTime());
      Files.move(databaseDirectory.toPath(), replaced.toPath());
      try {
        Files.move(stagingDirectory.toPath(), databaseDirectory.toPath());
      } catch (final IOException | RuntimeException e) {
        try {
          Files.move(replaced.toPath(), databaseDirectory.toPath());
        } catch (final IOException | RuntimeException putBack) {
          e.addSuppressed(putBack);
        }
        throw e;
      }
      FileUtils.deleteRecursively(replaced);
      return;
    }

    // NO REPLACE_EXISTING AND NO ATOMIC_MOVE: WITHOUT THE FIRST THE MOVE REFUSES AN EXISTING TARGET, AND ATOMIC_MOVE WOULD LET
    // rename(2) SILENTLY REPLACE AN EMPTY ONE ON POSIX (SEE ServerControlPlane.swapRestoredDatabase, ISSUE #7441)
    try {
      Files.move(stagingDirectory.toPath(), databaseDirectory.toPath());
    } catch (final FileAlreadyExistsException e) {
      throw new RestoreException(
          "The database directory '%s' was created while the restore was running: the restored copy was discarded rather than replace it".formatted(
              settings.databaseDirectory), e);
    }
  }

  /**
   * Extracts the archive into {@code directory} and checks the result is a database.
   */
  private ParallelZipExtractor.ExtractStats extractAndCheck(final RestoreInputSource inputSource, final File directory)
      throws Exception {
    // THE PARALLEL PATH NEEDS RANDOM ACCESS TO THE ARCHIVE. NEITHER OF THE OTHER TWO INPUT SOURCES CAN GIVE IT: AN
    // http(s) ARCHIVE IS A ONE-SHOT STREAM, AND AN ENCRYPTED ONE IS A SINGLE CIPHER STREAM THAT ONLY DECRYPTS FRONT TO
    // BACK. BOTH FALL BACK TO THE SEQUENTIAL WALK RATHER THAN BEING REFUSED - A RESTORE MUST NEVER FAIL BECAUSE OF A
    // PERFORMANCE SETTING
    final int threads = resolveThreads();
    final File randomAccessArchive = settings.encryptionKey == null ? inputSource.randomAccessArchive() : null;
    final boolean parallel = threads > 0 && randomAccessArchive != null;

    logger.logLine(0, "Executing full restore of database from file '%s' to '%s' (%s)...", settings.inputFileURL,
        settings.databaseDirectory, parallel ? threads + " threads" : "single threaded");

    final ParallelZipExtractor.ExtractStats stats = parallel ?
        new ParallelZipExtractor(threads, logger, progressCallback).extract(randomAccessArchive, directory) :
        restoreSequentially(inputSource, directory);

    if (stats.files() == 0)
      throw new RestoreException("Unable to perform restore");

    checkRestoredDirectoryIsADatabase(directory);
    return stats;
  }

  /**
   * Refuses a restore that produced a directory ArcadeDB does not recognise as a database (issue #7464).
   * <p>
   * Every build up to the one that carries #7464's backup-side fix could write an archive with no
   * {@code schema.json} in it: both backup paths treated the file as optional and completed anyway. Extracted,
   * such an archive leaves a directory full of page files that {@code DatabaseFactory.exists()} answers "no" to,
   * so the next thing the operator sees is "database not found" with nothing connecting it to the restore that
   * reported success minutes earlier. This build cannot stop those archives existing - it can stop them
   * restoring quietly.
   * <p>
   * THE SAME TWO ARMS AS {@code DatabaseFactory.exists()}, deliberately. {@code schema.prev.json} is what
   * {@code LocalSchema.readConfiguration()} loads from when the primary is missing or empty, so an archive
   * carrying only the fallback really does restore to a working database and must not be refused. The size test
   * is there for the same reason {@code readConfiguration()} applies one: a zero-length {@code schema.json}
   * passes a presence check and then loads as an EMPTY schema, which is the failure that reports nothing at all.
   * <p>
   * Checked on the extracted directory rather than on the entry names, so the sequential walk and the parallel
   * extractor of #6086 are both covered by one check at the point they both return through.
   * <p>
   * The thrown message deliberately carries no issue number: it is read by an operator holding an archive that
   * will not restore, years after #7464 stops meaning anything to anyone. The reference lives here instead.
   */
  private void checkRestoredDirectoryIsADatabase(final File databaseDirectory) {
    if (isUsable(new File(databaseDirectory, LocalSchema.SCHEMA_FILE_NAME))
        || isUsable(new File(databaseDirectory, LocalSchema.SCHEMA_PREV_FILE_NAME)))
      return;

    throw new RestoreException(
        ("Restore of '%s' aborted: the archive carries no usable '%s' (nor the '%s' ArcadeDB falls back to), so "
            + "the restored directory would not be recognised as a database. It was produced by a backup that "
            + "skipped the file and reported success").formatted(settings.inputFileURL,
            LocalSchema.SCHEMA_FILE_NAME, LocalSchema.SCHEMA_PREV_FILE_NAME));
  }

  private static boolean isUsable(final File file) {
    return file.exists() && file.length() > 0;
  }

  /**
   * The one-thread stream walk, which is both the pre-#6086 path and the only one available for an http(s) or an
   * encrypted archive.
   */
  private ParallelZipExtractor.ExtractStats restoreSequentially(final RestoreInputSource inputSource,
      final File databaseDirectory) throws Exception {
    // THE BufferedInputStream IS NOT A DETAIL, AND IT IS THE ONLY SPEEDUP THE http AND ENCRYPTED ARCHIVES CAN HAVE.
    // ZipInputStream FILLS ITS INFLATER 512 BYTES AT A TIME (THE SIZE ITS CONSTRUCTOR HARDCODES, WITH NO OVERLOAD TO
    // CHANGE IT), SO WITHOUT A BUFFER UNDERNEATH IT THAT IS ONE read() AGAINST THE FILE - OR AGAINST THE SOCKET, OR
    // THROUGH THE CIPHER - PER 512 COMPRESSED BYTES. MEASURED ON A 1.25 GB DATABASE: 5.16 s WITHOUT IT, 3.96 s WITH
    // IT, FOR A CHANGE THAT COSTS ONE 256 KB BUFFER. THE COPY BUFFER ABOVE IT IS WORTH ALMOST NOTHING BY COMPARISON
    // (3.96 s AT 8 KB AGAINST 3.92 s AT 256 KB), WHICH IS WHY THE ISSUE'S "THE 8 KB BUFFER IS SUSPICIOUSLY SMALL"
    // HYPOTHESIS WAS THE WRONG ONE: THE RESTORE IS INFLATE-BOUND, NOT WRITE-BOUND
    return decryptFile(new BufferedInputStream(inputSource.openStream(), ParallelZipExtractor.BUFFER_SIZE), zipFile -> {
      int restoredFiles = 0;
      long databaseOrigSize = 0L;

      ZipEntry compressedFile = zipFile.getNextEntry();
      while (compressedFile != null) {
        databaseOrigSize += uncompressFile(zipFile, compressedFile, databaseDirectory);
        compressedFile = zipFile.getNextEntry();
        ++restoredFiles;
        // -1 FOR THE DENOMINATOR BECAUSE THERE IS NO HONEST ONE HERE: THIS WALK LEARNS AN ENTRY ONLY WHEN IT
        // REACHES IT, SO THE COUNT OF ENTRIES STILL TO COME IS NOT KNOWN UNTIL THERE ARE NONE. OperationProgress
        // RENDERS -1 AS "PERCENTAGE UNKNOWN", WHICH IS THE TRUE ANSWER RATHER THAN A GUESSED ONE (ISSUE #7385)
        reportRestoreProgress(restoredFiles, -1L);
      }

      zipFile.close();

      return new ParallelZipExtractor.ExtractStats(restoredFiles, databaseOrigSize);
    });
  }

  /**
   * -1 sizes the pool automatically, 0 selects the sequential walk, N a pool of N. The setting is read from the JVM
   * configuration rather than from a database one because a restore has no database to read it from: the database it
   * is producing does not exist yet.
   */
  private int resolveThreads() {
    final int configured = settings.restoreThreads != null ?
        settings.restoreThreads :
        GlobalConfiguration.RESTORE_THREADS.getValueAsInteger();
    if (configured >= 0)
      return configured;
    return autoRestoreThreads(Runtime.getRuntime().availableProcessors());
  }

  /**
   * The automatic thread count: the available processors, capped at 8, never below 1. Unlike a backup - which runs
   * next to the live workload it is already throttling, and therefore takes only half the cores - a restore does not
   * compete with the database it is restoring, because that database is not open yet. The cap is there because the
   * unit of parallelism is one archive entry: past a handful of threads the limit is the entry count and the disk,
   * not the core count.
   * <p>
   * Package-private and taking the core count as an argument so the boundaries can be pinned by a test rather than
   * depending on whatever the machine running the suite happens to have.
   */
  static int autoRestoreThreads(final int availableProcessors) {
    return Math.max(1, Math.min(availableProcessors, 8));
  }

  private long uncompressFile(final ZipInputStream inputFile, final ZipEntry compressedFile, final File databaseDirectory)
      throws IOException {
    final String fileName = compressedFile.getName();

    FileUtils.checkValidName(fileName);

    logger.log(2, "- File '%s'...", fileName);

    final File uncompressedFile = new File(databaseDirectory, fileName);

    if (!uncompressedFile.toPath().normalize().startsWith(databaseDirectory.toPath().normalize())) {
      throw new IOException("Bad zip entry");
    }

    try (final FileOutputStream fileOut = new FileOutputStream(uncompressedFile)) {
      int len;
      while ((len = inputFile.read(BUFFER)) > 0) {
        fileOut.write(BUFFER, 0, len);
      }
      // DURABLE BEFORE THE RESTORE REPORTS SUCCESS: SEE ParallelZipExtractor, WHICH DOES THE SAME PER ENTRY (ISSUE #8626)
      fileOut.getFD().sync();
    }

    final long origSize = uncompressedFile.length();
    final long compressedSize = compressedFile.getCompressedSize();

    if (compressedSize > -1) {
      logger.logLine(2, " %s -> %s (%,d%% compression)",
          FileUtils.getSizeAsString(compressedSize), FileUtils.getSizeAsString(origSize),
          origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);
    } else
      logger.logLine(2, " uncompressed to %s", FileUtils.getSizeAsString(origSize));

    return origSize;
  }

  private RestoreInputSource openInputFile() throws IOException {
    if (settings.inputFileURL.startsWith("http://") || settings.inputFileURL.startsWith("https://")) {
      // ROUTE THROUGH THE SAME PER-HOP-REVALIDATING FETCHER `IMPORT DATABASE` USES (ImportSecurityValidator), RATHER
      // THAN LETTING HttpURLConnection FOLLOW REDIRECTS ITSELF: A REDIRECT (OR A DNS-REBOUND RE-RESOLUTION) THAT
      // LANDS ON A BLOCKED ADDRESS WOULD OTHERWISE BYPASS THE ONE-SHOT HOST CHECK
      // PostServerCommandHandler.validateClientRestoreImportUrl ALREADY DID BEFORE HANDING OFF THE URL (ISSUE #6381).
      //
      // settings.allowLocalUrls IS SET EXPLICITLY BY A CALLER THAT ALREADY RESOLVED THIS AGAINST ITS OWN
      // CONFIGURATION (THE SERVER COMMAND HANDLER, AGAINST ITS PER-INSTANCE ContextConfiguration) SO THE FETCH AGREES
      // WITH WHATEVER PRE-CHECK ALREADY ACCEPTED THE COMMAND. A CLI/EMBEDDED CALLER WITH NO SUCH CONTEXT LEAVES IT
      // null AND FALLS BACK TO THE STATIC GLOBAL DEFAULT.
      final boolean allowLocalUrls = settings.allowLocalUrls != null ?
          settings.allowLocalUrls : GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getValueAsBoolean();
      // THE CALLER'S OVERLAY FIRST, THE TARGET DATABASE'S ONLY AS A FALLBACK. BOTH TIMEOUTS ARE SCOPE.SERVER AND A
      // ContextConfiguration NEVER WRITES THROUGH TO THE ENUM, SO READING THE ENUM MEANS IGNORING WHAT THE OPERATOR
      // CONFIGURED. THE DATABASE ALONE IS NOT ENOUGH: A FRESH RESTORE HAS NO TARGET DATABASE YET AT FETCH TIME, SO
      // THAT BRANCH WAS null EVERY TIME ON THE ONE PATH THAT MATTERS - THE SERVER'S OWN RESTORE (PR #7755 REVIEW)
      final ContextConfiguration configuration = settings.configuration != null ?
          settings.configuration :
          database != null ? database.getConfiguration() : null;

      final HttpURLConnection connection = SafeHttpFetcher.open(settings.inputFileURL,
          address -> !allowLocalUrls && RESERVED_ADDRESSES.isBlocked(address), "RESTORE DATABASE", configuration);

      // body() and not getInputStream(): the read timeout the fetch applies is reported as a timeout naming the
      // setting that relaxes it, rather than as a bare "Read timed out" inside a restore failure (issue #7500).
      return new RestoreInputSource(null, SafeHttpFetcher.body(connection, "RESTORE DATABASE"), 0);
    }

    String path = settings.inputFileURL;
    if (path.startsWith("file://")) {
      path = path.substring("file://".length());
    } else if (path.startsWith("classpath://"))
      path = getClass().getClassLoader().getResource(path.substring("classpath://".length())).getFile();

    final File file = new File(path);
    if (!file.exists())
      throw new RestoreException("The backup file '%s' does not exist (local path=%s)".formatted(//
          settings.inputFileURL, new File(".").getAbsolutePath()));

    return new RestoreInputSource(file, null, file.length());
  }

  private ParallelZipExtractor.ExtractStats decryptFile(final InputStream fis,
      final FullRestoreFormat.RestoreCallback callback) throws Exception {
    final ZipInputStream zipFile;
    if (settings.encryptionKey != null) {
      // Read salt from the beginning of the file (16 bytes). readNBytes() and not read(): a single read() may return
      // fewer bytes than asked, which a socket does routinely between two segments of an http(s) archive (issue #10773)
      final byte[] salt = readHeaderField(fis, "salt");

      // Derive the key using PBKDF2 with the salt
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final KeySpec spec = new PBEKeySpec(settings.encryptionKey.toCharArray(), salt, 65536,
          256);
      final SecretKey tmp = factory.generateSecret(spec);
      final byte[] keyBytes = tmp.getEncoded();

      final SecretKey secretKey = new SecretKeySpec(keyBytes, settings.encryptionAlgorithm);
      // Read IV from the beginning of the file
      final byte[] iv = readHeaderField(fis, "IV");
      final IvParameterSpec ivSpec = new IvParameterSpec(iv);

      // Initialize cipher for decryption
      final Cipher cipher = Cipher.getInstance(settings.encryptionAlgorithm + "/CTR/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);

      // Wrap the input stream with CipherInputStream
      final CipherInputStream cis = new CipherInputStream(fis, cipher);
      zipFile = new ZipInputStream(cis, StandardCharsets.UTF_8);
    } else
      zipFile = new ZipInputStream(fis, StandardCharsets.UTF_8);

    try {
      return callback.restore(zipFile);
    } finally {
      zipFile.close();
    }
  }

  /**
   * Reads one of the two 16-byte fields an encrypted archive opens with. Only the end of the stream makes it shorter: an
   * archive cut before its header is complete is refused, a stream that merely delivers it in pieces is not.
   */
  private static byte[] readHeaderField(final InputStream in, final String field) throws IOException {
    final byte[] value = in.readNBytes(ENCRYPTION_HEADER_FIELD_SIZE);
    if (value.length != ENCRYPTION_HEADER_FIELD_SIZE)
      throw new IOException("Unable to read " + field + " from encrypted file: the archive ends after " + value.length + " of its "
          + ENCRYPTION_HEADER_FIELD_SIZE + " bytes");
    return value;
  }
}
