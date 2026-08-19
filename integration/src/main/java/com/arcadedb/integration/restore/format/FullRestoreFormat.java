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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreException;
import com.arcadedb.integration.restore.RestoreSettings;
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
import java.security.spec.KeySpec;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FullRestoreFormat extends AbstractRestoreFormat {
  // SHARED WITH PostServerCommandHandler.isBlockedHost AND ImportSecurityValidator.isBlockedAddress: SEE
  // IPAddressBlocklist FOR WHY THIS MUST NOT BE A PER-CALLER COPY (GHSA-67m7-7w7g-mpmh).
  private static final IPAddressBlocklist RESERVED_ADDRESSES = IPAddressBlocklist.defaultReservedRanges();

  private final byte[] BUFFER = new byte[ParallelZipExtractor.BUFFER_SIZE];

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
  }

  public FullRestoreFormat(final DatabaseInternal database, final RestoreSettings settings, final ConsoleLogger logger) {
    super(database, settings, logger);
  }

  @Override
  public void restoreDatabase() throws Exception {
    settings.validate();

    final RestoreInputSource inputSource = openInputFile();

    final File databaseDirectory = new File(settings.databaseDirectory);
    if (databaseDirectory.exists()) {
      if (!settings.overwriteDestination)
        throw new RestoreException(
            "The database directory '%s' already exist and '-o' setting is false".formatted(settings.databaseDirectory));

      FileUtils.deleteRecursively(databaseDirectory);
    }

    if (!databaseDirectory.mkdirs())
      throw new RestoreException(
          "Error on restoring database: the database directory '%s' cannot be created".formatted(settings.databaseDirectory));

    // THE PARALLEL PATH NEEDS RANDOM ACCESS TO THE ARCHIVE. NEITHER OF THE OTHER TWO INPUT SOURCES CAN GIVE IT: AN
    // http(s) ARCHIVE IS A ONE-SHOT STREAM, AND AN ENCRYPTED ONE IS A SINGLE CIPHER STREAM THAT ONLY DECRYPTS FRONT TO
    // BACK. BOTH FALL BACK TO THE SEQUENTIAL WALK RATHER THAN BEING REFUSED - A RESTORE MUST NEVER FAIL BECAUSE OF A
    // PERFORMANCE SETTING
    final int threads = resolveThreads();
    final File randomAccessArchive = settings.encryptionKey == null ? inputSource.randomAccessArchive() : null;
    final boolean parallel = threads > 0 && randomAccessArchive != null;

    logger.logLine(0, "Executing full restore of database from file '%s' to '%s' (%s)...", settings.inputFileURL,
        settings.databaseDirectory, parallel ? threads + " threads" : "single threaded");

    final long beginTime = System.currentTimeMillis();

    final ParallelZipExtractor.ExtractStats stats = parallel ?
        new ParallelZipExtractor(threads, logger).extract(randomAccessArchive, databaseDirectory) :
        restoreSequentially(inputSource, databaseDirectory);

    final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

    if (stats.files() == 0)
      throw new RestoreException("Unable to perform restore");

    logger.logLine(0, "Full restore completed in %d seconds %s -> %s (%,d%% compression)", elapsedInSecs,
        FileUtils.getSizeAsString(inputSource.fileSize()), FileUtils.getSizeAsString(stats.uncompressedSize()),
        stats.uncompressedSize() > 0 ? (stats.uncompressedSize() - inputSource.fileSize()) * 100 / stats.uncompressedSize() : 0);
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
      final HttpURLConnection connection = SafeHttpFetcher.open(settings.inputFileURL,
          address -> !allowLocalUrls && RESERVED_ADDRESSES.isBlocked(address), "RESTORE DATABASE");

      return new RestoreInputSource(null, connection.getInputStream(), 0);
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
      // Read salt from the beginning of the file (16 bytes)
      final byte[] salt = new byte[16];
      if (fis.read(salt) != salt.length) {
        throw new IOException("Unable to read salt from encrypted file");
      }

      // Derive the key using PBKDF2 with the salt
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      final KeySpec spec = new PBEKeySpec(settings.encryptionKey.toCharArray(), salt, 65536,
          256);
      final SecretKey tmp = factory.generateSecret(spec);
      final byte[] keyBytes = tmp.getEncoded();

      final SecretKey secretKey = new SecretKeySpec(keyBytes, settings.encryptionAlgorithm);
      // Read IV from the beginning of the file
      final byte[] iv = new byte[16];
      if (fis.read(iv) != iv.length) {
        throw new IOException("Unable to read IV from encrypted file");
      }
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
}
