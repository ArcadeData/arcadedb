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
 */
package com.arcadedb.integration.restore.format;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreException;
import com.arcadedb.integration.restore.RestoreSettings;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.zip.*;

public class FullRestoreFormat extends AbstractRestoreFormat {
  private final byte[] BUFFER = new byte[8192];

  public FullRestoreFormat(final DatabaseInternal database, final RestoreSettings settings, final ConsoleLogger logger) {
    super(database, settings, logger);
  }

  @Override
  public void restoreDatabase() throws Exception {
    final File file = new File(settings.file);
    if (!file.exists())
      throw new RestoreException(String.format("The backup file '%s' not exist", settings.file));

    final File databaseDirectory = new File(settings.databaseURL);
    if (databaseDirectory.exists()) {
      if (!settings.overwriteDestination)
        throw new RestoreException(String.format("The database directory '%s' already exist and '-o' setting is false", settings.databaseURL));

      FileUtils.deleteRecursively(databaseDirectory);
    }

    if (!databaseDirectory.mkdirs())
      throw new RestoreException(String.format("Error on restoring database: the database directory '%s' cannot be created", settings.databaseURL));

    logger.logLine(0, "Executing full restore of database from file '%s' to '%s'...", settings.file, settings.databaseURL);

    final File backupFile = new File(settings.file);
    final long databaseCompressedSize = backupFile.length();

    try (ZipInputStream zipFile = new ZipInputStream(new FileInputStream(backupFile), DatabaseFactory.getDefaultCharset())) {
      final long beginTime = System.currentTimeMillis();

      long databaseOrigSize = 0L;

      ZipEntry compressedFile = zipFile.getNextEntry();
      while (compressedFile != null) {
        databaseOrigSize += uncompressFile(zipFile, compressedFile, databaseDirectory);
        compressedFile = zipFile.getNextEntry();
      }

      zipFile.close();

      final long elapsedInSecs = (System.currentTimeMillis() - beginTime) / 1000;

      logger.logLine(0, "Full restore completed in %d seconds %s -> %s (%,d%% compression)", elapsedInSecs, FileUtils.getSizeAsString(databaseOrigSize),
          FileUtils.getSizeAsString((databaseCompressedSize)), databaseOrigSize > 0 ? (databaseOrigSize - databaseCompressedSize) * 100 / databaseOrigSize : 0);
    }
  }

  private long uncompressFile(final ZipInputStream inputFile, ZipEntry compressedFile, final File databaseDirectory) throws IOException {
    logger.log(2, "- File '%s'...", compressedFile.getName());

    final File uncompressedFile = new File(databaseDirectory, compressedFile.getName());

    try (final FileOutputStream fileOut = new FileOutputStream(uncompressedFile)) {
      int len;
      while ((len = inputFile.read(BUFFER)) > 0) {
        fileOut.write(BUFFER, 0, len);
      }
    }

    final long origSize = uncompressedFile.length();
    final long compressedSize = compressedFile.getCompressedSize();

    logger.logLine(2, " %s -> %s (%,d%% compressed)", FileUtils.getSizeAsString(origSize), FileUtils.getSizeAsString(compressedSize),
        origSize > 0 ? (origSize - compressedSize) * 100 / origSize : 0);

    return origSize;
  }
}
