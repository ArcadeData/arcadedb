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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreException;
import com.arcadedb.integration.restore.RestoreSettings;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FullRestoreFormat extends AbstractRestoreFormat {
  private final byte[] BUFFER = new byte[8192];

  private static class RestoreInputSource {
    public final InputStream inputStream;
    public final long        fileSize;

    public RestoreInputSource(final InputStream inputStream, final long fileSize) {
      this.inputStream = inputStream;
      this.fileSize = fileSize;
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
        throw new RestoreException(String.format("The database directory '%s' already exist and '-o' setting is false", settings.databaseDirectory));

      FileUtils.deleteRecursively(databaseDirectory);
    }

    if (!databaseDirectory.mkdirs())
      throw new RestoreException(String.format("Error on restoring database: the database directory '%s' cannot be created", settings.databaseDirectory));

    logger.logLine(0, "Executing full restore of database from file '%s' to '%s'...", settings.inputFileURL, settings.databaseDirectory);

    try (ZipInputStream zipFile = new ZipInputStream(inputSource.inputStream, DatabaseFactory.getDefaultCharset())) {
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
          FileUtils.getSizeAsString((inputSource.fileSize)), databaseOrigSize > 0 ? (databaseOrigSize - inputSource.fileSize) * 100 / databaseOrigSize : 0);
    }
  }

  private long uncompressFile(final ZipInputStream inputFile, ZipEntry compressedFile, final File databaseDirectory) throws IOException {
    final String fileName = compressedFile.getName();

    FileUtils.checkValidName(fileName);

    logger.log(2, "- File '%s'...", fileName);

    final File uncompressedFile = new File(databaseDirectory, fileName);

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

  private RestoreInputSource openInputFile() throws IOException {
    if (settings.inputFileURL.startsWith("http://") || settings.inputFileURL.startsWith("https://")) {
      final HttpURLConnection connection = (HttpURLConnection) new URL(settings.inputFileURL).openConnection();
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
      connection.connect();

      return new RestoreInputSource(connection.getInputStream(), 0);
    }

    String path = settings.inputFileURL;
    if (path.startsWith("file://")) {
      path = path.substring("file://".length());
    } else if (path.startsWith("classpath://"))
      path = getClass().getClassLoader().getResource(path.substring("classpath://".length())).getFile();

    final File file = new File(path);
    if (!file.exists())
      throw new RestoreException(String.format("The backup file '%s' not exist", settings.inputFileURL));

    return new RestoreInputSource(new FileInputStream(file), file.length());
  }
}
