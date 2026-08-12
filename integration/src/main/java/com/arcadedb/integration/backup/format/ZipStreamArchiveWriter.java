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

import com.arcadedb.integration.backup.IoThrottler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Historical single-threaded backup writer, a thin wrapper around {@code java.util.zip.ZipOutputStream}. Kept as the
 * escape hatch selected by {@code arcadedb.backup.compressionThreads = 0}: it is the exact code path every ArcadeDB
 * release before the parallel writer used, so an operator who hits a problem with the parallel one can fall back to a
 * writer with years of production mileage without changing anything else.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ZipStreamArchiveWriter implements BackupArchiveWriter {
  private static final int BUFFER_SIZE = 64 * 1024;

  private final ZipOutputStream zipStream;
  private final IoThrottler     throttler;
  private final byte[]          buffer = new byte[BUFFER_SIZE];

  public ZipStreamArchiveWriter(final OutputStream out, final int compressionLevel, final IoThrottler throttler) {
    // THE ZipOutputStream IS CLOSED, NOT JUST FINISHED, SO IT RELEASES ITS NATIVE Deflater. THE SHIELD KEEPS THAT
    // FROM PROPAGATING TO THE STREAM UNDERNEATH, WHICH THE CALLER OWNS
    this.zipStream = new ZipOutputStream(new NonClosingOutputStream(out), StandardCharsets.UTF_8);
    this.zipStream.setLevel(compressionLevel);
    this.throttler = throttler;
  }

  @Override
  public EntryStats addFile(final File inputFile) throws IOException {
    try (final FileInputStream fileIn = new FileInputStream(inputFile)) {
      return addEntry(inputFile.getName(), inputFile.lastModified(), fileIn);
    }
  }

  @Override
  public EntryStats addEntry(final String name, final long lastModified, final InputStream input) throws IOException {
    final ZipEntry zipEntry = new ZipEntry(name);
    if (lastModified > 0)
      zipEntry.setTime(lastModified);
    zipStream.putNextEntry(zipEntry);

    long uncompressedSize = 0L;
    try (final InputStream in = input) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        throttler.throttle(read);
        zipStream.write(buffer, 0, read);
        uncompressedSize += read;
      }
    }
    zipStream.closeEntry();

    return new EntryStats(uncompressedSize, zipEntry.getCompressedSize());
  }

  @Override
  public void close() throws IOException {
    zipStream.close();
  }

  @Override
  public void abort() {
    // DELIBERATELY DOES NOTHING. java.util.zip OFFERS NO WAY TO RELEASE A ZipOutputStream'S Deflater WITHOUT ALSO
    // FINISHING THE ARCHIVE, AND FINISHING IT IS EXACTLY WHAT MUST NOT HAPPEN HERE, SO THE STREAM IS LEFT
    // UNTERMINATED AND THE Deflater IS RECLAIMED BY ITS CLEANER. ONE DEFLATER PER FAILED BACKUP IS NOT A LEAK
    // WORTH TRADING A VALID-LOOKING TRUNCATED ARCHIVE FOR
  }

  private static final class NonClosingOutputStream extends OutputStream {
    private final OutputStream delegate;

    private NonClosingOutputStream(final OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(final int b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.flush();
    }
  }
}
