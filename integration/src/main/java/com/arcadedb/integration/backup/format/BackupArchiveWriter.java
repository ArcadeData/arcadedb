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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Minimal contract a full-backup archive writer has to satisfy: append whole files as independent ZIP entries and, on
 * {@link #close()}, terminate the archive (central directory + end-of-central-directory record).
 * <p>
 * Two implementations exist and both emit the very same archive layout, so the restore path
 * ({@code FullRestoreFormat}, a plain {@code ZipInputStream} walk) and any standard unzip tool read either one:
 * {@link ZipStreamArchiveWriter} (the historical single-threaded {@code java.util.zip.ZipOutputStream}) and
 * {@link ParallelZipArchiveWriter} (chunked, multi-threaded deflate).
 * <p>
 * Neither implementation closes the underlying stream: the caller owns it, because on an encrypted backup it is a
 * {@code CipherOutputStream} whose lifecycle is tied to the file, not to the archive.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface BackupArchiveWriter extends Closeable {
  /**
   * Sizes of a single archive entry: what was read from disk and what was written into the archive for it.
   */
  record EntryStats(long uncompressedSize, long compressedSize) {
  }

  /**
   * Appends the whole content of {@code inputFile}, which must exist, as a new archive entry named after the file.
   */
  EntryStats addFile(File inputFile) throws IOException;

  /**
   * Terminates the archive. The underlying stream is left open for the caller to close.
   */
  @Override
  void close() throws IOException;
}
