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
package com.arcadedb.engine;

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.zip.CRC32;

public class ComponentFile {
  public enum MODE {
    READ_ONLY, READ_WRITE
  }

  protected final MODE    mode;
  protected       String  filePath;
  protected       String  fileName;
  protected       File    osFile;
  protected       int     fileId;
  protected       int     version = 0;      // STARTING FROM 21.10.2 COMPONENTS HAVE VERSION IN THE FILE NAME
  protected       String  componentName;
  protected       String  fileExtension;
  /**
   * Volatile because it is read without any of this file's locks by the async flush thread while {@code close()}
   * writes it under the channel write lock. That was already a race before issue #7363; what makes the ordering
   * matter now is that a reader which observes the close of a dropped file has to observe the drop as well - see
   * {@code dropped} below, written first, so the pair reads consistently in that direction.
   */
  protected volatile boolean open;
  /**
   * Set by {@link #drop()} <b>before</b> the file is closed, and never cleared: this file has been deliberately
   * removed (an index compaction replacing a sub-index, a bucket or index drop), so anything still addressed to it
   * is superseded rather than lost.
   * <p>
   * The ordering is the whole point (issue #7363). The async flush thread checks {@link #isOpen()} and then writes,
   * and {@code close()} can run entirely between those two steps - which surfaced as
   * {@code IllegalArgumentException: Cannot write page N because the file '...' is closed} logged at SEVERE by
   * {@code PageManagerFlushThread}, on a path an operator watches for real corruption. Raising the flag first means
   * a writer that loses that race can still tell "the file went away under me" from "a live file failed to write",
   * whichever side of the window it observes.
   * <p>
   * Volatile rather than guarded: it is read by the flush thread without any of this file's locks, precisely
   * because the lock it would need is the one {@code close()} is holding. Written BEFORE {@code open} is cleared
   * and both being volatile, a reader that sees {@code open == false} is guaranteed to see this {@code true} as
   * well, which is what lets a writer decide "dropped" and "closed" from one consistent pair of reads.
   */
  protected volatile boolean dropped;

  public ComponentFile() {
    this.mode = MODE.READ_ONLY;
  }

  protected ComponentFile(final String filePath, final MODE mode) throws FileNotFoundException {
    this.mode = mode;
    open(filePath, mode);
  }

  public void close() {
  }

  public long getSize() throws IOException {
    return osFile.length();
  }

  protected void open(final String filePath, final MODE mode) throws FileNotFoundException {
    this.filePath = filePath;

    final int lastDotPos = filePath.lastIndexOf(".");
    String filePrefix = filePath.substring(0, lastDotPos);
    this.fileExtension = filePath.substring(lastDotPos + 1);

    final int versionPos = filePrefix.lastIndexOf(".");
    if (filePrefix.charAt(versionPos + 1) == 'v') {
      // STARTING FROM 21.10.2 COMPONENTS HAVE VERSION IN THE FILE NAME
      version = Integer.parseInt(filePrefix.substring(versionPos + 2));
      filePrefix = filePrefix.substring(0, versionPos);
    }

    final int fileIdPos = filePrefix.lastIndexOf(".");
    if (fileIdPos > -1) {
      fileId = Integer.parseInt(filePrefix.substring(fileIdPos + 1));
      final int pos = FileUtils.lastIndexOfSeparator(filePrefix);
      componentName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      final int pos = FileUtils.lastIndexOfSeparator(filePrefix);
      componentName = filePrefix.substring(pos + 1);
    }

    fileName = FileUtils.getFileNameFromPath(filePath);

    this.osFile = new File(filePath);
    this.open = true;
  }

  public void drop() throws IOException {
    markDropped();
    close();
    LogManager.instance().log(this, Level.FINE, "Deleting file %s (id=%d)...", null, filePath, fileId);
    Files.delete(Path.of(getFilePath()));
  }

  /**
   * Says this file is being removed. The one place the "before the close" half of the {@code dropped} invariant
   * lives, so that {@link FileManager#dropFile(int)} - which has to raise it before a DEFERRED drop that never
   * calls {@link #drop()} - states the same thing by calling this rather than by writing the field itself.
   */
  void markDropped() {
    // BEFORE close(), never after: see the field's note (issue #7363).
    dropped = true;
  }

  /**
   * @return {@code true} once {@link #drop()} has started removing this file. See the {@code dropped} field.
   */
  public boolean isDropped() {
    return dropped;
  }

  public String getFileName() {
    return fileName;
  }

  public boolean isOpen() {
    return open;
  }

  public String getFilePath() {
    return filePath;
  }

  public String getComponentName() {
    return componentName;
  }

  public String getFileExtension() {
    return fileExtension;
  }

  public int getFileId() {
    return fileId;
  }

  public File getOSFile() {
    return osFile;
  }

  public void setFileId(final int fileId) {
    this.fileId = fileId;
  }

  public int getVersion() {
    return version;
  }

  /**
   * The mode this file is open in, which selects the {@code RandomAccessFile} open string and nothing else.
   * <p>
   * Exposed (issue #6340) so that a component built on an <em>already-registered</em> file can take every file
   * property from that file rather than four from the file and one from a guess. The id, the page size and the
   * version already had accessors, and each of them is now taken from the file by the callers in that position
   * (issues #6283 and #6314); the mode was the only one left with nothing to read it back out of, which is why
   * {@code TimeSeriesTagDictionary}'s build-on-an-existing-file constructor had to hard-code {@code READ_WRITE}.
   * <p>
   * It is a property OF THE FILE and not of the component asking: {@link FileManager} opens every file it scans
   * with the database's own mode and {@link FileManager#getOrCreateFile} hands a registered file back instead of
   * reopening it, so this is the mode a second view is going to get whatever it asks for - which is precisely why
   * that method now refuses to hand back a file open in a different one.
   */
  public MODE getMode() {
    return mode;
  }

  public long calculateChecksum() throws IOException {
    final CRC32 crc = new CRC32();
    final String fileContent = FileUtils.readFileAsString(osFile);
    crc.update(fileContent.getBytes());
    return crc.getValue();
  }

  @Override
  public String toString() {
    return filePath;
  }
}
