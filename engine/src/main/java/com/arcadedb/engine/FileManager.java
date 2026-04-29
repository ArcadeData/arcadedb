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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class FileManager {
  private final        ComponentFile.MODE                        mode;
  private final        List<ComponentFile>                       files           = new ArrayList<>();
  private final        ConcurrentHashMap<String, ComponentFile>  fileNameMap     = new ConcurrentHashMap<>();
  private final        ConcurrentHashMap<Integer, ComponentFile> fileIdMap       = new ConcurrentHashMap<>();
  private final        AtomicLong                                maxFilesOpened  = new AtomicLong();
  private              List<FileChange>                          recordedChanges = null;
  private final static PaginatedComponentFile                    RESERVED_SLOT   = new PaginatedComponentFile();

  public static class FileChange {
    public final boolean create;
    public final int     fileId;
    public final String  fileName;

    public FileChange(final boolean create, final int fileId, final String fileName) {
      this.create = create;
      this.fileId = fileId;
      this.fileName = fileName;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (!(o instanceof FileChange))
        return false;
      final FileChange that = (FileChange) o;
      return fileId == that.fileId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileId);
    }
  }

  public static class FileManagerStats {
    public long maxOpenFiles;
    public long totalOpenFiles;
  }

  public FileManager(final String path, final ComponentFile.MODE mode, final Set<String> supportedFileExt) {
    this(path, mode, supportedFileExt, null);
  }

  /**
   * @param path             primary database directory; created if missing.
   * @param extraScanPath    optional secondary directory to scan for additional component files (e.g. paired
   *                         external-property buckets that have been tiered to a different disk via
   *                         {@code arcadedb.externalPropertyBucketPath}). May be null/empty.
   */
  public FileManager(final String path, final ComponentFile.MODE mode, final Set<String> supportedFileExt,
      final String extraScanPath) {
    this.mode = mode;

    final File dbDirectory = new File(path);
    if (!dbDirectory.exists()) {
      boolean created = dbDirectory.mkdirs();
      if (!created) {
        LogManager.instance().log(this, Level.SEVERE, "Cannot create the directory '%s'", null, dbDirectory);
        throw new IllegalArgumentException(String.format("Cannot create the directory '%s'", dbDirectory));
      }
    } else {
      if (!dbDirectory.canRead()) {
        LogManager.instance().log(this, Level.SEVERE, "The directory '%s' doesn't have the proper permissions", null, dbDirectory);
        throw new IllegalArgumentException(String.format("The directory '%s' doesn't have the proper permissions", dbDirectory));
      }

      scanDirectoryForComponentFiles(dbDirectory, supportedFileExt);
    }

    if (extraScanPath != null && !extraScanPath.isEmpty()) {
      final File extraDir = new File(extraScanPath);
      if (extraDir.exists() && extraDir.canRead() && !extraDir.equals(dbDirectory))
        scanDirectoryForComponentFiles(extraDir, supportedFileExt);
    }
  }

  private void scanDirectoryForComponentFiles(final File dir, final Set<String> supportedFileExt) {
    final File[] entries = dir.listFiles();
    if (entries == null)
      return;
    for (final File f : entries) {
      // Compute the extension from the file name (not the full path) so a database directory containing dots
      // (e.g. /home/u/my.db/bucket1) doesn't accidentally find the dot in the directory name.
      final String fileName = f.getName();
      final int lastDot = fileName.lastIndexOf(".");
      if (lastDot < 0)
        continue;
      final String fileExt = fileName.substring(lastDot + 1);
      if (!supportedFileExt.contains(fileExt))
        continue;
      try {
        final ComponentFile file = new PaginatedComponentFile(f.getAbsolutePath(), mode);
        registerFile(file);
      } catch (final FileNotFoundException e) {
        LogManager.instance().log(this, Level.WARNING, "Cannot load file '%s'", null, f);
      }
    }
  }

  /**
   * Start recording changes in file system. Changes can be returned (before the end of the lock in database) with {@link #getRecordedChanges()}.
   *
   * @return true if the recorded started and false if it was already started.
   */
  public boolean startRecordingChanges() {
    if (recordedChanges != null)
      return false;

    recordedChanges = new ArrayList<>();
    return true;
  }

  public List<FileChange> getRecordedChanges() {
    return recordedChanges;
  }

  public void stopRecordingChanges() {
    recordedChanges = null;
  }

  public void close() {
    for (final ComponentFile f : fileNameMap.values())
      f.close();

    files.clear();
    fileNameMap.clear();
    fileIdMap.clear();
  }

  public void dropFile(final int fileId) throws IOException {
    final ComponentFile file = fileIdMap.remove(fileId);
    if (file != null) {
      fileNameMap.remove(file.getComponentName());
      files.set(fileId, null);
      file.drop();

      final FileChange entry = new FileChange(false, fileId, file.getFileName());
      if (recordedChanges != null) {
        if (recordedChanges.remove(entry))
          // JUST ADDED: REMOVE THE ENTRY
          return;

        recordedChanges.add(entry);
      }
    }
  }

  public FileManagerStats getStats() {
    final FileManagerStats stats = new FileManagerStats();
    stats.maxOpenFiles = maxFilesOpened.get();
    stats.totalOpenFiles = fileIdMap.size();
    return stats;
  }

  public List<ComponentFile> getFiles() {
    return Collections.unmodifiableList(files);
  }

  public boolean existsFile(final int fileId) {
    return fileIdMap.containsKey(fileId);
  }

  public ComponentFile getFile(final int fileId) {
    final ComponentFile f = fileIdMap.get(fileId);
    if (f == null)
      throw new IllegalArgumentException("File with id " + fileId + " was not found");

    return f;
  }

  public ComponentFile getOrCreateFile(final String fileName, final String filePath, final ComponentFile.MODE mode)
      throws IOException {
    ComponentFile file = fileNameMap.get(fileName);
    if (file != null)
      return file;

    file = new PaginatedComponentFile(filePath, mode);
    registerFile(file);

    if (recordedChanges != null)
      recordedChanges.add(new FileChange(true, file.getFileId(), file.getFileName()));

    return file;
  }

  public ComponentFile getOrCreateFile(final int fileId, final String filePath) throws IOException {
    ComponentFile file = fileIdMap.get(fileId);
    if (file == null) {
      file = new PaginatedComponentFile(filePath, mode);
      registerFile(file);

      if (recordedChanges != null)
        recordedChanges.add(new FileChange(true, file.getFileId(), file.getFileName()));
    }

    return file;
  }

  public synchronized int newFileId() {
//    // LOOK FOR AN HOLE
//    for (int i = 0; i < files.size(); ++i) {
//      if (files.get(i) == null) {
//        files.set(i, RESERVED_SLOT);
//        return i;
//      }
//    }
//
    files.add(RESERVED_SLOT);
    return files.size() - 1;
  }

  public void renameFile(final String oldName, final String newName) {
    final ComponentFile file = fileNameMap.remove(oldName);
    if (file == null)
      return;

    fileNameMap.put(newName, file);
  }

  private void registerFile(final ComponentFile file) {
    final int pos = file.getFileId();
    while (files.size() < pos + 1)
      files.add(null);
    final ComponentFile prev = files.get(pos);
    if (prev != null && prev != RESERVED_SLOT)
      throw new IllegalArgumentException(
          "Cannot register file '" + file + "' at position " + pos + " because already occupied by file '" + prev + "'");

    files.set(pos, file);
    fileNameMap.put(file.getComponentName(), file);
    fileIdMap.put(pos, file);
    maxFilesOpened.incrementAndGet();
  }

}
