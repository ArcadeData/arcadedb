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

import java.io.*;
import java.nio.file.*;
import java.util.logging.*;
import java.util.zip.*;

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
  protected       boolean open;

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
      final int pos = filePrefix.lastIndexOf(File.separator);
      componentName = filePrefix.substring(pos + 1, filePrefix.lastIndexOf("."));
    } else {
      fileId = -1;
      final int pos = filePrefix.lastIndexOf(File.separator);
      componentName = filePrefix.substring(pos + 1);
    }

    final int lastSlash = filePath.lastIndexOf(File.separator);
    if (lastSlash > -1)
      fileName = filePath.substring(lastSlash + 1);
    else
      fileName = filePath;

    this.osFile = new File(filePath);
    this.open = true;
  }

  public void rename(final String newFileName) throws IOException {
    close();
    LogManager.instance().log(this, Level.FINE, "Renaming file %s (id=%d) to %s...", null, filePath, fileId, newFileName);
    final File newFile = new File(newFileName);
    new File(filePath).renameTo(newFile);
    open(newFile.getAbsolutePath(), mode);
  }

  public void drop() throws IOException {
    close();
    LogManager.instance().log(this, Level.FINE, "Deleting file %s (id=%d) to %s...", null, filePath, fileId);
    Files.delete(Paths.get(getFilePath()));
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
