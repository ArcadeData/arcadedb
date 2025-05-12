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
package com.arcadedb.utility;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.logging.Level;
import java.util.zip.GZIPOutputStream;

public class FileUtils {
  public static final int    KILOBYTE = 1024;
  public static final int    MEGABYTE = 1048576;
  public static final int    GIGABYTE = 1073741824;
  public static final long   TERABYTE = 1099511627776L;
  public static final String UTF8_BOM = "\uFEFF";

  public static String getStringContent(final Object iValue) {
    if (iValue == null)
      return null;

    final String s = iValue.toString();

    if (s == null)
      return null;

    if (s.length() > 1 && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\''
        || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))
      return s.substring(1, s.length() - 1);

    if (s.length() > 1 && (s.charAt(0) == '`' && s.charAt(s.length() - 1) == '`'))
      return s.substring(1, s.length() - 1);

    return s;
  }

  public static boolean isLong(final String text) {
    boolean isLong = true;
    final int size = text.length();
    for (int i = 0; i < size && isLong; i++) {
      final char c = text.charAt(i);
      isLong = isLong & ((c >= '0' && c <= '9'));
    }
    return isLong;
  }

  public static long getSizeAsNumber(final Object iSize) {
    if (iSize == null)
      throw new IllegalArgumentException("Size is null");

    if (iSize instanceof Number number)
      return number.longValue();

    String size = iSize.toString();

    boolean number = true;
    for (int i = size.length() - 1; i >= 0; --i) {
      final char c = size.charAt(i);
      if (!Character.isDigit(c)) {
        if (i > 0 || (c != '-' && c != '+'))
          number = false;
        break;
      }
    }

    if (number)
      return string2number(size).longValue();
    else {
      size = size.toUpperCase(Locale.ENGLISH);
      int pos = size.indexOf("KB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * KILOBYTE);

      pos = size.indexOf("MB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * MEGABYTE);

      pos = size.indexOf("GB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * GIGABYTE);

      pos = size.indexOf("TB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * TERABYTE);

      pos = size.indexOf('B');
      if (pos > -1)
        return (long) string2number(size.substring(0, pos)).floatValue();

      pos = size.indexOf('%');
      if (pos > -1)
        return (long) (-1 * string2number(size.substring(0, pos)).floatValue());

      // RE-THROW THE EXCEPTION
      throw new IllegalArgumentException("Size " + size + " has a unrecognizable format");
    }
  }

  public static Number string2number(final String iText) {
    if (iText.indexOf('.') > -1)
      return Double.parseDouble(iText);
    else
      return Long.parseLong(iText);
  }

  public static String getSizeAsString(final long iSize) {
    final long[] dividers = { TERABYTE, GIGABYTE, MEGABYTE, KILOBYTE };
    final String[] units = { "TB", "GB", "MB", "KB" };
    for (int i = 0; i < dividers.length; i++) {
      if (iSize > dividers[i]) {
        return String.format(Locale.ENGLISH, "%2.2f%s", (float) iSize / dividers[i], units[i]);
      }
    }
    return iSize + "b";
  }

  public static void checkValidName(final String iFileName) throws IOException {
    if (iFileName.contains("..") || iFileName.contains(File.separator))
      throw new IOException("Invalid file name '" + iFileName + "'");
  }

  public static void deleteRecursively(final File rootFile) {
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        if (rootFile.exists()) {
          if (rootFile.isDirectory()) {
            final File[] files = rootFile.listFiles();
            if (files != null) {
              for (final File f : files) {
                if (f.isFile()) {
                  Files.delete(Path.of(f.getAbsolutePath()));
                } else
                  deleteRecursively(f);
              }
            }
          }

          Files.delete(Path.of(rootFile.getAbsolutePath()));
        }

        break;

      } catch (final IOException e) {
        LogManager.instance().log(rootFile, Level.WARNING, "Cannot delete directory '%s'", e, rootFile);
      }
    }
  }

  public static void deleteFolderIfEmpty(final File dir) {
    if (dir != null && dir.listFiles() != null && dir.listFiles().length == 0) {
      deleteRecursively(dir);
    }
  }

  public static boolean deleteFile(final File file) {
    for (int attempt = 0; attempt < 3; attempt++) {
      try {
        if (file.exists())
          Files.delete(file.toPath());
        return true;
      } catch (final IOException e) {
//        if (System.getProperty("os.name").toLowerCase().contains("win")) {
//          // AVOID LOCKING UNDER WINDOWS
//          try {
//            LogManager.instance().log(file, Level.WARNING, "Cannot delete file '%s'. Forcing GC cleanup and try again (attempt=%d)", e, file, attempt);
//            System.gc();
//            Thread.sleep(1000);
//          } catch (Exception ex) {
//            // IGNORE IT
//          }
//        } else
        LogManager.instance().log(file, Level.WARNING, "Cannot delete file '%s'", e, file);
      }
    }
    return false;
  }

  public static void copyFile(final File source, final File destination) throws IOException {
    try (final FileInputStream fis = new FileInputStream(source); final FileOutputStream fos = new FileOutputStream(destination)) {
      final FileChannel sourceChannel = fis.getChannel();
      final FileChannel targetChannel = fos.getChannel();
      sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
    }
  }

  public static void copyDirectory(final File source, final File destination) throws IOException {
    if (!destination.exists())
      destination.mkdirs();

    for (final File f : source.listFiles()) {
      final File target = new File(destination.getAbsolutePath() + File.separator + f.getName());
      if (f.isFile())
        copyFile(f, target);
      else
        copyDirectory(f, target);
    }
  }

  public static boolean renameFile(final File from, final File to) throws IOException {
    final FileSystem fileSystem = FileSystems.getDefault();

    final Path fromPath = fileSystem.getPath(from.getAbsolutePath());
    final Path toPath = fileSystem.getPath(to.getAbsolutePath());
    Files.move(fromPath, toPath);

    return true;
  }

  public static String threadDump() {
    final StringBuilder dump = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (final ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }

  public static String readFileAsString(final File file, final String iCharset) throws IOException {
    try (final FileInputStream is = new FileInputStream(file)) {
      return readStreamAsString(is, iCharset, 0);
    }
  }

  public static String readFileAsString(final File file) throws IOException {
    return readFileAsString(file, "UTF8");
  }

  public static Binary readStreamAsBinary(final InputStream iStream) throws IOException {
    final Binary buffer = new Binary();

    final byte[] buf = new byte[1_000_000];
    int numRead;

    while ((numRead = iStream.read(buf)) != -1)
      buffer.putByteArray(buf, numRead);

    return buffer;
  }

  public static String readStreamAsString(final InputStream iStream) throws IOException {
    return readStreamAsString(iStream, "utf8", 0);
  }

  public static String readStreamAsString(final InputStream iStream, final String iCharset) throws IOException {
    return readStreamAsString(iStream, iCharset, 0);
  }

  public static String readStreamAsString(final InputStream iStream, final String iCharset, final long limit) throws IOException {
    final StringBuilder fileData = new StringBuilder(1000);
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(iStream, iCharset))) {
      final char[] buf = new char[1024];
      int numRead;

      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);

        if (fileData.length() == 0 && readData.startsWith(UTF8_BOM))
          // SKIP UTF-8 BOM IF ANY
          readData = readData.substring(1);

        if (limit > 0 && fileData.length() + readData.length() > limit) {
          // LIMIT REACHED
          fileData.append(readData, 0, (int) (limit - fileData.length()));
          break;
        } else
          fileData.append(readData);

        if (limit > 0 && fileData.length() >= limit)
          // LIMIT REACHED
          break;
      }
    }
    return fileData.toString();
  }

  public static void writeFile(final File iFile, final String iContent) throws IOException {
    try (final FileOutputStream fos = new FileOutputStream(iFile)) {
      writeContentToStream(fos, iContent);
    }
  }

  public static void writeContentToStream(final File file, final byte[] content) throws IOException {
    try (final FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(content);
    }
  }

  public static void appendContentToFile(final File file, final String content) throws IOException {
    try (final FileOutputStream fos = new FileOutputStream(file, true)) {
      fos.write(content.getBytes());
    }
  }

  public static void writeContentToStream(final OutputStream output, final String iContent) throws IOException {
    try (final OutputStreamWriter os = new OutputStreamWriter(output, DatabaseFactory.getDefaultCharset())) {
      writeContentToStream(os, iContent);
    }
  }

  public static void writeContentToStream(final OutputStreamWriter os, final String iContent) throws IOException {
    final BufferedWriter writer = new BufferedWriter(os);
    writer.write(iContent);
    writer.flush();
  }

  public static String encode(final String value, final String encoding) {
    try {
      return URLEncoder.encode(value, encoding);
    } catch (final UnsupportedEncodingException e) {
      LogManager.instance().log(FileUtils.class, Level.SEVERE, "Error on using encoding " + encoding, e);
      return value;
    }
  }

  public static void gzipFile(final File sourceFile, final File destFile) throws IOException {
    try (final FileInputStream fis = new FileInputStream(sourceFile);
        final FileOutputStream fos = new FileOutputStream(destFile);
        final GZIPOutputStream gzipOS = new GZIPOutputStream(fos)) {
      final byte[] buffer = new byte[1024 * 1024];
      int len;
      while ((len = fis.read(buffer)) != -1) {
        gzipOS.write(buffer, 0, len);
      }
    }
  }

  public static String escapeHTML(final String s) {
    final StringBuilder out = new StringBuilder(Math.max(16, s.length()));
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c > 127 || c == '"' || c == '\'' || c == '<' || c == '>' || c == '&') {
        out.append("&#");
        out.append((int) c);
        out.append(';');
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }

  public static String printWithLineNumbers(final String text) {
    // COUNT TOTAL LINES FIRST
    int totalLines = 1;
    int maxWidth = 0;
    int currWidth = 0;
    for (int i = 0; i < text.length(); i++) {
      final char current = text.charAt(i);
      if (current == '\n') {
        ++totalLines;

        if (currWidth > maxWidth)
          maxWidth = currWidth;

        currWidth = 0;
      } else
        ++currWidth;
    }

    if (currWidth > maxWidth)
      maxWidth = currWidth;

    final int maxLineDigits = String.valueOf(totalLines).length();

    final StringBuilder result = new StringBuilder();

    // PRINT /10
    for (int i = 0; i < maxLineDigits + 1; i++)
      result.append(" ");

    for (int i = 0; i < maxWidth; i++) {
      final String s = "" + i;
      final char unit = s.charAt(s.length() - 1);
      if (unit == '0') {
        final char decimal = s.length() > 1 ? s.charAt(s.length() - 2) : ' ';
        result.append(decimal);
      } else
        result.append(' ');
    }
    result.append("\n");

    // PRINT UNITS
    result.append(" ".repeat(maxLineDigits + 1));

    for (int i = 0; i < maxWidth; i++) {
      final String s = "" + i;
      final char unit = s.charAt(s.length() - 1);
      result.append(unit);
    }

    result.append(String.format("\n%-" + maxLineDigits + "d: ", 1));
    int line = 1;
    for (int i = 0; i < text.length(); i++) {
      final char current = text.charAt(i);
      final Character next = i + 1 < text.length() ? text.charAt(i) : null;
      if (current == '\n') {
        ++line;
        result.append(String.format("\n%-" + maxLineDigits + "d: ", line));
      } else if (current == '\r' && (next == null || next == '\n')) {
        ++line;
        result.append(String.format("\n%-" + maxLineDigits + "d: ", line));
        ++i;
      } else
        result.append(current);
    }

    return result.toString();
  }

  public static String decodeFromFile(final String line) {
    final StringBuilder decodedLine = new StringBuilder();
    boolean backslash = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '\\') {
        if (backslash) {
          backslash = false;
          decodedLine.append("\\\\\\");
          continue;
        } else
          backslash = true;
      }
      decodedLine.append(c);
    }
    return decodedLine.toString();
  }

  public static byte[] readInputStreamAsBytes(final InputStream fis) throws IOException {
    return fis.readAllBytes();
  }

  public static byte[] readFileAsBytes(final File file) throws IOException {
    final byte[] bytes = new byte[(int) file.length()];
    try (FileInputStream fis = new FileInputStream(file)) {
      fis.read(bytes);
    }
    return bytes;
  }

  public static byte[] readFileAsBytes(final File file, final int maxBytes) throws IOException {
    final byte[] bytes = new byte[maxBytes];
    try (FileInputStream fis = new FileInputStream(file)) {
      fis.read(bytes);
    }
    return bytes;
  }
}
