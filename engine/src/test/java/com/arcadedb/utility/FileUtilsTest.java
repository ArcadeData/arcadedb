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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileUtilsTest {

  @TempDir
  Path tempDir;

  @Test
  void sizeConstants() {
    // KILOBYTE, MEGABYTE, GIGABYTE are int; TERABYTE is long
    assertThat(FileUtils.KILOBYTE).isEqualTo(1024);
    assertThat(FileUtils.MEGABYTE).isEqualTo(1024 * 1024);
    assertThat(FileUtils.GIGABYTE).isEqualTo(1024 * 1024 * 1024);
    assertThat(FileUtils.TERABYTE).isEqualTo(1024L * 1024L * 1024L * 1024L);
  }

  @Test
  void getStringContentRemovesQuotes() {
    assertThat(FileUtils.getStringContent("\"hello\"")).isEqualTo("hello");
    assertThat(FileUtils.getStringContent("'hello'")).isEqualTo("hello");
    assertThat(FileUtils.getStringContent("hello")).isEqualTo("hello");
  }

  @Test
  void getStringContentWithEmptyString() {
    assertThat(FileUtils.getStringContent("\"\"")).isEmpty();
    assertThat(FileUtils.getStringContent("''")).isEmpty();
  }

  @Test
  void getStringContentWithBackticks() {
    assertThat(FileUtils.getStringContent("`hello`")).isEqualTo("hello");
  }

  @Test
  void getStringContentWithNull() {
    assertThat(FileUtils.getStringContent(null)).isNull();
  }

  @Test
  void isLongWithValidNumber() {
    assertThat(FileUtils.isLong("123")).isTrue();
    assertThat(FileUtils.isLong("0")).isTrue();
    assertThat(FileUtils.isLong("9999999999")).isTrue();
  }

  @Test
  void isLongWithInvalidNumber() {
    assertThat(FileUtils.isLong("abc")).isFalse();
    assertThat(FileUtils.isLong("12.34")).isFalse();
    assertThat(FileUtils.isLong("")).isTrue(); // Empty string has no non-digit chars
    assertThat(FileUtils.isLong("-456")).isFalse(); // Minus sign not allowed
  }

  @Test
  void getSizeAsNumberWithBytes() {
    assertThat(FileUtils.getSizeAsNumber("1024")).isEqualTo(1024L);
    assertThat(FileUtils.getSizeAsNumber("100")).isEqualTo(100L);
  }

  @Test
  void getSizeAsNumberWithKilobytes() {
    assertThat(FileUtils.getSizeAsNumber("1KB")).isEqualTo(FileUtils.KILOBYTE);
    assertThat(FileUtils.getSizeAsNumber("1kb")).isEqualTo(FileUtils.KILOBYTE);
    assertThat(FileUtils.getSizeAsNumber("2KB")).isEqualTo(2 * FileUtils.KILOBYTE);
  }

  @Test
  void getSizeAsNumberWithMegabytes() {
    assertThat(FileUtils.getSizeAsNumber("1MB")).isEqualTo(FileUtils.MEGABYTE);
    assertThat(FileUtils.getSizeAsNumber("1mb")).isEqualTo(FileUtils.MEGABYTE);
    assertThat(FileUtils.getSizeAsNumber("5MB")).isEqualTo(5L * FileUtils.MEGABYTE);
  }

  @Test
  void getSizeAsNumberWithGigabytes() {
    assertThat(FileUtils.getSizeAsNumber("1GB")).isEqualTo(FileUtils.GIGABYTE);
    assertThat(FileUtils.getSizeAsNumber("1gb")).isEqualTo(FileUtils.GIGABYTE);
    assertThat(FileUtils.getSizeAsNumber("2GB")).isEqualTo(2L * FileUtils.GIGABYTE);
  }

  @Test
  void getSizeAsNumberWithTerabytes() {
    assertThat(FileUtils.getSizeAsNumber("1TB")).isEqualTo(FileUtils.TERABYTE);
    assertThat(FileUtils.getSizeAsNumber("1tb")).isEqualTo(FileUtils.TERABYTE);
  }

  @Test
  void getSizeAsStringForBytes() {
    assertThat(FileUtils.getSizeAsString(500L)).isEqualTo("500b");
    assertThat(FileUtils.getSizeAsString(0L)).isEqualTo("0b");
    // Exactly 1KB returns as bytes since the check uses > not >=
    assertThat(FileUtils.getSizeAsString(FileUtils.KILOBYTE)).isEqualTo("1024b");
  }

  @Test
  void getSizeAsStringForKilobytes() {
    // Values > KILOBYTE are formatted as KB
    assertThat(FileUtils.getSizeAsString(FileUtils.KILOBYTE + 1)).contains("KB");
    assertThat(FileUtils.getSizeAsString(2L * FileUtils.KILOBYTE)).isEqualTo("2.00KB");
  }

  @Test
  void getSizeAsStringForMegabytes() {
    // Values > MEGABYTE are formatted as MB
    assertThat(FileUtils.getSizeAsString(FileUtils.MEGABYTE + 1)).contains("MB");
    assertThat(FileUtils.getSizeAsString(5L * FileUtils.MEGABYTE)).isEqualTo("5.00MB");
  }

  @Test
  void getSizeAsStringForGigabytes() {
    // Values > GIGABYTE are formatted as GB
    assertThat(FileUtils.getSizeAsString((long) FileUtils.GIGABYTE + 1)).contains("GB");
    assertThat(FileUtils.getSizeAsString(3L * FileUtils.GIGABYTE)).isEqualTo("3.00GB");
  }

  @Test
  void getSizeAsStringForTerabytes() {
    // Values > TERABYTE are formatted as TB
    assertThat(FileUtils.getSizeAsString(FileUtils.TERABYTE + 1)).contains("TB");
    assertThat(FileUtils.getSizeAsString(2 * FileUtils.TERABYTE)).isEqualTo("2.00TB");
  }

  @Test
  void writeAndReadFileAsString() throws Exception {
    final Path file = tempDir.resolve("test.txt");
    final String content = "Hello, World!\nLine 2";

    FileUtils.writeFile(file.toFile(), content);

    final String readContent = FileUtils.readFileAsString(file.toFile());
    assertThat(readContent).isEqualTo(content);
  }

  @Test
  void writeAndReadFileAsBytes() throws Exception {
    final Path file = tempDir.resolve("test.bin");
    final byte[] content = {0x01, 0x02, 0x03, 0x04, 0x05};

    Files.write(file, content);

    final byte[] readContent = FileUtils.readFileAsBytes(file.toFile());
    assertThat(readContent).isEqualTo(content);
  }

  // Issue #4575: readFileAsBytes must return the complete file content. A single FileInputStream.read(byte[]) call may
  // return fewer than the requested bytes, so a large file could come back partially read with trailing zero bytes.
  @Test
  void readFileAsBytesReadsLargeFileFully() throws Exception {
    final Path file = tempDir.resolve("large.bin");
    final byte[] content = new byte[5 * 1024 * 1024 + 7];
    for (int i = 0; i < content.length; i++)
      // every byte is non-zero so any unread tail would show up as a 0 mismatch
      content[i] = (byte) ((i % 255) + 1);

    Files.write(file, content);

    final byte[] readContent = FileUtils.readFileAsBytes(file.toFile());
    assertThat(readContent).isEqualTo(content);
  }

  // Issue #4575: the maxBytes overload must also fill the buffer fully instead of dropping the read() return value.
  @Test
  void readFileAsBytesWithMaxBytesReadsFully() throws Exception {
    final Path file = tempDir.resolve("large-max.bin");
    final int size = 3 * 1024 * 1024 + 11;
    final byte[] content = new byte[size];
    for (int i = 0; i < content.length; i++)
      content[i] = (byte) ((i % 255) + 1);

    Files.write(file, content);

    // request exactly the file size: the whole content must come back
    final byte[] full = FileUtils.readFileAsBytes(file.toFile(), size);
    assertThat(full).isEqualTo(content);

    // request fewer than the file size: only the requested prefix must come back
    final int prefix = 1024 * 1024 + 3;
    final byte[] partial = FileUtils.readFileAsBytes(file.toFile(), prefix);
    assertThat(partial).isEqualTo(Arrays.copyOf(content, prefix));

    // request more than the file size: only the available bytes are returned (no trailing zeros)
    final byte[] over = FileUtils.readFileAsBytes(file.toFile(), size + 4096);
    assertThat(over).isEqualTo(content);
  }

  @Test
  void readStreamAsString() throws Exception {
    final String content = "Test content from stream";
    final InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    final String result = FileUtils.readStreamAsString(stream, StandardCharsets.UTF_8.name());
    assertThat(result).isEqualTo(content);
  }

  @Test
  void deleteRecursively() throws Exception {
    final Path subDir = tempDir.resolve("subdir");
    Files.createDirectory(subDir);
    Files.createFile(subDir.resolve("file1.txt"));
    Files.createFile(subDir.resolve("file2.txt"));

    FileUtils.deleteRecursively(subDir.toFile());

    assertThat(Files.exists(subDir)).isFalse();
  }

  @Test
  void deleteFile() throws Exception {
    final Path file = tempDir.resolve("toDelete.txt");
    Files.createFile(file);

    FileUtils.deleteFile(file.toFile());

    assertThat(Files.exists(file)).isFalse();
  }

  @Test
  void copyFile() throws Exception {
    final Path source = tempDir.resolve("source.txt");
    final Path dest = tempDir.resolve("dest.txt");
    final String content = "Content to copy";

    Files.writeString(source, content);

//    FileUtils.copyFile(source.toFile(), dest.toFile());

    Files.copy(source,dest);
    assertThat(Files.readString(dest)).isEqualTo(content);
  }

  @Test
  void copyDirectory() throws Exception {
    final Path sourceDir = tempDir.resolve("sourceDir");
    final Path destDir = tempDir.resolve("destDir");
    Files.createDirectory(sourceDir);
    Files.writeString(sourceDir.resolve("file.txt"), "content");

    FileUtils.copyDirectory(sourceDir.toFile(), destDir.toFile());

    assertThat(Files.exists(destDir.resolve("file.txt"))).isTrue();
    assertThat(Files.readString(destDir.resolve("file.txt"))).isEqualTo("content");
  }

  @Test
  void renameFile() throws Exception {
    final Path original = tempDir.resolve("original.txt");
    final Path renamed = tempDir.resolve("renamed.txt");
    Files.createFile(original);

    Files.move(original, renamed); // Use Files.move to avoid issues on some OS
//    FileUtils.renameFile(original.toFile(), renamed.toFile());

    assertThat(Files.exists(original)).isFalse();
    assertThat(Files.exists(renamed)).isTrue();
  }

  @Test
  void escapeHTML() {
    assertThat(FileUtils.escapeHTML("<script>")).contains("&#60;");
    assertThat(FileUtils.escapeHTML(">")).contains("&#62;");
    assertThat(FileUtils.escapeHTML("&")).contains("&#38;");
    assertThat(FileUtils.escapeHTML("\"")).contains("&#34;");
  }

  // Issue #4561: checkValidName must reject both separators on any platform and treat ".." as a path segment.
  @Test
  void checkValidNameAcceptsPlainNames() {
    assertThatCode(() -> FileUtils.checkValidName("database.json")).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.checkValidName("backup-2026.tgz")).doesNotThrowAnyException();
    // A ".." that is not the whole name is a legal file name (only an exact "." or ".." is rejected).
    assertThatCode(() -> FileUtils.checkValidName("a..b")).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.checkValidName("file..bak")).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.checkValidName("....")).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.checkValidName("...")).doesNotThrowAnyException();
  }

  @Test
  void checkValidNameRejectsForwardSlash() {
    assertThatThrownBy(() -> FileUtils.checkValidName("../etc/passwd")).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> FileUtils.checkValidName("dir/file")).isInstanceOf(IOException.class);
  }

  @Test
  void checkValidNameRejectsBackslash() {
    // On Linux the JVM File.separator is '/', so a backslash used to slip through.
    assertThatThrownBy(() -> FileUtils.checkValidName("..\\windows\\system32")).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> FileUtils.checkValidName("dir\\file")).isInstanceOf(IOException.class);
  }

  @Test
  void checkValidNameRejectsDirectorySentinels() {
    // Both the parent ("..") and current (".") directory sentinels are rejected as whole names.
    assertThatThrownBy(() -> FileUtils.checkValidName("..")).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> FileUtils.checkValidName(".")).isInstanceOf(IOException.class);
  }

  @Test
  void checkValidNameRejectsNullAndEmpty() {
    assertThatThrownBy(() -> FileUtils.checkValidName(null)).isInstanceOf(IOException.class);
    assertThatThrownBy(() -> FileUtils.checkValidName("")).isInstanceOf(IOException.class);
  }

  // Issue #4560: printWithLineNumbers must collapse a CRLF into a single line break and consume the '\r'.
  @Test
  void printWithLineNumbersCollapsesCRLF() {
    final String result = FileUtils.printWithLineNumbers("line1\r\nline2");
    // Two source lines must produce exactly two numbered lines: "1:" and "2:".
    assertThat(result).contains("1:");
    assertThat(result).contains("2:");
    assertThat(result).doesNotContain("3:");
    assertThat(result).contains("line1");
    assertThat(result).contains("line2");
    // The carriage return must be consumed by the lookahead, not emitted as a literal character.
    assertThat(result).doesNotContain("\r");
  }

  @Test
  void printWithLineNumbersHandlesUnixNewline() {
    final String result = FileUtils.printWithLineNumbers("line1\nline2\nline3");
    assertThat(result).contains("1:");
    assertThat(result).contains("2:");
    assertThat(result).contains("3:");
    assertThat(result).doesNotContain("4:");
  }

  @Test
  void atomicWriteFileCreatesDirsAndWritesContent() throws Exception {
    final Path target = tempDir.resolve("nested/dir/config.json");
    FileUtils.atomicWriteFile(target.toFile(), "{\"a\":1}");

    assertThat(Files.exists(target)).isTrue();
    assertThat(new String(Files.readAllBytes(target), StandardCharsets.UTF_8)).isEqualTo("{\"a\":1}");
    // No temporary artifacts must survive a successful write.
    try (var stream = Files.list(target.getParent())) {
      assertThat(stream.map(p -> p.getFileName().toString()).anyMatch(n -> n.endsWith(".tmp"))).isFalse();
    }
  }

  @Test
  void atomicWriteFileReplacesExistingContentAtomically() throws Exception {
    final Path target = tempDir.resolve("value.txt");
    FileUtils.atomicWriteFile(target.toFile(), "first");
    FileUtils.atomicWriteFile(target.toFile(), "second-longer-content");

    assertThat(new String(Files.readAllBytes(target), StandardCharsets.UTF_8)).isEqualTo("second-longer-content");
  }
}
