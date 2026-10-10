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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupportLogCollectorTest {
  private static final ZoneId ROME = ZoneId.of("Europe/Rome");
  private static final ZoneId UTC  = ZoneId.of("UTC");

  @TempDir
  Path dir;

  private static final String CURRENT = """

      2026-09-30 12:00:00.000 INFO  [Server] Started
      2026-09-30 12:05:00.100 SEVER [Http] Error on request password=hunter2
      java.lang.IllegalStateException: bad state token=abc123
      \tat com.arcadedb.Foo.bar(Foo.java:10)
      \tat com.arcadedb.Foo.baz(Foo.java:20)
      Caused by: java.io.IOException: disk
      \tat com.arcadedb.Io.read(Io.java:5)

      2026-09-30 12:06:00.000 WARNI [Http] slow query
      2026-09-30 12:10:00.000 INFO  [Server] Later
      """;

  private Path write(final String name, final String content, final Instant modified) throws IOException {
    final Path file = dir.resolve(name);
    Files.writeString(file, content, StandardCharsets.UTF_8);
    Files.setLastModifiedTime(file, FileTime.from(modified));
    return file;
  }

  private static List<String> read(final Path zip, final String entry) throws IOException {
    try (final ZipFile zf = new ZipFile(zip.toFile())) {
      final ZipEntry e = zf.getEntry(entry);
      assertThat(e).as("entry " + entry).isNotNull();
      try (final InputStream in = zf.getInputStream(e)) {
        return List.of(new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n"));
      }
    }
  }

  @Test
  void windowIsConvertedToTheZoneOfTheLog() throws Exception {
    final Path log = write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T10:11:00Z"));
    final Path zip = dir.resolve("out.zip");

    // 12:03 - 12:08 in Rome (UTC+2 in September) is 10:03Z - 10:08Z
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T10:03:00Z"), Instant.parse("2026-09-30T10:08:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(log), window, zip, 10_000_000);

    assertThat(result.isEmpty()).isFalse();
    final List<String> lines = read(zip, "arcadedb.log");
    assertThat(lines.get(0)).startsWith("2026-09-30 12:05:00.100 SEVER");
    assertThat(lines).anyMatch(l -> l.contains("WARNI [Http] slow query"));
    assertThat(lines).noneMatch(l -> l.contains("Started")).noneMatch(l -> l.contains("Later"));
    // the stack trace belongs to the entry before it, including the "Caused by" and the tab-indented frames
    assertThat(lines).anyMatch(l -> l.startsWith("java.lang.IllegalStateException"));
    assertThat(lines).anyMatch(l -> l.contains("at com.arcadedb.Io.read"));
  }

  @Test
  void theSameWindowInAnotherZoneSelectsOtherLines() throws Exception {
    final Path log = write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T12:11:00Z"));
    final Path zip = dir.resolve("out.zip");

    // The log was written in UTC this time: 12:03Z - 12:08Z contains the same wall-clock lines
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T12:03:00Z"), Instant.parse("2026-09-30T12:08:00Z"));
    new SupportLogCollector(UTC).collect(List.of(log), window, zip, 10_000_000);
    assertThat(read(zip, "arcadedb.log").get(0)).startsWith("2026-09-30 12:05:00.100");

    // Read as Rome wall-clock that window is 14:03 - 14:08: no line
    final Path zip2 = dir.resolve("out2.zip");
    final SupportLogCollector.Result none = new SupportLogCollector(ROME).collect(List.of(log), window, zip2, 10_000_000);
    assertThat(none.isEmpty()).isTrue();
    assertThat(zip2).doesNotExist();
    assertThat(none.getWarnings()).anyMatch(w -> w.contains("No log lines"));
  }

  @Test
  void secretsAreRedactedBeforeTheyAreWrittenAndCounted() throws Exception {
    final Path log = write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T10:11:00Z"));
    final Path zip = dir.resolve("out.zip");
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(log), window, zip, 10_000_000);

    final String content = String.join("\n", read(zip, "arcadedb.log"));
    assertThat(content).doesNotContain("hunter2").doesNotContain("abc123").contains("password=***").contains("token=***");
    assertThat(result.getRedactions()).isEqualTo(2);
    assertThat(result.getFiles()).hasSize(1);
    assertThat(result.getFiles().get(0).redactions()).isEqualTo(2);
    assertThat(result.getFiles().get(0).lines()).isEqualTo(result.getLines());
  }

  /** Issue #9625: a log line echoing the default databases setting reaches the bundle without the passwords. */
  @Test
  void defaultDatabasesPasswordsAreRedactedInCollectedLogs() throws Exception {
    final Path log = write("arcadedb.log", """

        2026-09-30 12:05:00.000 INFO  [Server] Starting with -Darcadedb.server.defaultDatabases=Universe[albert:einstein:admin];Beer[ada:lovelace]
        2026-09-30 12:06:00.000 INFO  [Server] Started
        """, Instant.parse("2026-09-30T10:11:00Z"));
    final Path zip = dir.resolve("out.zip");
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(log), window, zip, 10_000_000);

    final String content = String.join("\n", read(zip, "arcadedb.log"));
    assertThat(content).doesNotContain("einstein").doesNotContain("lovelace")
        .contains("defaultDatabases=Universe[albert:*****:admin];Beer[ada:*****]");
    assertThat(result.getRedactions()).isEqualTo(1);
  }

  @Test
  void rotatedAndGzippedFilesAreCollectedInOrder() throws Exception {
    write("arcadedb.log.1", "\n2026-09-30 09:00:00.000 INFO  [A] in rotated 1\n2026-09-30 09:30:00.000 INFO  [A] second in rotated 1\n",
        Instant.parse("2026-09-30T07:31:00Z"));
    // gzip file, with a name that does not say so for the second one: recognised by its content
    final Path gz = dir.resolve("arcadedb.log.2.gz");
    try (final OutputStream out = new GZIPOutputStream(Files.newOutputStream(gz))) {
      out.write("\n2026-09-30 08:00:00.000 INFO  [A] in gzip\n".getBytes(StandardCharsets.UTF_8));
    }
    Files.setLastModifiedTime(gz, FileTime.from(Instant.parse("2026-09-30T06:01:00Z")));
    final Path hidden = dir.resolve("arcadedb.log.3");
    try (final OutputStream out = new GZIPOutputStream(Files.newOutputStream(hidden))) {
      out.write("\n2026-09-30 07:00:00.000 INFO  [A] gzip without extension\n".getBytes(StandardCharsets.UTF_8));
    }
    Files.setLastModifiedTime(hidden, FileTime.from(Instant.parse("2026-09-30T05:01:00Z")));
    write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T10:11:00Z"));
    write("arcadedb.log.0.lck", "lock", Instant.parse("2026-09-30T10:11:00Z"));
    write("unrelated.txt", "\n2026-09-30 12:00:00.000 INFO  [A] not a log of the server\n", Instant.parse("2026-09-30T10:11:00Z"));

    final List<Path> files = SupportLogCollector.locate(dir, "arcadedb.log");
    assertThat(files).extracting(p -> p.getFileName().toString())
        .containsExactly("arcadedb.log.3", "arcadedb.log.2.gz", "arcadedb.log.1", "arcadedb.log");

    final Path zip = dir.resolve("out.zip");
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T04:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(files, window, zip, 10_000_000);

    assertThat(result.getFiles()).extracting(SupportLogCollector.FileStat::name)
        .containsExactly("arcadedb.log.3", "arcadedb.log.2", "arcadedb.log.1", "arcadedb.log");
    assertThat(read(zip, "arcadedb.log.2")).anyMatch(l -> l.contains("in gzip"));
    assertThat(read(zip, "arcadedb.log.3")).anyMatch(l -> l.contains("gzip without extension"));
    assertThat(read(zip, "arcadedb.log.1")).anyMatch(l -> l.contains("second in rotated 1"));
  }

  @Test
  void filesOlderThanTheWindowAreSkippedAndReported() throws Exception {
    final Path old = write("arcadedb.log.1", "\n2026-09-29 09:00:00.000 INFO  [A] old\n", Instant.parse("2026-09-29T07:00:00Z"));
    final Path log = write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T10:11:00Z"));
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(old, log), window, dir.resolve("o.zip"),
        10_000_000);
    assertThat(result.getFiles()).extracting(SupportLogCollector.FileStat::name).containsExactly("arcadedb.log");
    assertThat(result.getWarnings()).anyMatch(w -> w.contains("skipped"));
  }

  @Test
  void aPemBlockInALogIsReplacedByOneMarker() throws Exception {
    final Path log = write("arcadedb.log", """

        2026-09-30 12:00:00.000 INFO  [Ssl] Loaded key
        -----BEGIN PRIVATE KEY-----
        MIIEvQIBADANBgkqhkiG9w0BAQEFAASC
        -----END PRIVATE KEY-----
        2026-09-30 12:00:01.000 INFO  [Ssl] next
        """, Instant.parse("2026-09-30T10:11:00Z"));
    final Path zip = dir.resolve("out.zip");
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(log), window, zip, 10_000_000);
    final List<String> lines = read(zip, "arcadedb.log");
    assertThat(String.join("\n", lines)).doesNotContain("MIIEvQ").contains(SupportRedactor.PEM);
    assertThat(lines).anyMatch(l -> l.contains("[Ssl] next"));
    assertThat(result.getRedactions()).isEqualTo(1);
  }

  @Test
  void aZipOverTheCapStopsWithAClearErrorAndLeavesNothing() throws Exception {
    final Random random = new Random(42);
    final StringBuilder big = new StringBuilder("\n");
    for (int i = 0; i < 2000; i++) {
      final byte[] noise = new byte[200];
      random.nextBytes(noise);
      big.append("2026-09-30 12:00:").append(String.format("%02d", i % 60)).append(".000 INFO  [A] ")
          .append(java.util.Base64.getEncoder().encodeToString(noise)).append('\n');
    }
    final Path log = write("arcadedb.log", big.toString(), Instant.parse("2026-09-30T10:11:00Z"));
    final Path zip = dir.resolve("out.zip");
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));

    assertThatThrownBy(() -> new SupportLogCollector(ROME).collect(List.of(log), window, zip, 50_000))
        .isInstanceOfSatisfying(SupportException.class, e -> {
          assertThat(e.getCode()).isEqualTo("bundle_too_large");
          assertThat(e.getMessage()).contains("Narrow the time window");
          assertThat(e.getStudioStatus()).isEqualTo(413);
        });
    assertThat(zip).doesNotExist();
  }

  @Test
  void emptyWindowIsReportedNotAnError() throws Exception {
    final Path log = write("arcadedb.log", CURRENT, Instant.parse("2026-09-30T10:11:00Z"));
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(log), window, dir.resolve("o.zip"),
        10_000_000);
    assertThat(result.isEmpty()).isTrue();
    assertThat(result.getFiles()).isEmpty();
    assertThat(result.getWarnings()).isNotEmpty();
  }

  @Test
  void aMissingLogDirectoryGivesNoFiles() {
    assertThat(SupportLogCollector.locate(dir.resolve("nope"), "arcadedb.log")).isEmpty();
  }

  @Test
  void timestampParsing() {
    final long key = SupportLogCollector.toKey(java.time.LocalDateTime.of(2026, 9, 30, 12, 5, 0, 100_000_000));
    assertThat(SupportLogCollector.parseKey("2026-09-30 12:05:00.100 SEVER [x]", ROME)).isEqualTo(key);
    assertThat(SupportLogCollector.parseKey("2026-09-30T12:05:00.100 SEVER [x]", ROME)).isEqualTo(key);
    assertThat(SupportLogCollector.parseKey("2026-09-30 12:05:00,1 x", ROME)).isEqualTo(key);
    assertThat(SupportLogCollector.parseKey("2026-09-30 12:05:00 x", ROME)).isEqualTo(key - 100);
    // with an offset it is converted to the zone of the log: 10:05Z is 12:05 in Rome
    assertThat(SupportLogCollector.parseKey("2026-09-30T10:05:00.100Z INFO x", ROME)).isEqualTo(key);
    assertThat(SupportLogCollector.parseKey("2026-09-30T13:05:00.100+03:00 INFO x", ROME)).isEqualTo(key);
    assertThat(SupportLogCollector.parseKey("2026-09-30T13:05:00.100+0300 INFO x", ROME)).isEqualTo(key);
    // not timestamps
    assertThat(SupportLogCollector.parseKey("\tat com.arcadedb.Foo.bar(Foo.java:10)", ROME)).isEqualTo(-1);
    assertThat(SupportLogCollector.parseKey("", ROME)).isEqualTo(-1);
    assertThat(SupportLogCollector.parseKey("2026-13-30 12:05:00 x", ROME)).isEqualTo(-1);
    assertThat(SupportLogCollector.parseKey("Caused by: java.io.IOException: 2026-09-30 12:05:00", ROME)).isEqualTo(-1);
    assertThat(SupportLogCollector.fromKey(key)).isEqualTo(java.time.LocalDateTime.of(2026, 9, 30, 12, 5, 0, 100_000_000));
  }

  @Test
  void levelsAreNormalisedFromTheFiveCharacterColumn() {
    assertThat(SupportLogCollector.parseLevel("2026-09-30 12:05:00.100 SEVER [x] a")).isEqualTo("SEVERE");
    assertThat(SupportLogCollector.parseLevel("2026-09-30 12:05:00.100 WARNI [x] a")).isEqualTo("WARNING");
    assertThat(SupportLogCollector.parseLevel("2026-09-30 12:05:00.100 INFO  [x] a")).isEqualTo("INFO");
    assertThat(SupportLogCollector.parseLevel("2026-09-30 12:05:00.100 FINE  [x] a")).isEqualTo("FINE");
    assertThat(SupportLogCollector.parseLevel("2026-09-30T12:05:00.100Z ERROR x")).isEqualTo("SEVERE");
    assertThat(SupportLogCollector.parseLevel("2026-09-30 12:05:00.100 [x] a")).isNull();
  }

  @Test
  void summaryOfTheWindow() throws Exception {
    final String log = """

        2026-09-30 12:00:00.000 SEVER [A] first
        java.lang.IllegalStateException: bad state
        \tat a.B.c(B.java:1)
        2026-09-30 12:01:00.000 WARNI [A] w
        2026-09-30 12:02:00.000 SEVER [A] second
        java.lang.IllegalStateException: bad state
        \tat a.B.c(B.java:1)
        Caused by: java.io.IOException: inner
        2026-09-30 12:03:00.000 SEVER [A] third
        com.arcadedb.exception.X: other password=secret1
        2026-09-30 12:04:00.000 INFO  [A] fine
        """;
    final Path file = write("arcadedb.log", log, Instant.parse("2026-09-30T10:11:00Z"));
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(file), window, dir.resolve("o.zip"),
        10_000_000);

    final JSONObject summary = result.getSummary();
    assertThat(summary.getInt("schema")).isEqualTo(1);
    assertThat(summary.getLong("lines")).isEqualTo(result.getLines());
    assertThat(summary.getJSONObject("levels").getInt("SEVERE")).isEqualTo(3);
    assertThat(summary.getJSONObject("levels").getInt("WARNING")).isEqualTo(1);
    assertThat(summary.getJSONObject("levels").getInt("INFO")).isEqualTo(1);

    final JSONArray top = summary.getJSONArray("topExceptions");
    assertThat(top.length()).isEqualTo(2);
    final JSONObject first = top.getJSONObject(0);
    assertThat(first.getString("class")).isEqualTo("java.lang.IllegalStateException");
    assertThat(first.getString("message")).isEqualTo("bad state");
    assertThat(first.getInt("count")).isEqualTo(2);
    // 12:00 and 12:02 Rome time are 10:00Z and 10:02Z
    assertThat(first.getString("firstSeen")).isEqualTo("2026-09-30T10:00:00Z");
    assertThat(first.getString("lastSeen")).isEqualTo("2026-09-30T10:02:00Z");
    assertThat(first.getString("sampleStack")).contains("java.lang.IllegalStateException: bad state").contains("at a.B.c(B.java:1)");
    // the message is the redacted one
    assertThat(top.getJSONObject(1).getString("message")).isEqualTo("other password=***");
    assertThat(summary.toString()).doesNotContain("secret1");
  }

  @Test
  void sampleStackIsLimitedToFifteenLines() throws Exception {
    final StringBuilder log = new StringBuilder("\n2026-09-30 12:00:00.000 SEVER [A] x\njava.lang.RuntimeException: deep\n");
    for (int i = 0; i < 40; i++)
      log.append("\tat a.B.m").append(i).append("(B.java:").append(i).append(")\n");
    final Path file = write("arcadedb.log", log.toString(), Instant.parse("2026-09-30T10:11:00Z"));
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final JSONObject summary = new SupportLogCollector(ROME).collect(List.of(file), window, dir.resolve("o.zip"), 10_000_000).getSummary();
    final String stack = summary.getJSONArray("topExceptions").getJSONObject(0).getString("sampleStack");
    assertThat(stack.split("\n")).hasSize(15);
  }

  @Test
  void windowParsing() {
    final Instant now = Instant.parse("2026-09-30T12:00:00Z");
    assertThat(SupportLogWindow.preset("10m", now).from()).isEqualTo(now.minusSeconds(600));
    assertThat(SupportLogWindow.preset("1w", now).from()).isEqualTo(now.minusSeconds(7 * 86400));
    for (final String p : List.of("10m", "30m", "1h", "12h", "24h", "1w"))
      assertThat(SupportLogWindow.preset(p, now).to()).isEqualTo(now);
    assertThatThrownBy(() -> SupportLogWindow.preset("2h", now)).isInstanceOf(IllegalArgumentException.class);

    final SupportLogWindow custom = SupportLogWindow.parse(
        new JSONObject().put("from", "2026-09-30T10:00:00Z").put("to", "2026-09-30T14:00:00+02:00"), now, ROME);
    assertThat(custom.from()).isEqualTo(Instant.parse("2026-09-30T10:00:00Z"));
    assertThat(custom.to()).isEqualTo(Instant.parse("2026-09-30T12:00:00Z"));

    // no offset: the zone of the log
    final SupportLogWindow local = SupportLogWindow.parse(
        new JSONObject().put("from", "2026-09-30T10:00:00").put("to", "2026-09-30T11:00:00"), now, ROME);
    assertThat(local.from()).isEqualTo(Instant.parse("2026-09-30T08:00:00Z"));
    assertThat(local.to()).isEqualTo(Instant.parse("2026-09-30T09:00:00Z"));

    assertThatThrownBy(() -> SupportLogWindow.parse(new JSONObject().put("from", "2026-09-30T11:00:00Z").put("to", "2026-09-30T10:00:00Z"), now,
        ROME)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("after");
    assertThatThrownBy(() -> SupportLogWindow.parse(new JSONObject().put("from", "yesterday").put("to", "today"), now, ROME))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportLogWindow.parse(null, now, ROME)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportLogWindow.parse(new JSONObject().put("from", "2026-09-30T10:00:00Z"), now, ROME))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void collectedLinesKeepTheirOrderAcrossManyEntries() throws Exception {
    final List<String> expected = new ArrayList<>();
    final StringBuilder log = new StringBuilder("\n");
    for (int i = 0; i < 1000; i++) {
      final String line = "2026-09-30 12:%02d:%02d.000 INFO  [A] entry %d".formatted(i / 60, i % 60, i);
      log.append(line).append('\n');
      expected.add(line);
    }
    final Path file = write("arcadedb.log", log.toString(), Instant.parse("2026-09-30T10:30:00Z"));
    final SupportLogWindow window = new SupportLogWindow(Instant.parse("2026-09-30T09:00:00Z"), Instant.parse("2026-09-30T11:00:00Z"));
    final Path zip = dir.resolve("o.zip");
    final SupportLogCollector.Result result = new SupportLogCollector(ROME).collect(List.of(file), window, zip, 10_000_000);
    assertThat(result.getLines()).isEqualTo(1000);
    assertThat(read(zip, "arcadedb.log")).containsExactlyElementsOf(expected);
  }

  @Test
  void aVeryLongLineIsCutAndCountedInsteadOfFillingTheHeap() throws Exception {
    final int max = 16;
    final String text = "short\r\n" + "x".repeat(max + 500) + "\n\nlast\rmid\r\nend";
    final int[] truncated = new int[1];
    final SupportLineReader reader = new SupportLineReader(new java.io.StringReader(text), max, truncated);
    assertThat(reader.readLine()).isEqualTo("short");
    final String cut = reader.readLine();
    assertThat(cut).isEqualTo("x".repeat(max) + " ...[500 characters cut]");
    // an empty line is a line, and \r, \n and \r\n all terminate
    assertThat(reader.readLine()).isEmpty();
    assertThat(reader.readLine()).isEqualTo("last");
    assertThat(reader.readLine()).isEqualTo("mid");
    assertThat(reader.readLine()).isEqualTo("end");
    assertThat(reader.readLine()).isNull();
    assertThat(truncated[0]).isEqualTo(1);
  }

  @Test
  void theLineReaderAgreesWithBufferedReaderOnAnyInputAcrossChunkBoundaries() throws Exception {
    // Lines of every length around the 64K chunk size, with mixed terminators, read back the same as BufferedReader does
    final StringBuilder text = new StringBuilder();
    final String[] terminators = { "\n", "\r\n", "\r" };
    for (int i = 0; i < 60; i++) {
      text.append("L").append(i).append("-").append("y".repeat(65_530 + i)).append(terminators[i % 3]);
      text.append(i % 7 == 0 ? terminators[(i + 1) % 3] : "");
    }
    text.append("tail without terminator");
    final java.util.List<String> expected = new java.util.ArrayList<>();
    try (final java.io.BufferedReader buffered = new java.io.BufferedReader(new java.io.StringReader(text.toString()))) {
      String line;
      while ((line = buffered.readLine()) != null)
        expected.add(line);
    }
    final SupportLineReader reader = new SupportLineReader(new java.io.StringReader(text.toString()), Integer.MAX_VALUE, new int[1]);
    final java.util.List<String> actual = new java.util.ArrayList<>();
    String line;
    while ((line = reader.readLine()) != null)
      actual.add(line);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void onlyLogFilesAndTheirRotationsAreDiscoveredNotOtherFilesStartingWithTheStem(@org.junit.jupiter.api.io.TempDir final Path logs)
      throws Exception {
    for (final String name : new String[] { "arcadedb.log", "arcadedb.log.1", "arcadedb.log.2.gz", "arcadedb0.log", "arcadedb.log.lck",
        "arcadedb-notes.txt", "arcadedb.dump", "arcadedb-heap.hprof", "other.log" })
      Files.writeString(logs.resolve(name), "x");
    assertThat(SupportLogCollector.locate(logs, "arcadedb")).extracting(p -> p.getFileName().toString())
        .containsExactlyInAnyOrder("arcadedb.log", "arcadedb.log.1", "arcadedb.log.2.gz", "arcadedb0.log");
  }
}
