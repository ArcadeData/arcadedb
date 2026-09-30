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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Collects the server log lines of a time window into a zip file, redacting them on the way.
 * <p>
 * <b>Where the log is</b>: the file pattern of the JUL {@code FileHandler} the server was started with
 * ({@code arcadedb-log.properties}, by default {@code ${arcadedb.server.logsDirectory}/arcadedb.log}, with the rotated
 * files {@code arcadedb.log.1...}); every file of that directory that starts with the file name of the pattern is a
 * candidate, plain or gzipped (recognised by its content, not its name), and a file last modified before the window cannot
 * hold a line of it and is skipped.
 * <p>
 * <b>Timestamps and time zones</b>: {@code LogFormatter} writes {@code yyyy-MM-dd HH:mm:ss.SSS} in the time zone of the JVM
 * ({@code LocalDateTime.now()}), with no zone in the line. The window is converted to that zone before comparing, and
 * ISO-8601 timestamps that do carry an offset ({@code 2026-09-30T12:00:00.123Z}) are converted to it too. A line without a
 * timestamp (a stack trace, a wrapped message, a blank line) belongs to the entry before it: it is kept or dropped with it.
 * <p>
 * <b>Size</b>: the zip is written to a file, never held in the heap. When it grows over the cap the collection stops with an
 * error asking to narrow the window: nothing is ever truncated silently.
 * <p>
 * <b>Long lines</b>: a line is cut to {@link #MAX_LINE_CHARS} characters when it is read (see {@link SupportLineReader}), and the
 * redactor runs on the cut line, so a line longer than that is never held whole in the heap. A {@code password=...} that starts
 * before the cut is still masked (the value is cut with the line); a keyword split exactly at the boundary is kept as a
 * fragment, which names nothing usable. The preview warns with the number of lines cut.
 */
public class SupportLogCollector {
  public static final long DEFAULT_MAX_ZIP_BYTES = 100L * 1024 * 1024;

  private static final String DEFAULT_LOG_FILE = "arcadedb.log";
  private static final int    CHECK_EVERY      = 200;
  /** A log line longer than this is truncated when read: one pathological line must not fill the heap. */
  static final         int    MAX_LINE_CHARS   = 1 << 16;

  private final ZoneId zone;

  /** What went in the zip for one log file. */
  public record FileStat(String name, long sizeBytes, long lines, int redactions) {
    public JSONObject toJSON() {
      return new JSONObject().put("name", name).put("sizeBytes", sizeBytes).put("lines", lines).put("redactions", redactions);
    }
  }

  public static final class Result {
    private final List<FileStat> files;
    private final List<String>   warnings;
    private final long           lines;
    private final int            redactions;
    private final long           zipBytes;
    private final JSONObject     summary;

    Result(final List<FileStat> files, final List<String> warnings, final long lines, final int redactions, final long zipBytes,
        final JSONObject summary) {
      this.files = files;
      this.warnings = warnings;
      this.lines = lines;
      this.redactions = redactions;
      this.zipBytes = zipBytes;
      this.summary = summary;
    }

    public List<FileStat> getFiles() {
      return files;
    }

    public List<String> getWarnings() {
      return warnings;
    }

    /** Lines in the window, all files. Zero means the window is empty: no zip was written. */
    public long getLines() {
      return lines;
    }

    public int getRedactions() {
      return redactions;
    }

    public long getZipBytes() {
      return zipBytes;
    }

    public JSONObject getSummary() {
      return summary;
    }

    public boolean isEmpty() {
      return lines == 0;
    }
  }

  /** @param zone the time zone the log timestamps are written in: the JVM zone of the server */
  public SupportLogCollector(final ZoneId zone) {
    this.zone = zone;
  }

  public ZoneId getZone() {
    return zone;
  }

  /** The log files of this server, oldest first. */
  public static List<Path> locate(final ContextConfiguration configuration) {
    Path directory;
    String stem = DEFAULT_LOG_FILE;

    // Fully qualified: the ArcadeDB LogManager (used elsewhere in this class) has the same simple name
    final String pattern = java.util.logging.LogManager.getLogManager().getProperty("java.util.logging.FileHandler.pattern");
    if (pattern != null && !pattern.isBlank()) {
      final String expanded = pattern.replace("%%", "\u0000").replace("%t", System.getProperty("java.io.tmpdir", "/tmp"))
          .replace("%h", System.getProperty("user.home", "/")).replace("\u0000", "%");
      final Path path = Paths.get(expanded);
      directory = path.getParent() != null ? path.getParent() : Paths.get(".");
      final String name = path.getFileName().toString();
      final int percent = name.indexOf('%');
      stem = percent > 0 ? name.substring(0, percent) : name;
    } else {
      final String configured = configuration.getValueAsString(GlobalConfiguration.SERVER_LOGS_DIRECTORY);
      directory = Paths.get(configured == null || configured.isBlank() ? "./log" : configured);
    }
    return locate(directory, stem);
  }

  /** Every file of {@code directory} whose name starts with {@code stem}, oldest first. */
  public static List<Path> locate(final Path directory, final String stem) {
    if (!Files.isDirectory(directory))
      return List.of();
    try (final Stream<Path> stream = Files.list(directory)) {
      final List<Path> result = new ArrayList<>(stream.filter(Files::isRegularFile).filter(p -> isLogFile(p, stem)).toList());
      result.sort(Comparator.comparingLong(SupportLogCollector::lastModified).thenComparing(p -> p.getFileName().toString()));
      return result;
    } catch (final IOException e) {
      LogManager.instance().log(SupportLogCollector.class, Level.WARNING, "Cannot list the log directory '%s'", e, directory);
      return List.of();
    }
  }

  private static boolean isLogFile(final Path path, final String stem) {
    final String name = path.getFileName().toString();
    if (!name.startsWith(stem))
      return false;
    final String lower = name.toLowerCase(Locale.ROOT);
    return !(lower.endsWith(".lck") || lower.endsWith(".hprof") || lower.endsWith(".tmp") || lower.endsWith(".jfr"));
  }

  private static long lastModified(final Path path) {
    try {
      return Files.getLastModifiedTime(path).toMillis();
    } catch (final IOException e) {
      return 0L;
    }
  }

  /**
   * Writes the lines of the window into {@code zipFile}: one zip entry per log file, named like the file (without
   * {@code .gz}), redacted.
   *
   * @return what was collected; when {@link Result#isEmpty()} no zip file exists
   *
   * @throws SupportException {@code bundle_too_large} when the zip would exceed {@code maxZipBytes}
   */
  public Result collect(final List<Path> files, final SupportLogWindow window, final Path zipFile, final long maxZipBytes)
      throws IOException {
    final long fromKey = toKey(LocalDateTime.ofInstant(window.from(), zone));
    final long toKey = toKey(LocalDateTime.ofInstant(window.to(), zone));
    // A file last modified before the window cannot hold a line of it (one minute of slack for clock granularity)
    final long oldestModified = window.from().toEpochMilli() - 60_000L;

    final List<FileStat> stats = new ArrayList<>();
    final List<String> warnings = new ArrayList<>();
    final SupportSummaryBuilder summary = new SupportSummaryBuilder(zone);
    long totalLines = 0;
    int totalRedactions = 0;
    int skipped = 0;
    final int[] truncated = new int[1];

    boolean success = false;
    try (final CountingOutputStream counting = new CountingOutputStream(new BufferedOutputStream(Files.newOutputStream(zipFile)));
        final ZipOutputStream zip = new ZipOutputStream(counting)) {
      zip.setLevel(6);

      for (final Path file : files) {
        if (lastModified(file) < oldestModified) {
          skipped++;
          continue;
        }

        final String entryName = entryName(file);
        final SupportRedactor.Session session = new SupportRedactor.Session();
        long fileLines = 0;
        long fileBytes = 0;
        boolean entryOpen = false;
        boolean include = false;
        long sinceCheck = 0;

        try (final InputStreamReader input = new InputStreamReader(open(file), StandardCharsets.UTF_8)) {
          final SupportLineReader reader = new SupportLineReader(input, MAX_LINE_CHARS, truncated);
          String line;
          while ((line = reader.readLine()) != null) {
            final long key = parseKey(line, zone);
            if (key >= 0) {
              session.resetBlock();
              if (key > toKey)
                break;
              include = key >= fromKey;
              if (include && !entryOpen) {
                zip.putNextEntry(new ZipEntry(entryName));
                entryOpen = true;
              }
              if (!include)
                continue;
              final String redacted = session.redactLine(line);
              fileLines++;
              summary.beginEntry(key, parseLevel(line));
              if (redacted != null)
                fileBytes += write(zip, redacted);
            } else if (include) {
              // A continuation of the entry (stack trace, wrapped message, blank line)
              final String redacted = session.redactLine(line);
              fileLines++;
              if (redacted != null) {
                fileBytes += write(zip, redacted);
                summary.continuation(redacted);
              } else
                summary.continuation("");
            }

            if (++sinceCheck >= CHECK_EVERY) {
              sinceCheck = 0;
              checkCap(counting, maxZipBytes);
            }
          }
        } catch (final IOException e) {
          // A file being rotated, truncated or not gzip after all: what was read is kept, and the user is told
          warnings.add("Could not read all of '" + file.getFileName() + "' (" + e.getClass().getSimpleName() + "): the collected part"
              + " of this file may be incomplete");
          LogManager.instance().log(this, Level.WARNING, "Support: cannot read the log file '%s'", e, file);
        }

        if (entryOpen) {
          zip.closeEntry();
          stats.add(new FileStat(entryName, fileBytes, fileLines, session.getCount()));
          totalLines += fileLines;
          totalRedactions += session.getCount();
        }
        checkCap(counting, maxZipBytes);
      }

      zip.finish();
      zip.flush();
      checkCap(counting, maxZipBytes);
      success = true;
    } finally {
      if (!success || totalLines == 0)
        Files.deleteIfExists(zipFile);
    }

    if (skipped > 0)
      warnings.add(skipped + " log file(s) skipped: last modified before the start of the window");
    if (truncated[0] > 0)
      warnings.add(truncated[0] + " very long log line(s) were cut to " + MAX_LINE_CHARS + " characters");
    if (totalLines == 0)
      warnings.add("No log lines in the selected window (" + files.size() + " log file(s) examined, time zone of the log: " + zone
          + "). Widen the window or send the diagnostics only");

    final long zipBytes = totalLines == 0 ? 0 : Files.size(zipFile);
    if (zipBytes > maxZipBytes) {
      Files.deleteIfExists(zipFile);
      throw tooLarge(maxZipBytes);
    }
    return new Result(stats, warnings, totalLines, totalRedactions, zipBytes, summary.finish(window));
  }

  private static void checkCap(final CountingOutputStream counting, final long maxZipBytes) {
    if (counting.count > maxZipBytes)
      throw tooLarge(maxZipBytes);
  }

  private static SupportException tooLarge(final long maxZipBytes) {
    return new SupportException("bundle_too_large", "The logs of this window are larger than the limit of " + (maxZipBytes >> 20)
        + " MB (zipped). Narrow the time window: nothing was truncated and nothing was sent");
  }

  private static long write(final ZipOutputStream zip, final String line) throws IOException {
    final byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
    zip.write(bytes);
    zip.write('\n');
    return bytes.length + 1L;
  }

  private static String entryName(final Path file) {
    final String name = file.getFileName().toString();
    return name.toLowerCase(Locale.ROOT).endsWith(".gz") ? name.substring(0, name.length() - 3) : name;
  }

  /** Opens the file, un-gzipping it when it is gzip (magic number 1f 8b), whatever its name. */
  static InputStream open(final Path file) throws IOException {
    final InputStream raw = new BufferedInputStream(Files.newInputStream(file), 1 << 16);
    try {
      raw.mark(2);
      final int b1 = raw.read();
      final int b2 = raw.read();
      raw.reset();
      if (b1 == 0x1f && b2 == 0x8b)
        return new GZIPInputStream(raw, 1 << 16);
      return raw;
    } catch (final IOException e) {
      raw.close();
      throw e;
    }
  }

  // ---------------------------------------------------------------------------------------------- timestamps

  /** {@code yyyyMMddHHmmssSSS} as a number: compares like the time, allocates nothing. */
  public static long toKey(final LocalDateTime t) {
    return ((((((t.getYear() * 100L + t.getMonthValue()) * 100 + t.getDayOfMonth()) * 100 + t.getHour()) * 100 + t.getMinute()) * 100
        + t.getSecond()) * 1000) + t.getNano() / 1_000_000;
  }

  public static LocalDateTime fromKey(final long key) {
    long k = key;
    final int millis = (int) (k % 1000);
    k /= 1000;
    final int second = (int) (k % 100);
    k /= 100;
    final int minute = (int) (k % 100);
    k /= 100;
    final int hour = (int) (k % 100);
    k /= 100;
    final int day = (int) (k % 100);
    k /= 100;
    final int month = (int) (k % 100);
    k /= 100;
    return LocalDateTime.of((int) k, month, day, hour, minute, Math.min(second, 59), millis * 1_000_000);
  }

  /**
   * The timestamp at the start of a log line as a {@link #toKey key} in {@code zone}, or -1 when the line has none.
   * Accepts {@code yyyy-MM-dd HH:mm:ss[.SSS]} and the ISO-8601 form ({@code T}, optional {@code Z} or {@code +hh:mm}).
   */
  static long parseKey(final String line, final ZoneId zone) {
    final int n = line.length();
    if (n < 19 || line.charAt(4) != '-' || line.charAt(7) != '-' || (line.charAt(10) != ' ' && line.charAt(10) != 'T')
        || line.charAt(13) != ':' || line.charAt(16) != ':')
      return -1;

    final int year = digits(line, 0, 4);
    final int month = digits(line, 5, 2);
    final int day = digits(line, 8, 2);
    final int hour = digits(line, 11, 2);
    final int minute = digits(line, 14, 2);
    final int second = digits(line, 17, 2);
    if (year < 0 || month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0
        || second > 60)
      return -1;

    int pos = 19;
    int millis = 0;
    if (pos < n && (line.charAt(pos) == '.' || line.charAt(pos) == ',')) {
      pos++;
      int digits = 0;
      while (pos < n && Character.isDigit(line.charAt(pos))) {
        if (digits < 3)
          millis = millis * 10 + (line.charAt(pos) - '0');
        digits++;
        pos++;
      }
      if (digits == 0)
        return -1;
      for (int i = digits; i < 3; i++)
        millis *= 10;
    }

    final long key = ((((((year * 100L + month) * 100 + day) * 100 + hour) * 100 + minute) * 100 + Math.min(second, 59)) * 1000) + millis;

    if (pos < n) {
      final char c = line.charAt(pos);
      ZoneOffset offset = null;
      if (c == 'Z')
        offset = ZoneOffset.UTC;
      else if ((c == '+' || c == '-') && n >= pos + 3 && digits(line, pos + 1, 2) >= 0) {
        final int oh = digits(line, pos + 1, 2);
        int om = 0;
        if (n >= pos + 5 && line.charAt(pos + 3) == ':' && digits(line, pos + 4, 2) >= 0)
          om = digits(line, pos + 4, 2);
        else if (n >= pos + 5 && digits(line, pos + 3, 2) >= 0)
          om = digits(line, pos + 3, 2);
        try {
          offset = ZoneOffset.ofHoursMinutes(c == '-' ? -oh : oh, c == '-' ? -om : om);
        } catch (final RuntimeException e) {
          offset = null;
        }
      }
      if (offset != null)
        try {
          final Instant instant = OffsetDateTime.of(fromKey(key), offset).toInstant();
          return toKey(LocalDateTime.ofInstant(instant, zone));
        } catch (final RuntimeException e) {
          return -1;
        }
    }
    return key;
  }

  private static int digits(final String s, final int from, final int count) {
    int value = 0;
    for (int i = from; i < from + count; i++) {
      final char c = s.charAt(i);
      if (c < '0' || c > '9')
        return -1;
      value = value * 10 + (c - '0');
    }
    return value;
  }

  /** The level after the timestamp, normalised: the log writes it on five characters ({@code SEVER}, {@code WARNI}). */
  static String parseLevel(final String line) {
    int pos = 19;
    final int n = line.length();
    while (pos < n && line.charAt(pos) != ' ')
      pos++;
    while (pos < n && line.charAt(pos) == ' ')
      pos++;
    final int start = pos;
    while (pos < n && pos - start < 8 && Character.isLetter(line.charAt(pos)))
      pos++;
    if (pos - start < 3)
      return null;
    final String level = line.substring(start, pos).toUpperCase(Locale.ROOT);
    return switch (level) {
      case "SEVER", "SEVERE", "ERROR", "ERR" -> "SEVERE";
      case "WARNI", "WARNING", "WARN" -> "WARNING";
      case "CONFI", "CONFIG" -> "CONFIG";
      case "FINES", "FINEST" -> "FINEST";
      case "FINER" -> "FINER";
      case "FINE" -> "FINE";
      case "INFO" -> "INFO";
      case "DEBUG", "TRACE" -> level;
      default -> null;
    };
  }

  /** Counts the bytes that reach the file. */
  private static final class CountingOutputStream extends OutputStream {
    private final OutputStream delegate;
    long count;

    CountingOutputStream(final OutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(final int b) throws IOException {
      delegate.write(b);
      count++;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      delegate.write(b, off, len);
      count += len;
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
