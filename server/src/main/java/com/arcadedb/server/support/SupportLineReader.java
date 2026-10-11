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

import java.io.IOException;
import java.io.Reader;

/**
 * Reads lines like {@link java.io.BufferedReader#readLine()} (terminated by {@code \n}, {@code \r} or {@code \r\n}) but keeps
 * at most {@code maxLine} characters of each: the rest of an over-long line is read and dropped, and the line ends with a note
 * saying how much was cut. One pathological line in a log (hundreds of megabytes without a newline) therefore never fills the
 * heap.
 * <p>
 * It works on chunks of a {@code char[]}, scanning for the terminator, so it costs close to {@code readLine()} and far less
 * than reading one character at a time (every {@code Reader.read()} takes the reader's lock).
 * <p>
 * {@link #failingOver(Reader, int)} builds the strict variant for text a third party sends (the AI Assistant's streams, issue
 * #9627): there an over-long line is not a legitimate event to cut and carry on with, and a peer that never sends a terminator
 * would keep this reader dropping bytes for as long as it keeps sending. The strict reader stops at the cap with
 * {@link LineTooLongException} instead, having held at most {@code maxLine} characters plus one chunk.
 */
public final class SupportLineReader {
  private static final int CHUNK = 1 << 16;

  private final Reader  in;
  private final int     maxLine;
  private final char[]  buffer = new char[CHUNK];
  private       int     position;
  private       int     limit;
  private       boolean skipLineFeed;
  private final int[]   truncatedLines;
  private final boolean failOnOverlong;

  /** A line longer than the cap of a {@link #failingOver strict} reader. */
  public static final class LineTooLongException extends IOException {
    public LineTooLongException(final int maxLine) {
      super("A line longer than " + maxLine + " characters, which this server does not read");
    }
  }

  /** @param truncatedLines {@code truncatedLines[0]} is incremented for every line that was cut (shared across files) */
  SupportLineReader(final Reader in, final int maxLine, final int[] truncatedLines) {
    this(in, maxLine, truncatedLines, false);
  }

  private SupportLineReader(final Reader in, final int maxLine, final int[] truncatedLines, final boolean failOnOverlong) {
    this.in = in;
    this.maxLine = maxLine;
    this.truncatedLines = truncatedLines;
    this.failOnOverlong = failOnOverlong;
  }

  /**
   * A reader whose {@link #readLine()} throws {@link LineTooLongException} as soon as a line exceeds {@code maxLine} characters,
   * without reading the rest of it.
   */
  public static SupportLineReader failingOver(final Reader in, final int maxLine) {
    return new SupportLineReader(in, maxLine, new int[1], true);
  }

  /**
   * @return the next line without its terminator, or {@code null} at the end of the input
   * @throws LineTooLongException on a line longer than the cap, if this reader is {@link #failingOver strict}
   */
  public String readLine() throws IOException {
    StringBuilder line = null;
    long dropped = 0;
    boolean any = false;

    while (true) {
      if (position >= limit) {
        final int read = in.read(buffer, 0, buffer.length);
        if (read < 0)
          break;
        position = 0;
        limit = read;
        continue;
      }
      if (skipLineFeed) {
        skipLineFeed = false;
        // The \n of a \r\n pair belongs to the terminator already consumed
        if (buffer[position] == '\n') {
          position++;
          continue;
        }
      }

      final int start = position;
      while (position < limit && buffer[position] != '\n' && buffer[position] != '\r')
        position++;
      final int length = position - start;
      if (length > 0) {
        any = true;
        if (line == null)
          line = new StringBuilder(Math.min(length + 16, 256));
        final int room = maxLine - line.length();
        if (length > room && failOnOverlong)
          throw new LineTooLongException(maxLine);
        if (room > 0)
          line.append(buffer, start, Math.min(length, room));
        if (length > room)
          dropped += length - Math.max(room, 0);
      }

      if (position < limit) {
        // A terminator: the line is complete (an empty one is a line too)
        if (buffer[position++] == '\r')
          skipLineFeed = true;
        any = true;
        return finish(line, dropped);
      }
    }
    return any ? finish(line, dropped) : null;
  }

  private String finish(final StringBuilder line, final long dropped) {
    if (line == null)
      return "";
    if (dropped > 0) {
      line.append(" ...[").append(dropped).append(" characters cut]");
      truncatedLines[0]++;
    }
    return line.toString();
  }
}
