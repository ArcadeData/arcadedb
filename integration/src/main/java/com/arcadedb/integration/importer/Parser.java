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
package com.arcadedb.integration.importer;

import com.arcadedb.database.DatabaseFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

public class Parser {
  /**
   * What {@link #nextChar()} returns once the source is exhausted: {@link java.io.Reader#read()}'s {@code -1}
   * narrowed to a {@code char}. It is a Unicode non-character, so no source can legitimately carry it, but the
   * reliable test is {@link #isEndOfStream()} - the cast is what used to make "the source ended" indistinguishable
   * from "the source contains this" (issue #7494).
   */
  public static final char END_OF_STREAM = (char) -1;

  /**
   * {@link #peeked}'s "nothing is buffered" value. Distinct from {@code -1}, which {@link java.io.Reader#read()}
   * returns at the end of the stream and which {@link #peekChar()} has to be able to hand back more than once.
   */
  private static final int NOTHING_PEEKED = -2;

  /**
   * {@link LeadingCommentInputStream}'s "this byte was never read" marker. Distinct from {@code -1} (end of stream)
   * and from {@code 0} (a NUL byte), both of which {@link InputStream#read()} can legitimately return: a source
   * opening {@code '/'} NUL would otherwise have had its NUL read and never given back, which is silent data loss.
   */
  private static final int NOT_READ = -2;

  private final Source            source;
  private       InputStream       is;
  private       InputStreamReader reader;
  private final long              limit;
  private final AtomicLong        position = new AtomicLong();
  private final long              total;
  private       char              currentChar;
  private       int               peeked   = NOTHING_PEEKED;
  private       boolean           endOfStream;
  private       boolean           skipLeadingComments;
  private final boolean           compressed;

  /**
   * A parser whose {@link #getInputStream()} and {@link #getReader()} are positioned past the source's leading
   * comment block - what every import FORMAT wants. Content sniffing wants the opposite and builds one with
   * {@link #Parser(Source, long, boolean)}.
   */
  public Parser(final Source source, final long limit) throws IOException {
    this(source, limit, true);
  }

  /**
   * @param skipLeadingComments whether {@link #getInputStream()} and {@link #getReader()} drop the source's leading
   *                            {@code #} / {@code //} comment lines. False only for
   *                            {@link SourceDiscovery}'s content sniffing, which walks the comment block itself -
   *                            counting the lines and rewinding over them - and so must be handed the source whole
   *                            (issue #7490).
   */
  public Parser(final Source source, final long limit, final boolean skipLeadingComments) throws IOException {
    this.source = source;
    this.limit = limit;
    this.skipLeadingComments = skipLeadingComments;
    resetInput();

    this.compressed = source.compressed;
    this.total = source.totalSize;
  }

  public char getCurrentChar() {
    return currentChar;
  }

  /**
   * Whether the last {@link #nextChar()} hit the end of the source rather than reading a character. The flag
   * {@link #END_OF_STREAM} cannot be, since {@code nextChar()} narrows {@code -1} to a {@code char} and so reports
   * the end of the source as a character value (issue #7494).
   */
  public boolean isEndOfStream() {
    return endOfStream;
  }

  public char nextChar() throws IOException {
    final int read;
    if (peeked != NOTHING_PEEKED) {
      read = peeked;
      peeked = NOTHING_PEEKED;
    } else {
      position.incrementAndGet();
      read = reader.read();
    }

    endOfStream = read < 0;
    currentChar = (char) read;
    return currentChar;
  }

  /**
   * The character AFTER the current one, without advancing: {@link #getCurrentChar()} still answers what it
   * answered before, and the next {@link #nextChar()} returns this same character and makes it current. Repeated
   * calls return the same character.
   * <p>
   * The one-character lookahead content sniffing needs to tell a {@code //} comment line from a data line that
   * merely starts with a single {@code /}: reading the second character to find out and then discovering it is data
   * loses it, and {@link #reset()} is the only way back and rewinds the whole source (issue #7347).
   */
  public char peekChar() throws IOException {
    return (char) peek();
  }

  /**
   * {@link #peekChar()} without the narrowing cast, so the end of the source stays distinguishable from a character
   * value. Reads one character when nothing is buffered, which BLOCKS if the source has more to send but has not
   * sent it yet - the whole point of {@link #isAvailable()} being written on top of it (issue #7494).
   */
  private int peek() throws IOException {
    if (peeked == NOTHING_PEEKED) {
      position.incrementAndGet();
      peeked = reader.read();
    }
    return peeked;
  }

  public void reset() throws IOException {
    currentChar = 0;
    peeked = NOTHING_PEEKED;
    endOfStream = false;
    position.set(0);
    source.reset();
    resetInput();
  }

  /**
   * Whether {@link #getInputStream()} and {@link #getReader()} drop the source's leading comment block. Takes
   * effect at the next {@link #reset()}, which is where the stream is rebuilt: {@link SourceDiscovery#getSchema}
   * sniffs the source with it off and then turns it on for the {@code analyze()} call that follows the reset it
   * already does (issue #7490).
   */
  public void setSkipLeadingComments(final boolean skipLeadingComments) {
    this.skipLeadingComments = skipLeadingComments;
  }

  /**
   * Whether the source has a character left to read - NOT whether one can be read without blocking, which is what
   * this used to answer and is a different question on anything but a local file.
   * <p>
   * It was {@code reader.ready() || is.available() > 0}, and neither of those means "not at the end". A
   * {@link BufferedInputStream} over a socket answers {@code available()} with the bytes that have ALREADY ARRIVED,
   * which is zero between two packets of a perfectly healthy transfer, so on a slow or chunked remote source
   * ({@code -url http://...}) the answer was {@code false} in the MIDDLE of the stream and every caller - all of
   * them in {@link SourceDiscovery} - read that as end-of-input and sniffed a partial first line. A local file
   * could never show it, which is why no test ever did (issue #7494).
   * <p>
   * The only way to know a stream is not exhausted is to read from it, so this peeks one character and keeps it for
   * the next {@link #nextChar()}. It therefore BLOCKS where it used to guess, which is the correction.
   */
  public boolean isAvailable() throws IOException {
    if (peeked != NOTHING_PEEKED)
      // A PEEKED CHARACTER IS STILL TO BE CONSUMED BY nextChar(), SO THE SOURCE IS AVAILABLE WHEN THAT CHARACTER IS
      // NOT THE END-OF-STREAM MARKER - AND, WHEN A LIMIT IS SET, WHEN THE LIMIT STILL ALLOWS IT. THE PEEK HAS
      // ALREADY ADVANCED position FOR THE BUFFERED CHARACTER, SO THE COMPARISON IS AGAINST position - 1: THIS ANSWERS
      // EXACTLY WHAT THE BRANCH BELOW WOULD HAVE ANSWERED HAD THE PEEK NOT HAPPENED
      return peeked >= 0 && (limit <= 0 || position.get() - 1 < limit);
    if (limit > 0 && position.get() >= limit)
      return false;
    return peek() >= 0;
  }

  /**
   * The source's byte stream, positioned past its leading comment block unless this parser was built to skip that
   * (see {@link #Parser(Source, long, boolean)}).
   * <p>
   * Read it, do not rewind it. While the block is being dropped the pushback the lookahead needs is not markable,
   * so the stream answers {@link InputStream#markSupported()} with {@code false}, {@code mark()} does nothing and
   * {@code reset()} throws {@link IOException} - the {@link InputStream} contract for an unmarkable stream, and not
   * the silent partial rewind an unwrapped source would give. Rewinding a source is {@link #reset()}'s job anyway:
   * it re-opens the source and rebuilds this stream, which is the only thing that puts the comment-block scan back
   * at the start too.
   */
  public InputStream getInputStream() {
    return is;
  }

  public InputStreamReader getReader() {
    return reader;
  }

  public long getPosition() {
    return position.get();
  }

  public long getTotal() {
    return limit > 0 ? Math.min(limit, total) : total;
  }

  public boolean isCompressed() {
    return compressed;
  }

  public Source getSource() {
    return source;
  }

  private void resetInput() {
    final BufferedInputStream buffered = new BufferedInputStream(source.inputStream) {
      @Override
      public synchronized int read() throws IOException {
        position.incrementAndGet();
        return super.read();
      }

      @Override
      public synchronized int read(final byte[] b) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        final int res = super.read(b);
        position.addAndGet(res);
        return res;
      }

      @Override
      public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
        if (limit > 0 && position.get() > limit)
          throw new EOFException();

        final int res = super.read(b, off, len);
        position.addAndGet(res);
        return res;
      }

      @Override
      public synchronized int available() throws IOException {
        if (limit > 0 && position.get() > limit)
          return 0;

        return super.available();
      }

      @Override
      public synchronized void reset() {
        pos = 0;
        position.set(0);
      }
    };
    buffered.mark(0);

    final Charset charset = DatabaseFactory.getDefaultCharset();
    this.is = skipLeadingComments && isCommentStrippableCharset(charset) ?
        new LeadingCommentInputStream(buffered) :
        buffered;
    this.reader = new InputStreamReader(this.is, charset);
  }

  /**
   * Whether {@code charset} encodes the three characters the comment rule is written in - {@code #}, {@code /} and
   * {@code \n} - as the one ASCII byte each of them is. That is what makes dropping the comment block on the BYTE
   * stream, which is what the XML and vector formats consume, the same operation as dropping it on the decoded
   * characters.
   * <p>
   * True for UTF-8 and every ISO-8859/windows-125x charset. False for UTF-16 and UTF-32, where a byte-oriented scan
   * would match the low byte of an unrelated character and leave the stream misaligned - so the block is left in
   * place there rather than stripped wrongly.
   * <p>
   * {@link com.arcadedb.database.DatabaseFactory#getDefaultCharset()} answers UTF-8 and only UTF-8 today, so the
   * false arm is a guard on an assumption rather than a live path - which is exactly why it is package-private
   * and asserted directly: byte-alignment logic nobody exercises is the kind that rots.
   */
  static boolean isCommentStrippableCharset(final Charset charset) {
    final byte[] probe = "#/\n".getBytes(charset);
    return probe.length == 3 && probe[0] == '#' && probe[1] == '/' && probe[2] == '\n';
  }

  /**
   * The source with its leading comment block ({@code #} and {@code //} lines) removed, which is what every import
   * format is handed.
   * <p>
   * Content sniffing skips that block before it decides the format ({@code SourceDiscovery.analyzeText}), and until
   * this existed the block was then passed INTACT to the format that decision picked: only univocity's own
   * {@code #} was dropped at parse time, so a {@code //}-commented source broke every format and a
   * {@code #}-commented one broke XML, JSON and JSONL - a half-built feature whose working half was the invisible
   * one (issue #7490).
   * <p>
   * The rule is {@link SourceDiscovery#isCommentLineStart(char, char)}, shared with the sniffer rather than
   * restated: the two walk different abstractions but have to answer "is this a comment" identically, or a line one
   * of them skipped arrives at the other as data.
   * <p>
   * The LEADING block only: a {@code //} appearing later in a source is data, and a value legitimately beginning
   * with {@code //} - a protocol-relative URL - keeps its row. The block is dropped on the FIRST read rather than
   * eagerly, because {@link #resetInput()} runs inside the constructor and a source nobody has asked for a byte yet
   * must not be made to block for one.
   */
  private static final class LeadingCommentInputStream extends FilterInputStream {
    private boolean stripped;

    private LeadingCommentInputStream(final InputStream in) {
      // TWO BYTES OF PUSHBACK: THE '/' OF A DATA LINE THAT MERELY BEGINS WITH ONE, AND THE CHARACTER READ TO FIND OUT
      super(new PushbackInputStream(in, 2));
    }

    @Override
    public int read() throws IOException {
      strip();
      return in.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      strip();
      return in.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
      strip();
      return in.skip(n);
    }

    /**
     * Zero until the block has been dropped, rather than {@link FilterInputStream}'s delegation to a stream still
     * positioned on the comment: those bytes are not readable from THIS stream, and the first read has to drop them
     * before it can answer, which may block. Zero is always a valid answer to "how many bytes can be read without
     * blocking", and once the block is gone the underlying estimate is the right one again.
     */
    @Override
    public int available() throws IOException {
      return stripped ? in.available() : 0;
    }

    private void strip() throws IOException {
      if (stripped)
        return;
      stripped = true;

      final PushbackInputStream pushback = (PushbackInputStream) in;
      while (true) {
        final int first = pushback.read();
        if (first < 0)
          return;

        // READ THE SECOND BYTE ONLY WHEN IT CAN MATTER, AND GIVE IT BACK WHEN IT TURNS OUT TO BE DATA. THE
        // "NOT READ" SENTINEL IS NOT_READ AND NOT 0, BECAUSE read() ANSWERS 0 FOR A REAL NUL BYTE
        final int second = first == '/' ? pushback.read() : NOT_READ;

        if (!SourceDiscovery.isCommentLineStart((char) first, second >= 0 ? (char) second : 0)) {
          if (second >= 0)
            pushback.unread(second);
          pushback.unread(first);
          return;
        }

        // A COMMENT LINE: DISCARD IT WHOLE AND LOOK AT THE NEXT ONE
        for (int c = pushback.read(); c >= 0 && c != '\n'; c = pushback.read())
          ;
      }
    }
  }
}
