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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.http.RequestBodyTooLargeException;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;

/**
 * Decodes a {@code Content-Encoding} off a request body under a budget (issue #8084).
 * <p>
 * {@code arcadedb.server.httpBodyContentMaxSize} bounds the bytes that ARRIVE. Every route that decodes an
 * encoding materializes the decoded result whole in heap, so on those routes the wire cap is a compression-ratio
 * multiplier rather than a bound: InfluxDB line protocol is repetitive text and therefore close to the best case
 * for DEFLATE, ratios in the hundreds to low thousands are ordinary and a crafted body does far better, so an
 * accepted 100MB body is worth tens of GB. Snappy's format bounds the ratio far lower, but
 * {@code Snappy.uncompress} still allocates the whole output array up front from a length the payload itself
 * declares - which is the same hazard with a smaller constant.
 * <p>
 * One budget, two shapes of enforcement, because the two formats offer different evidence:
 * <ul>
 * <li>Snappy DECLARES its uncompressed length, so {@link #snappyUncompress} reads that first and refuses without
 * allocating anything. The check is not advisory - it is what stops the allocation.</li>
 * <li>gzip declares nothing that can be trusted (the trailer's {@code ISIZE} is the low 32 bits and is written by
 * the sender), so {@link #gunzip} reads through a stream that stops at the budget. A refusal therefore costs the
 * budget and never the body.</li>
 * </ul>
 * Streaming the decode INTO the parser instead would bound this one copy and not the answer: both routes then
 * materialize a sample list whose size is proportional to the decoded body, so a cap on the decoded size is the
 * bound either way and streaming would only lower the constant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CompressedBodyDecoder {

  private CompressedBodyDecoder() {
  }

  /**
   * The budget for a decoded body, or a value of zero or less for "unlimited".
   * <p>
   * {@code arcadedb.server.httpBodyContentDecompressedMaxSize} whenever it is set at all, and
   * {@code arcadedb.server.httpBodyContentMaxSize} only when it is NEGATIVE - the documented "follow the wire cap"
   * sentinel. The fallback carries that setting's own value through unchanged, its {@code -1} included, so an
   * administrator who declares the wire cap unlimited is not silently given a decoded cap.
   * <p>
   * The test is {@code >= 0} and not {@code > 0} deliberately (review of PR #8095). Zero is not "unset" here: it
   * is what {@code httpBodyContentMaxSize} itself spells "unlimited" with, next to {@code -1}, in
   * {@code HttpServer.createBodySizeLimitHandler}'s own {@code maxEntitySize > 0} gate. Folding it into the
   * fallback would make an explicit {@code 0} mean the wire cap - a different number, quietly - on the one
   * setting whose whole purpose is to be set independently of it.
   * <p>
   * Read off the {@link ContextConfiguration} the server was started with and never off the enum's own value, so
   * {@code SET SERVER SETTING} and a per-server override are both honoured (issue #7233).
   */
  public static long maxDecompressedSize(final ContextConfiguration configuration) {
    final long explicit = configuration.getValueAsLong(
        GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE);
    if (explicit >= 0)
      return explicit;
    return configuration.getValueAsLong(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE);
  }

  /**
   * Decodes a gzip body to a string, refusing it the moment it passes {@code maxSize} decoded bytes.
   *
   * @param maxSize the budget in decoded bytes; a value of zero or less means unlimited
   *
   * @throws RequestBodyTooLargeException when the body decodes past the budget
   * @throws IOException                  when the body is not valid gzip
   */
  public static String gunzip(final byte[] compressed, final long maxSize, final Charset charset) throws IOException {
    try (final InputStream in = boundedGzipStream(compressed, maxSize)) {
      // readAllBytes and then one String: with compact strings an ASCII line-protocol body ends up in a byte[] of
      // the same size, so this is two copies of the decoded body. Reading through a Reader into a StringBuilder
      // would be three, because the intermediate char[] is two bytes per character.
      return new String(in.readAllBytes(), charset);
    }
  }

  /**
   * Decodes a Snappy body, refusing it BEFORE the output array is allocated when the length it declares is past
   * {@code maxSize}.
   *
   * @param maxSize the budget in decoded bytes; a value of zero or less means unlimited
   *
   * @throws RequestBodyTooLargeException when the declared length is past the budget
   * @throws IOException                  when the body is not valid Snappy
   */
  public static byte[] snappyUncompress(final byte[] compressed, final long maxSize) throws IOException {
    if (maxSize > 0) {
      // The declared length, read out of the payload's own header without decompressing anything. Snappy.uncompress
      // reads the same field to size its output array, so refusing here is refusing before the allocation rather
      // than after it.
      final long declared = Snappy.uncompressedLength(compressed);
      if (declared > maxSize)
        throw tooLarge(declared + " bytes", maxSize);
    }
    return Snappy.uncompress(compressed);
  }

  private static InputStream boundedGzipStream(final byte[] compressed, final long maxSize) throws IOException {
    final InputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
    if (maxSize <= 0)
      return gzip;
    return new BoundedInputStream(gzip, maxSize);
  }

  private static RequestBodyTooLargeException tooLarge(final String decodedSize, final long maxSize) {
    return new RequestBodyTooLargeException(
        "The request body decodes to " + decodedSize + ", above the maximum of " + maxSize
            + " bytes allowed by '" + GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE.getKey()
            + "'. The limit on the compressed bytes ('" + GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_MAX_SIZE.getKey()
            + "') bounds what arrives, not what it expands to", maxSize);
  }

  /**
   * Stops the read the moment the budget is spent, rather than after it.
   * <p>
   * Both overrides count what they actually returned and throw as soon as the total passes the budget, so a body
   * that would decode to a hundred times the budget is refused having decompressed the budget - which is the whole
   * point of reading it through a stream instead of calling {@code readAllBytes} on the raw
   * {@code GZIPInputStream}.
   * <p>
   * What that bounds is a SOFT multiple of the budget and not the budget itself (review of PR #8095). The caller
   * accumulates into {@code readAllBytes}' own buffer, which grows geometrically, so at the instant the cap trips
   * that buffer can already be about the size of the budget and the copy that grew it transiently held the
   * previous one too: roughly 2x the configured value, not "the budget plus a small fixed buffer". Every buffered
   * read has that shape and it is bounded either way - which is the property that matters here, the unbounded one
   * being what issue #8084 is about - but the number an operator is sizing is the budget, so say which multiple of
   * it they are really agreeing to.
   * <p>
   * The throw is a {@code RuntimeException} out of a method declared to throw {@code IOException} alone, and that
   * is deliberate: the stream never escapes this class - it is created, drained and closed inside
   * {@link #gunzip} - and both public methods declare the exception. A future caller that hands this stream
   * somewhere else owns making that call site expect it.
   */
  private static final class BoundedInputStream extends FilterInputStream {
    private final long maxSize;
    private       long read;

    private BoundedInputStream(final InputStream in, final long maxSize) {
      super(in);
      this.maxSize = maxSize;
    }

    @Override
    public int read() throws IOException {
      final int b = super.read();
      if (b >= 0)
        count(1);
      return b;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
      final int n = super.read(buffer, offset, length);
      if (n > 0)
        count(n);
      return n;
    }

    private void count(final int bytes) {
      read += bytes;
      if (read > maxSize)
        throw tooLarge("more than " + maxSize + " bytes", maxSize);
    }
  }
}
