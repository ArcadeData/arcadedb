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
package com.arcadedb.redis;

import com.arcadedb.database.DatabaseFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Lossless bytes &lt;-&gt; {@link String} codec for Redis bulk strings, which are binary-safe on the wire (issue #9057).
 * <p>
 * Valid UTF-8 is decoded as usual. Every byte that is not part of a valid UTF-8 sequence is mapped to the lone low
 * surrogate {@code U+DC00 | byte} (the same idea as Python's {@code surrogateescape}), and mapped back to that exact byte
 * when encoding, so any byte sequence survives a SET/GET round trip. A genuine supplementary character (a high surrogate
 * followed by a low one) is never mistaken for an escape. If the configured default charset is not UTF-8 the codec falls
 * back to plain charset conversion.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RedisBinaryCodec {
  private static final char ESCAPE_MIN = '\uDC80';
  private static final char ESCAPE_MAX = '\uDCFF';

  private RedisBinaryCodec() {
  }

  private static boolean utf8() {
    return DatabaseFactory.getDefaultCharset() == StandardCharsets.UTF_8;
  }

  static String decode(final byte[] bytes, final int length) {
    if (!utf8())
      return new String(bytes, 0, length, DatabaseFactory.getDefaultCharset());

    boolean ascii = true;
    for (int i = 0; i < length; i++)
      if (bytes[i] < 0) {
        ascii = false;
        break;
      }
    if (ascii)
      return new String(bytes, 0, length, StandardCharsets.ISO_8859_1);

    final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
    final ByteBuffer in = ByteBuffer.wrap(bytes, 0, length);
    final CharBuffer out = CharBuffer.allocate(length);
    while (true) {
      final CoderResult result = decoder.decode(in, out, true);
      if (result.isUnderflow()) {
        decoder.flush(out);
        break;
      }
      if (result.isError()) {
        // escape each offending byte on its own
        for (int i = 0; i < result.length(); i++)
          out.put((char) (0xDC00 | (in.get() & 0xFF)));
        decoder.reset();
      }
    }
    out.flip();
    return out.toString();
  }

  static byte[] encode(final CharSequence text) {
    final String s = text.toString();
    if (!utf8() || !hasEscape(s))
      return s.getBytes(DatabaseFactory.getDefaultCharset());

    final byte[] out = new byte[s.length() * 3];
    int pos = 0;
    final int len = s.length();
    for (int i = 0; i < len; i++) {
      final char c = s.charAt(i);
      if (isEscape(s, i, c))
        out[pos++] = (byte) c;
      else if (c < 0x80)
        out[pos++] = (byte) c;
      else if (c < 0x800) {
        out[pos++] = (byte) (0xC0 | (c >> 6));
        out[pos++] = (byte) (0x80 | (c & 0x3F));
      } else if (Character.isHighSurrogate(c) && i + 1 < len && Character.isLowSurrogate(s.charAt(i + 1))) {
        final int cp = Character.toCodePoint(c, s.charAt(++i));
        out[pos++] = (byte) (0xF0 | (cp >> 18));
        out[pos++] = (byte) (0x80 | ((cp >> 12) & 0x3F));
        out[pos++] = (byte) (0x80 | ((cp >> 6) & 0x3F));
        out[pos++] = (byte) (0x80 | (cp & 0x3F));
      } else if (Character.isSurrogate(c)) {
        out[pos++] = '?'; // unpaired surrogate that is not an escape: same as String.getBytes()
      } else {
        out[pos++] = (byte) (0xE0 | (c >> 12));
        out[pos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        out[pos++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return Arrays.copyOf(out, pos);
  }

  static int encodedLength(final String s) {
    if (!utf8() || !hasEscape(s))
      return s.getBytes(DatabaseFactory.getDefaultCharset()).length;
    return encode(s).length;
  }

  private static boolean hasEscape(final String s) {
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (c >= ESCAPE_MIN && c <= ESCAPE_MAX)
        return true;
    }
    return false;
  }

  private static boolean isEscape(final String s, final int i, final char c) {
    return c >= ESCAPE_MIN && c <= ESCAPE_MAX && (i == 0 || !Character.isHighSurrogate(s.charAt(i - 1)));
  }
}
