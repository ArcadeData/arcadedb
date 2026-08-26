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

import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #6180: a request body that has FAILED must never hand the batch parser another byte.
 * <p>
 * The stream under test is what stands between the parser and a connection that lies. Undertow's
 * {@code UndertowInputStream} allocates a pooled buffer before the channel read that fails and does not release it,
 * so the next read is served from that buffer - which holds whatever the POOL last left in it, on this connection
 * usually its own request head and the records already loaded. {@code InputStreamReader} is what reaches it, because
 * it probes with {@code available()} between decodes ({@code StreamDecoder.inReady}) and swallows the
 * {@link IOException} - so the failure that should have ended the load disappears and the replay is parsed as
 * payload. {@link FailsOnProbeThenOffersStaleBytes} is that connection, in the small.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostBatchHandlerBodyFailureTest {

  private static final String SENT  = "{\"@type\":\"vertex\",\"@class\":\"V1\",\"id\":1}\n";
  /** What a poisoned pooled buffer offers after the failure: bytes of this connection the parser already handled. */
  private static final String STALE = "POST /api/v1/batch/graph HTTP/1.1\r\n" + SENT + SENT;

  @Test
  void aProbeThatFailsEndsTheBodyInsteadOfPoisoningIt() throws Exception {
    final FailsOnProbeThenOffersStaleBytes connection = new FailsOnProbeThenOffersStaleBytes();
    final CountingInputStream body = new CountingInputStream(new HttpServerExchange(null), connection);

    assertThat(readFully(body, SENT.length())).isEqualTo(SENT);

    // The probe InputStreamReader makes between decodes: -1 is Undertow's own "the body is finished", which is what
    // the handler's truncation check reads, and it must not surface as an exception the decoder would swallow.
    assertThat(body.available()).as("a failed probe reports the end of the body, it does not throw").isEqualTo(-1);
    assertThat(body.hasBodyFailed()).isTrue();

    // Refused with the failure itself: the load is answered with the same reason it would have carried had the
    // read been the first thing to fail, which is what the 408 says.
    assertThatThrownBy(() -> body.read(new byte[64], 0, 64))
        .as("no byte may be read from a body that failed")
        .isInstanceOf(IOException.class)
        .hasMessage("peer closed the connection");
    assertThatThrownBy(body::read).isInstanceOf(IOException.class).hasMessage("peer closed the connection");

    assertThat(body.getBytesRead())
        .as("the load consumed exactly what the client sent")
        .isEqualTo(SENT.length());
    assertThat(connection.readsAfterFailure).as("the poisoned connection is never read again").isZero();
  }

  /**
   * The whole point, end to end over the very reader the batch parser uses: on a body that fails on the probe, a
   * {@link BufferedReader} must reach the end of the payload and then fail - never return the replayed lines. Without
   * the guard this reads the request line and the two records again, which on a unique index is the 409 duplicate key
   * {@code Issue5470BatchStreamStallIT} sees instead of a 408 (issues #6180 and #6176).
   */
  @Test
  void aReaderOverAFailedBodyNeverSeesTheReplayedLines() throws Exception {
    final FailsOnProbeThenOffersStaleBytes connection = new FailsOnProbeThenOffersStaleBytes();
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new CountingInputStream(new HttpServerExchange(null), connection),
            StandardCharsets.UTF_8));

    assertThat(reader.readLine()).isEqualTo(SENT.strip());
    assertThatThrownBy(reader::readLine).isInstanceOf(IOException.class);
  }

  @Test
  void aReadThatFailsIsNotAttemptedASecondTime() throws Exception {
    final InputStream connection = new InputStream() {
      private int reads;

      @Override
      public int read() throws IOException {
        if (++reads == 1)
          throw new IOException("connection reset");
        return 'x';
      }
    };
    final CountingInputStream body = new CountingInputStream(new HttpServerExchange(null), connection);

    assertThatThrownBy(body::read).isInstanceOf(IOException.class).hasMessage("connection reset");
    // Refused rather than attempted again: a second attempt would have succeeded and returned the 'x' below.
    assertThatThrownBy(body::read).isInstanceOf(IOException.class).hasMessage("connection reset");
    assertThat(body.getBytesRead()).isZero();
  }

  @Test
  void anIntactBodyIsUnaffected() throws Exception {
    final CountingInputStream body = new CountingInputStream(new HttpServerExchange(null),
        new ByteArrayInputStream(SENT.getBytes(StandardCharsets.UTF_8)));

    assertThat(body.available()).isEqualTo(SENT.length());
    assertThat(readFully(body, SENT.length())).isEqualTo(SENT);
    assertThat(body.read()).isEqualTo(-1);
    assertThat(body.isEndOfBody()).isTrue();
    assertThat(body.hasBodyFailed()).isFalse();
    assertThat(body.getBytesRead()).isEqualTo(SENT.length());
  }

  private static String readFully(final InputStream in, final int length) throws IOException {
    final byte[] buffer = new byte[length];
    int read = 0;
    while (read < length) {
      final int n = in.read(buffer, read, length - read);
      if (n < 0)
        break;
      read += n;
    }
    return new String(buffer, 0, read, StandardCharsets.UTF_8);
  }

  /**
   * A request body that delivers what the client sent, fails on the next probe, and - as a pooled buffer Undertow
   * leaked on that failure does - would happily serve unrelated bytes to whoever asks again.
   */
  private static class FailsOnProbeThenOffersStaleBytes extends InputStream {
    private final byte[] sent  = SENT.getBytes(StandardCharsets.UTF_8);
    private final byte[] stale = STALE.getBytes(StandardCharsets.UTF_8);
    private       int    position;
    private       boolean failed;
    private       int     readsAfterFailure;

    @Override
    public int available() throws IOException {
      if (position < sent.length)
        return sent.length - position;
      failed = true;
      throw new IOException("peer closed the connection");
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
      if (failed) {
        ++readsAfterFailure;
        final int copied = Math.min(len, stale.length);
        System.arraycopy(stale, 0, b, off, copied);
        return copied;
      }
      final int copied = Math.min(len, sent.length - position);
      System.arraycopy(sent, position, b, off, copied);
      position += copied;
      return copied;
    }

    @Override
    public int read() {
      final byte[] one = new byte[1];
      return read(one, 0, 1) <= 0 ? -1 : one[0] & 0xFF;
    }
  }
}
