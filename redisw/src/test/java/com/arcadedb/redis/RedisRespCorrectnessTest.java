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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for three follow-ups filed during the security review of #5895/#5902:
 * <ul>
 *   <li>#5907 - {@code RedisNetworkExecutor.parseChars()} widened each byte to a UTF-16 char instead of
 *   decoding it, mangling any non-ASCII bulk string.</li>
 *   <li>#5911 - the {@code $} (bulk string) branch of {@code parseNext()} unconditionally skipped a
 *   trailing CRLF even for a RESP2 null bulk string ({@code $-1\r\n}), which has none, desyncing the
 *   parser from the next token on the wire.</li>
 *   <li>#5912 - the Redis listener never configured a socket read timeout, so an unauthenticated client
 *   that opens a connection and never sends anything held the connection thread open indefinitely.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisRespCorrectnessTest extends BaseGraphServerTest {

  private static final int    DEF_PORT = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final String USER     = "root";
  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

  @Test
  void bulkStringWithMultibyteUtf8RoundTripsExactly() {
    // Accented Latin, CJK and a 4-byte emoji code point: every one of these has at least one byte >= 0x80,
    // which the old `(char) b` per-byte widening corrupted instead of decoding.
    final String valueText = "héllo wörld 日本語 😀";

    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      jedis.auth(USER, PASSWORD);
      jedis.set("utf8Key", valueText);
      assertThat(jedis.get("utf8Key")).isEqualTo(valueText);
    }
  }

  @Test
  void bulkStringRawWireBytesDecodeAsUtf8NotPerByteWidening() throws Exception {
    // Same regression as above, but talking raw RESP so the assertion is against the exact bytes the
    // server sends back rather than however the client library happens to decode them.
    final byte[] payloadValue = "café".getBytes(StandardCharsets.UTF_8); // 5 bytes: c,a,f,e-acute(2 bytes)

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendRaw(socket, "*3\r\n$3\r\nSET\r\n$6\r\nrawKey\r\n$" + payloadValue.length + "\r\n");
      socket.getOutputStream().write(payloadValue);
      socket.getOutputStream().write("\r\n".getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();
      readReply(socket); // +OK

      sendCommand(socket, "GET", "rawKey");
      final String reply = readReply(socket);

      // A well-formed bulk-string reply: "$<len>\r\n<payload>" (readReply() strips the trailing CRLF) with
      // <len> matching the UTF-8 byte length of the original value, not the char-widened (and re-encoded)
      // mismatch the bug produced.
      assertThat(reply).isEqualTo("$" + payloadValue.length + "\r\n" + new String(payloadValue, StandardCharsets.UTF_8));
    }
  }

  @Test
  void nullBulkStringArgumentDoesNotDesyncParser() throws Exception {
    // A RESP2 null bulk string ($-1) as a command argument, immediately followed - on the very same
    // connection - by a normal AUTH/PING pair. Before the fix, the unconditional skipLF() after $-1
    // swallowed the leading byte of the next command, corrupting everything parsed afterward.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      final StringBuilder payload = new StringBuilder();
      payload.append("*1\r\n$-1\r\n"); // a 1-element array containing a null bulk string
      payload.append("*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$").append(PASSWORD.length()).append("\r\n").append(PASSWORD).append("\r\n");
      payload.append("*1\r\n$4\r\nPING\r\n");

      socket.getOutputStream().write(payload.toString().getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      // The null-bulk command itself has no valid command name, so it produces no reply at all; the very
      // next bytes on the wire must be AUTH's clean "+OK", followed by PING's clean "+PONG" - proving the
      // parser stayed in sync instead of drifting into the following command's bytes.
      assertThat(readReply(socket)).isEqualTo("+OK");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void nonMinusOneNegativeBulkLengthIsAlsoTreatedAsNull() throws Exception {
    // RESP2 only defines -1 as the null bulk string, but parseNext() deliberately treats every negative
    // size the same way (see the comment on that branch) rather than adding a separate protocol-error case
    // for e.g. $-5. Locks in that this is a real, tested decision and not just an artifact of `size < 0`
    // happening to also be true for -1: same parser-resync proof as the $-1 case above, with $-5 instead.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      final StringBuilder payload = new StringBuilder();
      payload.append("*1\r\n$-5\r\n");
      payload.append("*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$").append(PASSWORD.length()).append("\r\n").append(PASSWORD).append("\r\n");
      payload.append("*1\r\n$4\r\nPING\r\n");

      socket.getOutputStream().write(payload.toString().getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      assertThat(readReply(socket)).isEqualTo("+OK");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void nullBulkStringAsCommandArgumentIsRejectedCleanly() throws Exception {
    // $-1 is reachable as any array element now (issue #5911's fix), not just the command name - including
    // as a SET argument, which used to reach ConcurrentHashMap.put() with a null value and NPE deep inside
    // setVariable() instead of getting one clear reply. Must get a clean protocol error instead, and the
    // connection must stay usable afterward.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      assertThat(readReply(socket)).isEqualTo("+OK");

      sendRaw(socket, "*3\r\n$3\r\nSET\r\n$6\r\nsomeky\r\n$-1\r\n");
      final String reply = readReply(socket);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("null bulk string");

      // The connection must still be usable - the malformed command must not have desynced the parser or
      // left the connection in a broken state.
      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  @Tag("slow")
  void failedReauthenticationRearmsThePreAuthTimeout() throws Exception {
    // A connection that already authenticated once has its idle timeout lifted to infinite. If it then
    // fails a *subsequent* AUTH on the same connection, it goes back to logically unauthenticated and must
    // not be left with an infinite read timeout - otherwise it could be held open forever despite never
    // (currently) holding valid credentials, exactly the resource-exhaustion shape #5912 fixes pre-auth.
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      assertThat(readReply(socket)).isEqualTo("+OK");

      sendCommand(socket, "AUTH", USER, "wrong-password");
      final String reply = readReply(socket);
      assertThat(reply).startsWith("-");
      assertThat(reply).containsIgnoringCase("WRONGPASS");

      // Now idle: must be closed like a never-authenticated connection, not held open by the timeout that
      // got lifted on the earlier successful AUTH.
      assertThat(socket.getInputStream().read()).isEqualTo(-1);
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  @Tag("slow")
  void idleUnauthenticatedConnectionIsClosedInsteadOfHeldOpenIndefinitely() throws Exception {
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try {
      try (final Socket socket = new Socket("localhost", DEF_PORT)) {
        // Safety bound for the test itself only, well above the lowered server-side timeout: if this
        // fires instead of a clean EOF, the server is still holding the idle connection open.
        socket.setSoTimeout(10_000);
        assertThat(socket.getInputStream().read()).isEqualTo(-1);
      }
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  @Tag("slow")
  void authenticatedConnectionIsNotClosedWhileIdle() throws Exception {
    // The idle timeout only bounds the pre-authentication window: once authenticated, a RESP connection is
    // expected to sit idle between commands (that is normal client usage, not a hostile pattern).
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      Thread.sleep(1_500); // well over the lowered pre-auth timeout
      assertThat(jedis.ping()).isEqualTo("PONG");
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }
  }

  @Test
  void incrByAcceptsSigned64BitIncrements() {
    // INCRBY amount was parsed as 32-bit int before #6466: a 64-bit increment threw
    // NumberFormatException -> "-ERR For input string". Real Redis accepts any signed 64-bit value.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      jedis.auth(USER, PASSWORD);
      // ArcadeDB Redis wire has no DEL; use SET to initialise the key
      jedis.set("incr64", "0");
      assertThat(jedis.incrBy("incr64", 3_000_000_000L)).isEqualTo(3_000_000_000L);
      assertThat(jedis.incrBy("incr64", -1_000_000_000L)).isEqualTo(2_000_000_000L);
      assertThat(jedis.decrBy("incr64", 2_000_000_000L)).isZero();
    }
  }

  @Test
  void incrOverflowReturnsErrorInsteadOfWrapping() throws Exception {
    // (issue #6466): INCR on Long.MAX_VALUE wrapped silently to Long.MIN_VALUE. Real Redis answers
    // with an error; the wire reply must be "-ERR ... overflow" and the stored value must be untouched.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "overflowKey", "9223372036854775807"); // Long.MAX_VALUE
      readReply(socket); // +OK

      sendCommand(socket, "INCR", "overflowKey");
      final String reply = readReply(socket);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("overflow");

      // The key must keep its pre-increment value: a failed INCR is a no-op.
      sendCommand(socket, "GET", "overflowKey");
      assertThat(readReply(socket)).isEqualTo("$19\r\n9223372036854775807");
    }
  }

  @Test
  void setNxDoesNotOverwriteExistingKey() throws Exception {
    // (issue #6466): SET k v NX on an existing key returned +OK and overwrote it, so a distributed-lock
    // client believed it acquired a lock it did not. Real Redis replies with the RESP2 null bulk string.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "lockKey", "1");
      readReply(socket); // +OK

      sendCommand(socket, "SET", "lockKey", "2", "NX");
      assertThat(readReply(socket)).isEqualTo("$-1");

      sendCommand(socket, "GET", "lockKey");
      assertThat(readReply(socket)).isEqualTo("$1\r\n1");
    }
  }

  @Test
  void setXxDoesNotCreateMissingKey() throws Exception {
    // (issue #6466): XX must only set an existing key; on a missing key real Redis replies nil.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      // ArcadeDB Redis wire has no DEL; absentKey is already absent
      sendCommand(socket, "SET", "absentKey", "v", "XX");
      assertThat(readReply(socket)).isEqualTo("$-1");

      sendCommand(socket, "GET", "absentKey");
      assertThat(readReply(socket)).isEqualTo("$-1");
    }
  }

  @Test
  void setGetReturnsPreviousValue() throws Exception {
    // (issue #6466): SET k v GET must return the previous value (bulk string) instead of +OK.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "prevKey", "old");
      readReply(socket); // +OK

      sendCommand(socket, "SET", "prevKey", "new", "GET");
      final String reply = readReply(socket);
      assertThat(reply).isEqualTo("$3\r\nold");

      sendCommand(socket, "GET", "prevKey");
      assertThat(readReply(socket)).isEqualTo("$3\r\nnew");
    }
  }

  @Test
  void setNxGetReturnsExistingValueOnVetoedWrite() throws Exception {
    // Real Redis: combining GET with NX/XX still returns the pre-existing value when the NX/XX condition
    // vetoes the write (it does not fall back to nil just because nothing was written).
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "nxGetKey", "original");
      readReply(socket); // +OK

      sendCommand(socket, "SET", "nxGetKey", "shouldNotApply", "NX", "GET");
      assertThat(readReply(socket)).isEqualTo("$8\r\noriginal");

      // The vetoed write must not have touched the stored value.
      sendCommand(socket, "GET", "nxGetKey");
      assertThat(readReply(socket)).isEqualTo("$8\r\noriginal");
    }
  }

  @Test
  void setWithUnsupportedExpiryOptionIsRejectedInsteadOfSilentlyIgnored() throws Exception {
    // (issue #6466): EX/PX were silently dropped, so a client setting EX 10 believed the key would
    // expire. ArcadeDB transient keys have no TTL store, so the honest reply is a clear error.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "ttlKey", "v", "EX", "10");
      final String reply = readReply(socket);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("unsupported");
    }
  }

  @Test
  void incrByFloatRepliesWithBulkString() throws Exception {
    // (issue #6466 minor): INCRBYFLOAT replied with a RESP simple string ("+3.3\r\n") instead of a bulk
    // string ("$3\r\n3.3\r\n"). Jedis parses both forms into the same double, so routing this through the
    // client (as the original version of this test did) would pass whether or not the wire format was
    // fixed; asserting on the raw frame via readReply() is what actually exercises the fix.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "floatKey", "0");
      readReply(socket); // +OK

      sendCommand(socket, "INCRBYFLOAT", "floatKey", "3.3");
      assertThat(readReply(socket)).isEqualTo("$3\r\n3.3");
    }
  }

  @Test
  void pingWithoutArgumentRepliesAsSimpleString() throws Exception {
    // Real Redis: PING with no argument is +PONG, not a bulk string.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void pingWithMessageRepliesAsBulkString() throws Exception {
    // (issue #6942): PING <message> replied with a RESP simple string ("+hello\r\n") instead of a bulk
    // string ("$5\r\nhello\r\n"). Real Redis always echoes the PING argument as a bulk string.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "PING", "hello");
      assertThat(readReply(socket)).isEqualTo("$5\r\nhello");
    }
  }

  @Test
  void pingWithCrLfInMessageDoesNotDesyncConnection() throws Exception {
    // (issue #6942): a simple-string PING reply is CRLF-terminated, so a message containing "\r\n" split
    // the reply into two frames and desynchronized the connection permanently - every later reply on that
    // connection was read one frame early. A length-prefixed bulk-string reply carries the CRLF as payload
    // instead of treating it as a frame terminator, so the next command's reply must still line up.
    final String payload = "a\r\n+INJECTED";
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "PING", payload);
      assertThat(readReply(socket)).isEqualTo("$" + payload.length() + "\r\n" + payload);

      // If the parser had desynced, this PING's reply would actually be leftover bytes from the previous
      // frame instead of a fresh +PONG.
      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void decrOnFloatKeyReturnsErrorInsteadOfInvalidIntegerReply() throws Exception {
    // (issue #6942): once INCRBYFLOAT turns a key into a Double, DECR blindly wrote a `:`-prefixed
    // (RESP integer) header in front of a floating-point value, which is not a valid RESP integer. Real
    // Redis rejects DECR/INCR on a non-integer value with "-ERR value is not an integer or out of range".
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "decrFloatKey", "0");
      readReply(socket); // +OK

      sendCommand(socket, "INCRBYFLOAT", "decrFloatKey", "3.3");
      readReply(socket); // $3\r\n3.3

      sendCommand(socket, "DECR", "decrFloatKey");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      // A rejected DECR must be a no-op: the stored value stays untouched.
      sendCommand(socket, "GET", "decrFloatKey");
      assertThat(readReply(socket)).isEqualTo("$3\r\n3.3");
    }
  }

  @Test
  void incrOnFloatKeyReturnsErrorInsteadOfInvalidIntegerReply() throws Exception {
    // (issue #6942): the mirror case for INCR - incrBy's non-decimal branch answered with a RESP simple
    // string ("+3.3") rather than rejecting the non-integer value the way real Redis does.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "incrFloatKey", "0");
      readReply(socket); // +OK

      sendCommand(socket, "INCRBYFLOAT", "incrFloatKey", "3.3");
      readReply(socket); // $3\r\n3.3

      sendCommand(socket, "INCR", "incrFloatKey");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      sendCommand(socket, "GET", "incrFloatKey");
      assertThat(readReply(socket)).isEqualTo("$3\r\n3.3");
    }
  }

  @Test
  void incrOnDirectlySetFractionalStringReturnsIntegerTypeError() throws Exception {
    // (issue #6942 code review): a key set directly to a fractional string via SET (never touched by
    // INCRBYFLOAT) hit a different code path than the Double-typed case above and answered with the
    // generic "Key 'x' is not a number" instead of Redis' exact integer-command error. Same fix, same
    // message, for INCR/INCRBY/DECR/DECRBY, whichever way the non-integral value got there.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "fractionalStringKey", "3.3");
      readReply(socket); // +OK

      sendCommand(socket, "INCR", "fractionalStringKey");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      sendCommand(socket, "DECR", "fractionalStringKey");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      // Neither rejected command touched the stored value.
      sendCommand(socket, "GET", "fractionalStringKey");
      assertThat(readReply(socket)).isEqualTo("$3\r\n3.3");
    }
  }

  @Test
  void incrByFloatOnDirectlySetFractionalStringSucceeds() throws Exception {
    // (issue #6942 code review): INCRBYFLOAT has no integer restriction - it must accept a fractional
    // string exactly like real Redis, not reject it the way INCR/DECR correctly do.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "incrByFloatStringKey", "3.3");
      readReply(socket); // +OK

      sendCommand(socket, "INCRBYFLOAT", "incrByFloatStringKey", "1.0");
      assertThat(readReply(socket)).isEqualTo("$3\r\n4.3");
    }
  }

  @Test
  void incrByAndDecrByWithExplicitAmountRejectFractionalValueTheSameWayAsIncrAndDecr() throws Exception {
    // (issue #6942 code review): INCRBY/DECRBY dispatch through the same incrBy()/decrBy() as INCR/DECR
    // (RedisNetworkExecutor's INCR/INCRBY and DECR/DECRBY cases both call the same method), so this locks
    // in that the explicit-amount form goes through requireIntegralValue() exactly like the no-amount form.
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "incrByDecrByFloatKey", "3.3");
      readReply(socket); // +OK

      sendCommand(socket, "INCRBY", "incrByDecrByFloatKey", "5");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      sendCommand(socket, "DECRBY", "incrByDecrByFloatKey", "5");
      assertThat(readReply(socket)).isEqualTo("-ERR value is not an integer or out of range");

      sendCommand(socket, "GET", "incrByDecrByFloatKey");
      assertThat(readReply(socket)).isEqualTo("$3\r\n3.3");
    }
  }

  @Test
  void incrByOverflowStillReturnsOverflowErrorAfterIntegralValueRefactor() throws Exception {
    // (issue #6942 code review): confirms Math.addExact's ArithmeticException -> "increment or decrement
    // would overflow" still fires for INCRBY after routing the stored value through requireIntegralValue().
    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);

      sendCommand(socket, "AUTH", USER, PASSWORD);
      readReply(socket); // +OK

      sendCommand(socket, "SET", "incrByOverflowKey", "9223372036854775807"); // Long.MAX_VALUE
      readReply(socket); // +OK

      sendCommand(socket, "INCRBY", "incrByOverflowKey", "1");
      final String reply = readReply(socket);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("overflow");

      sendCommand(socket, "GET", "incrByOverflowKey");
      assertThat(readReply(socket)).isEqualTo("$19\r\n9223372036854775807");
    }
  }

  @Test
  void concurrentSetNxOnSharedDatabaseKeyOnlyLetsOneWinnerThrough() throws Exception {
    // (issue #6466 follow-up): the initial fix evaluated NX as "does the key exist?" then "write it" as two
    // separate calls. That is only safe when the key lives in a per-connection bucket; once a key is backed
    // by a database's global variables (SELECT, or an explicit "db.key" prefix) it is shared by every
    // connection, and two racing SET k v NX calls could both observe "absent" and both believe they set a
    // lock. This drives many concurrent connections at the same key and asserts exactly one NX succeeds.
    final int racers = 16;
    final String databaseName = getDatabaseName();
    final String key = databaseName + ".raceLock";

    final CyclicBarrier barrier = new CyclicBarrier(racers);
    final ExecutorService pool = Executors.newFixedThreadPool(racers);
    try {
      final List<Future<Boolean>> results = new ArrayList<>();
      for (int i = 0; i < racers; i++) {
        final int id = i;
        results.add(pool.submit(() -> {
          try (final Socket socket = new Socket("localhost", DEF_PORT)) {
            socket.setSoTimeout(10_000);
            sendCommand(socket, "AUTH", USER, PASSWORD);
            readReply(socket); // +OK

            barrier.await(10, TimeUnit.SECONDS);
            sendCommand(socket, "SET", key, "owner-" + id, "NX");
            return readReply(socket).startsWith("+OK");
          }
        }));
      }

      long winners = 0;
      for (final Future<Boolean> result : results)
        if (result.get(10, TimeUnit.SECONDS))
          winners++;

      assertThat(winners).isEqualTo(1);
    } finally {
      pool.shutdownNow();
    }
  }

  private static void sendCommand(final Socket socket, final String... args) throws IOException {
    sendRaw(socket, encodeCommand(args));
  }

  private static void sendRaw(final Socket socket, final String raw) throws IOException {
    socket.getOutputStream().write(raw.getBytes(StandardCharsets.US_ASCII));
    socket.getOutputStream().flush();
  }

  private static String encodeCommand(final String... args) {
    final StringBuilder sb = new StringBuilder();
    sb.append("*").append(args.length).append("\r\n");
    for (final String arg : args)
      sb.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
    return sb.toString();
  }

  /**
   * Reads exactly one RESP reply frame (simple string, error, integer or bulk string) from the socket.
   * Returns the frame without its trailing CRLF, e.g. {@code "+OK"} or {@code "$4\r\ntest"}.
   */
  private static String readReply(final Socket socket) throws IOException {
    final String firstLine = readLine(socket);
    if (firstLine.startsWith("$")) {
      final int size = Integer.parseInt(firstLine.substring(1));
      if (size < 0)
        return firstLine;
      final byte[] body = new byte[size];
      int read = 0;
      while (read < size) {
        final int n = socket.getInputStream().read(body, read, size - read);
        if (n == -1)
          throw new IOException("Unexpected EOF while reading bulk string body");
        read += n;
      }
      readLine(socket); // trailing CRLF
      return firstLine + "\r\n" + new String(body, StandardCharsets.UTF_8);
    }
    return firstLine;
  }

  private static String readLine(final Socket socket) throws IOException {
    final StringBuilder sb = new StringBuilder();
    int prev = -1;
    while (true) {
      final int b = socket.getInputStream().read();
      if (b == -1)
        throw new IOException("Unexpected EOF while reading a line");
      if (prev == '\r' && b == '\n') {
        sb.setLength(sb.length() - 1);
        break;
      }
      sb.append((char) b);
      prev = b;
    }
    return sb.toString();
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
