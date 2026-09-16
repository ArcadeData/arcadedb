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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for {@code AbstractServerHttpHandler.buildIdempotencyKey}. The headline defect of
 * issue #5023 is that the idempotency cache keyed on the raw {@code X-Request-Id} alone, so two unrelated
 * requests reusing the same correlation id replayed each other's response and silently skipped writes.
 * The key must be bound to method, path, database and body.
 */
class IdempotencyKeyTest {

  @Test
  void sameRequestIdDifferentDatabaseProducesDifferentKey() {
    final String kA = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA",
        "{\"command\":\"INSERT INTO V SET n=1\"}");
    final String kB = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbB", "dbB",
        "{\"command\":\"INSERT INTO V SET n=1\"}");
    assertThat(kA).isNotEqualTo(kB);
  }

  @Test
  void sameRequestIdDifferentBodyProducesDifferentKey() {
    final String k1 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA",
        "{\"command\":\"INSERT INTO V SET n=1\"}");
    final String k2 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA",
        "{\"command\":\"INSERT INTO V SET n=2\"}");
    assertThat(k1).isNotEqualTo(k2);
  }

  @Test
  void sameRequestIdDifferentMethodProducesDifferentKey() {
    final String post = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA", "{}");
    final String put = AbstractServerHttpHandler.buildIdempotencyKey("abc", "PUT", "/api/v1/command/dbA", "dbA", "{}");
    assertThat(post).isNotEqualTo(put);
  }

  @Test
  void differentRequestIdProducesDifferentKey() {
    final String k1 = AbstractServerHttpHandler.buildIdempotencyKey("id-1", "POST", "/api/v1/command/dbA", "dbA", "{}");
    final String k2 = AbstractServerHttpHandler.buildIdempotencyKey("id-2", "POST", "/api/v1/command/dbA", "dbA", "{}");
    assertThat(k1).isNotEqualTo(k2);
  }

  @Test
  void identicalInputsProduceStableKey() {
    final String a = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA",
        "{\"command\":\"INSERT INTO V SET n=1\"}");
    final String b = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/command/dbA", "dbA",
        "{\"command\":\"INSERT INTO V SET n=1\"}");
    assertThat(a).isEqualTo(b);
    // SHA-256 hex is 64 characters.
    assertThat(a).hasSize(64);
  }

  @Test
  void fieldBoundaryCannotBeSpoofedByConcatenation() {
    // Without a field separator ("ab" + "") and ("a" + "b") would hash identically; the NUL delimiter
    // must keep them distinct.
    final String k1 = AbstractServerHttpHandler.buildIdempotencyKey("ab", "", "", "", null);
    final String k2 = AbstractServerHttpHandler.buildIdempotencyKey("a", "b", "", "", null);
    assertThat(k1).isNotEqualTo(k2);
  }

  @Test
  void nullBodyIsHandled() {
    final String k = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/begin/dbA", "dbA", null);
    assertThat(k).hasSize(64);
  }

  /**
   * Issue #7704: a route whose body is BYTES binds to it exactly as a text route binds to its string. The binary
   * routes ({@code /prom/write}, {@code /prom/read}) return {@code null} for the string payload by construction,
   * so before the byte arm existed their key was the request id, method, path and database and nothing else - and
   * two remote-write requests carrying different samples under one {@code X-Request-Id} replayed each other.
   */
  @Test
  void sameRequestIdDifferentBinaryBodyProducesDifferentKey() {
    final String k1 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write", "dbA",
        null, new byte[] { 1, 2, 3 });
    final String k2 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write", "dbA",
        null, new byte[] { 1, 2, 4 });
    assertThat(k1).isNotEqualTo(k2);
  }

  /** The same bytes are the same request, which is the retry the cache exists for. */
  @Test
  void anIdenticalBinaryBodyProducesTheSameKey() {
    final String k1 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write", "dbA",
        null, new byte[] { 9, 9, 9 });
    final String k2 = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write", "dbA",
        null, new byte[] { 9, 9, 9 });
    assertThat(k1).isEqualTo(k2);
  }

  /**
   * An empty byte body hashes as no body, which is the same treatment the text arm gives an empty string. Nothing
   * downstream distinguishes the two - an empty remote_write is answered 204 without reaching the engine - so the
   * digest does not pretend to either.
   */
  @Test
  void anEmptyBinaryBodyHashesAsNoBody() {
    final String absent = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write",
        "dbA", null, null);
    final String empty = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/api/v1/ts/dbA/prom/write",
        "dbA", null, new byte[0]);
    assertThat(absent).isEqualTo(empty);
  }

  /**
   * The two body forms cannot be confused for each other: a text body and a byte body carrying the same bytes are
   * two different requests, which is what the separator between the arms is for.
   */
  @Test
  void aTextBodyAndAByteBodyOfTheSameBytesAreDifferentKeys() {
    final String text = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/p", "dbA", "AB", null);
    final String binary = AbstractServerHttpHandler.buildIdempotencyKey("abc", "POST", "/p", "dbA", null,
        new byte[] { 'A', 'B' });
    assertThat(text).isNotEqualTo(binary);
  }
}
