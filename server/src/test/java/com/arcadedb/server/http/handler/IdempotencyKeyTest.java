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
}
