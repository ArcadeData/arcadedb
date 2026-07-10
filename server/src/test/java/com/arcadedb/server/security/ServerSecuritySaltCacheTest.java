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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class ServerSecuritySaltCacheTest {

  private static final String PLAINTEXT = "SuperSecretPwd12345";

  @BeforeEach
  void setUp() {
    // keep PBKDF2 iterations small so the test runs fast
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.setValue(64);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.reset();
  }

  @Test
  void shouldNotRetainPlaintextPasswordAsCacheKey() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final String salt = ServerSecurity.generateRandomSalt();
    // populate the cache
    security.encodePassword(PLAINTEXT, salt);

    final Set<String> keys = security.getSaltCacheKeysSnapshot();
    assertThat(keys).isNotEmpty();

    // no cache key may contain (or equal) the plaintext password
    for (final String key : keys) {
      assertThat(key).doesNotContain(PLAINTEXT);
      assertThat(key).doesNotContain(salt);
    }
  }

  @Test
  void shouldReturnSameEncodedHashOnCacheHit() {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final String salt = ServerSecurity.generateRandomSalt();

    final String first = security.encodePassword(PLAINTEXT, salt);
    // second call must resolve from cache to the identical encoded value
    final String second = security.encodePassword(PLAINTEXT, salt);

    assertThat(second).isEqualTo(first);

    // and it must still verify correctly
    assertThat(security.passwordMatch(PLAINTEXT, first)).isTrue();
    assertThat(security.passwordMatch("wrong-password", first)).isFalse();
  }

  @Test
  void shouldEncodeAndMatchWithCacheDisabled() {
    // cacheSize == 0 disables the cache (saltCache == null). The old code threw
    // UnsupportedOperationException on the unconditional put into Collections.emptyMap().
    GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.setValue(0);

    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final String salt = ServerSecurity.generateRandomSalt();
    final String encoded = security.encodePassword(PLAINTEXT, salt);

    assertThat(security.passwordMatch(PLAINTEXT, encoded)).isTrue();
    // nothing is retained when the cache is disabled
    assertThat(security.getSaltCacheKeysSnapshot()).isEmpty();
  }

  @Test
  void shouldEncodeConsistentlyUnderConcurrency() throws Exception {
    final ServerSecurity security = new ServerSecurity(null, new ContextConfiguration(), "./target");

    final String salt = ServerSecurity.generateRandomSalt();
    final String expected = security.encodePassword(PLAINTEXT, salt);

    final int threads = 16;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReference<String> mismatch = new AtomicReference<>();

    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < 200; i++) {
            final String encoded = security.encodePassword(PLAINTEXT, salt);
            if (!expected.equals(encoded))
              mismatch.compareAndSet(null, encoded);
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, SECONDS)).isTrue();

    assertThat(mismatch.get()).isNull();

    // plaintext still not reachable as a key after heavy concurrent use
    for (final String key : security.getSaltCacheKeysSnapshot())
      assertThat(key).doesNotContain(PLAINTEXT);
  }
}
