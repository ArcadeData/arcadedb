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
package com.arcadedb.network;

import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class HttpClientLifecycleTest {

  @Test
  void supportFollowsTheRuntime() {
    assertThat(HttpClientLifecycle.isSupported()).isEqualTo(Runtime.version().feature() >= 21);
  }

  @Test
  void aReleasedClientReportsTerminated() throws InterruptedException {
    final HttpClient client = HttpClient.newHttpClient();
    if (HttpClientLifecycle.isSupported())
      assertThat(HttpClientLifecycle.isTerminated(client)).isFalse();

    HttpClientLifecycle.shutdownNow(client);

    // On Java 17 nothing can be released and every answer is "terminated"; on Java 21+ the client really terminates
    assertThat(HttpClientLifecycle.awaitTermination(client, Duration.ofSeconds(10))).isTrue();
    assertThat(HttpClientLifecycle.isTerminated(client)).isTrue();
  }

  @Test
  void nullAndRepeatedReleasesAreHarmless() {
    final HttpClient client = HttpClient.newHttpClient();
    HttpClientLifecycle.shutdown(null);
    HttpClientLifecycle.close(null);
    HttpClientLifecycle.shutdown(client);
    HttpClientLifecycle.close(client);
    HttpClientLifecycle.close(client);
    assertThat(HttpClientLifecycle.isTerminated(client)).isTrue();
  }
}
