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
package com.arcadedb.server.http;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the per-request correlation context (issue #4466): an {@code X-Request-Id} is echoed when
 * supplied and generated when absent, the correlation is populated on the worker thread while the
 * request runs, and it is cleared afterwards so it never leaks across the pooled Undertow workers.
 * A probe {@link ObservationHandler} reads the worker-thread correlation in {@code onStop} (which the
 * handler runs in its finally block, before {@code clearCorrelation()}), giving an in-thread view.
 */
@Tag("slow")
class LogCorrelationIT extends BaseGraphServerTest {

  private final AtomicBoolean probeActive = new AtomicBoolean(true);

  @Override
  protected int getServerCount() {
    return 1;
  }

  @AfterEach
  void deactivateProbe() {
    probeActive.set(false);
    LogManager.instance().clearCorrelation();
  }

  @Test
  void requestIdHeaderIsEchoedAndPopulatedOnWorkerThread() throws Exception {
    final List<String> seenRequestIds = new CopyOnWriteArrayList<>();
    attachRequestIdProbe(seenRequestIds);

    final String requestId = "corr-test-echo-42";
    final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/ready").toURL().openConnection();
    c.setRequestMethod("GET");
    c.setRequestProperty("X-Request-Id", requestId);
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(204);

    // The client-supplied id must be echoed back on the response.
    assertThat(c.getHeaderField("X-Request-Id")).isEqualTo(requestId);
    c.disconnect();

    // ...and it must have been populated as the correlation context on the worker thread.
    assertThat(pollFor(seenRequestIds, requestId)).as("worker-thread correlation requestId").isTrue();
  }

  @Test
  void requestIdIsGeneratedWhenAbsent() throws Exception {
    final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/ready").toURL().openConnection();
    c.setRequestMethod("GET");
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(204);

    final String generated = c.getHeaderField("X-Request-Id");
    c.disconnect();

    assertThat(generated).as("a request id must be generated when the client sends none").isNotNull().isNotEmpty();
  }

  @Test
  void correlationDoesNotLeakOnTestThread() {
    // The test (client) thread never serves a request, so its correlation thread-local stays clear.
    assertThat(LogManager.instance().getCorrelation()).isNull();
  }

  private void attachRequestIdProbe(final List<String> sink) {
    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
      @Override
      public boolean supportsContext(final Observation.Context context) {
        return probeActive.get();
      }

      @Override
      public void onStop(final Observation.Context context) {
        if (probeActive.get() && "arcadedb.http.server.requests".equals(context.getName())) {
          final String id = LogManager.instance().getRequestId();
          if (id != null)
            sink.add(id);
        }
      }
    });
  }

  private static boolean pollFor(final List<String> sink, final String expected) throws InterruptedException {
    for (int attempt = 0; attempt < 100; attempt++) {
      if (sink.contains(expected))
        return true;
      Thread.sleep(20);
    }
    return false;
  }
}
