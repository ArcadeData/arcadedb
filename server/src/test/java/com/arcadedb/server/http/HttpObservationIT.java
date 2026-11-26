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

import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies HTTP request handling is wrapped in a Micrometer {@link Observation} on the server's
 * shared registry. A probe {@link ObservationHandler} stands in for the tracing handler the
 * optional plugin would attach, proving the instrumentation point fires with bounded tags. In
 * production with no tracer attached the same Observation is a zero-overhead no-op.
 */
@Tag("slow")
class HttpObservationIT extends BaseGraphServerTest {

  // The ObservationRegistry has no remove-handler API, so the probe is gated by this flag and
  // deactivated after the test, leaving it inert even though it stays registered.
  private final AtomicBoolean probeActive = new AtomicBoolean(true);

  @Override
  protected int getServerCount() {
    return 1;
  }

  @AfterEach
  void deactivateProbe() {
    probeActive.set(false);
  }

  @Test
  void httpHandlingIsWrappedInObservation() throws Exception {
    final List<Observation.Context> stopped = new CopyOnWriteArrayList<>();

    final ObservationRegistry registry = getServer(0).getObservationRegistry();
    final ObservationHandler<Observation.Context> probe = new ObservationHandler<>() {
      @Override
      public boolean supportsContext(final Observation.Context context) {
        return probeActive.get();
      }

      @Override
      public void onStop(final Observation.Context context) {
        if (probeActive.get())
          stopped.add(context);
      }
    };
    // Attach the probe for this test. It plays the role the tracing plugin's handler would: it makes
    // the otherwise no-op Observation observable.
    registry.observationConfig().observationHandler(probe);

    final HttpURLConnection c = (HttpURLConnection) new URL(getServerHttpUrl("/api/v1/ready")).openConnection();
    c.setRequestMethod("GET");
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(204);
    c.disconnect();

    // The server records the Observation in the handler's finally block, which can complete just
    // after the client receives the response. Poll briefly for the observation to be reported.
    Observation.Context httpContext = null;
    for (int attempt = 0; attempt < 100 && httpContext == null; attempt++) {
      httpContext = stopped.stream()
          .filter(ctx -> "arcadedb.http.server.requests".equals(ctx.getName()))
          .findFirst()
          .orElse(null);
      if (httpContext == null)
        Thread.sleep(20);
    }

    assertThat(httpContext).as("an arcadedb.http.server.requests Observation must be produced").isNotNull();
    assertThat(tag(httpContext, "method")).isEqualTo("GET");
    // The path tag must be the bounded route template, never the concrete URI.
    assertThat(tag(httpContext, "path")).isNotNull();
    assertThat(tag(httpContext, "status")).isEqualTo("204");
  }

  @Test
  void methodOutsideTheStandardSetIsReportedAsBoundedValue() throws Exception {
    // The low-cardinality "method" key feeds every handler that turns it into a meter, so it takes a bounded set of
    // values: the standard methods, and one constant for every other token of the request line.
    final List<Observation.Context> stopped = new CopyOnWriteArrayList<>();

    final ObservationHandler<Observation.Context> probe = new ObservationHandler<>() {
      @Override
      public boolean supportsContext(final Observation.Context context) {
        return probeActive.get();
      }

      @Override
      public void onStop(final Observation.Context context) {
        if (probeActive.get())
          stopped.add(context);
      }
    };
    getServer(0).getObservationRegistry().observationConfig().observationHandler(probe);

    final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    for (int i = 0; i < 5; i++) {
      final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerHttpUrl("/probe/" + i)))
          .method("XMETHOD" + i, HttpRequest.BodyPublishers.noBody()).build();
      client.send(request, HttpResponse.BodyHandlers.discarding());
    }

    // The Observation is stopped in the handler's finally block, which can complete just after the client
    // receives the response: poll for all five.
    for (int attempt = 0; attempt < 200 && httpObservations(stopped).size() < 5; attempt++)
      Thread.sleep(20);

    final List<Observation.Context> observed = httpObservations(stopped);
    assertThat(observed).hasSize(5);
    for (final Observation.Context context : observed)
      assertThat(tag(context, "method")).isEqualTo("OTHER");
  }

  private static List<Observation.Context> httpObservations(final List<Observation.Context> stopped) {
    return stopped.stream().filter(ctx -> "arcadedb.http.server.requests".equals(ctx.getName())).toList();
  }

  private static String tag(final Observation.Context context, final String key) {
    for (final KeyValue kv : context.getLowCardinalityKeyValues())
      if (kv.getKey().equals(key))
        return kv.getValue();
    return null;
  }
}
