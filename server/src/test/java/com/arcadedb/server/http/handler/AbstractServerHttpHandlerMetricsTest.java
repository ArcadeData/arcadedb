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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import io.micrometer.core.instrument.Timer;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.PathTemplateMatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the bounded HTTP RED-metric plumbing in {@link AbstractServerHttpHandler} (issue #5025):
 * the {@code path} tag must never carry the raw client URI, and the timers must be resolved from a cache
 * keyed by the tag tuple so the request hot path allocates no new {@code Meter.Id}/builder for a seen tuple.
 */
class AbstractServerHttpHandlerMetricsTest {

  @Test
  void pathTemplateCollapsesUnmatchedUriToBoundedConstant() {
    // No PathTemplateMatch is attached: the exchange represents a Studio "/" fallback / 404-probe request
    // whose relative path is fully client-controlled.
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.setRelativePath("/unmatched/attacker-controlled-12345?cache-buster=987");

    // The raw URI must never leak into the tag value; it collapses to a single bounded constant.
    assertThat(AbstractServerHttpHandler.pathTemplate(exchange)).isEqualTo("unmatched");
  }

  @Test
  void pathTemplateReturnsRouteTemplateWhenMatched() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.putAttachment(PathTemplateMatch.ATTACHMENT_KEY,
        new PathTemplateMatch("/command/{database}", Map.of("database", "graph")));

    // A matched route keeps its low-cardinality template, never the concrete database name.
    assertThat(AbstractServerHttpHandler.pathTemplate(exchange)).isEqualTo("/command/{database}");
  }

  @Test
  void httpRequestTimerIsCachedPerTagTuple() {
    final Timer first = AbstractServerHttpHandler.httpRequestTimer("GET", "unmatched", "200", "none");
    final Timer second = AbstractServerHttpHandler.httpRequestTimer("GET", "unmatched", "200", "none");

    // Same tag tuple -> the exact same cached Timer instance (no new Meter.Id/builder per request).
    assertThat(second).isSameAs(first);

    // A different tuple resolves to a distinct timer.
    final Timer differentStatus = AbstractServerHttpHandler.httpRequestTimer("GET", "unmatched", "500", "none");
    assertThat(differentStatus).isNotSameAs(first);
  }

  @Test
  void databaseTagKeepsTheNameOfADatabaseThatExists(@TempDir final Path root) {
    final DatabaseInternal graph = (DatabaseInternal) new DatabaseFactory(root.resolve("graph").toString()).create();
    try {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create().database("graph", new ServerDatabase(null, graph));

      final HttpServerExchange exchange = new HttpServerExchange(null);
      exchange.putAttachment(PathTemplateMatch.ATTACHMENT_KEY,
          new PathTemplateMatch("/query/{database}", Map.of("database", "graph")));

      assertThat(AbstractServerHttpHandler.databaseTag(exchange, server)).isEqualTo("graph");
    } finally {
      graph.drop();
    }
  }

  @Test
  void databaseTagCollapsesAnUnknownDatabaseToBoundedConstant() {
    // Issue #6805: {database} matches any path segment and the RED timer is recorded in a finally block that
    // also runs for the 401 an unauthenticated caller gets, so echoing the raw parameter registered one
    // permanent percentile-histogram Timer per invented name - the #5025 leak on the other half of the tuple.
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.putAttachment(PathTemplateMatch.ATTACHMENT_KEY,
        new PathTemplateMatch("/query/{database}", Map.of("database", "db12345")));

    assertThat(AbstractServerHttpHandler.databaseTag(exchange, server)).isEqualTo("unknown");
  }

  @Test
  void databaseTagIsNoneForRoutesThatAreNotDatabaseScoped() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    final HttpServerExchange withoutParameter = new HttpServerExchange(null);
    withoutParameter.putAttachment(PathTemplateMatch.ATTACHMENT_KEY, new PathTemplateMatch("/ready", Map.of()));
    assertThat(AbstractServerHttpHandler.databaseTag(withoutParameter, server)).isEqualTo("none");

    // No route matched at all (Studio "/" fallback, 404 probe).
    assertThat(AbstractServerHttpHandler.databaseTag(new HttpServerExchange(null), server)).isEqualTo("none");
  }
}
