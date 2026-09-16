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

import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7722: two PromQL discovery endpoints answered a full-range scan of every sample on an Undertow IO
 * thread, and the two PromQL evaluation endpoints ran an expression on one.
 * <p>
 * An IO thread serves many connections at once. The cost of parking one is not paid by the caller whose
 * label-values request is scanning a year of samples - it is paid by every unrelated connection multiplexed onto
 * that thread. The work is unbounded in the size of the SERIES rather than in the size of the request, so there
 * is no request the server can refuse in order to make it bounded, and
 * {@code GET /prom/api/v1/label/{name}/values} is on Grafana's variable-refresh path: one templated variable
 * issues it on every dashboard load and every refresh interval.
 * <p>
 * The assertion is the DISPATCH, not a latency: what went wrong was which thread the work ran on, and a timing
 * bound would say nothing about that while being a coin flip on a loaded machine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7722PromQLWorkerThreadDispatchTest {

  /**
   * The two discovery endpoints whose cost is the whole series, and the two evaluation endpoints whose cost is
   * the expression's range. The handler-wide answer, so it holds for the session-less request Grafana and
   * Prometheus actually send - neither ever sets {@code arcadedb-session-id} - and not only for one that names a
   * session.
   */
  @Test
  void everyScanningPromQLHandlerRunsOnAWorkerThread() {
    assertThat(new GetPromQLLabelValuesHandler(null).mustExecuteOnWorkerThread())
        .as("a full-range scan of every sample carrying the label").isTrue();
    assertThat(new GetPromQLSeriesHandler(null).mustExecuteOnWorkerThread())
        .as("a full-range scan of every type the match[] selector resolves to").isTrue();
    assertThat(new GetPromQLQueryHandler(null).mustExecuteOnWorkerThread())
        .as("evaluates a PromQL expression over the samples it selects").isTrue();
    assertThat(new GetPromQLQueryRangeHandler(null).mustExecuteOnWorkerThread())
        .as("evaluates that expression once per step of a range").isTrue();
  }

  /**
   * The per-request override defaults to the handler-wide one, so a session-less request gets the same answer.
   * This is the half issue #7722 is about: the session-carrying case was already handled, and the case Grafana
   * and Prometheus exercise was not.
   */
  @Test
  void theDispatchDoesNotDependOnTheRequestCarryingASession() {
    final HttpServerExchange sessionless = new HttpServerExchange(null);

    assertThat(new GetPromQLLabelValuesHandler(null).mustExecuteOnWorkerThread(sessionless)).isTrue();
    assertThat(new GetPromQLSeriesHandler(null).mustExecuteOnWorkerThread(sessionless)).isTrue();
    assertThat(new GetPromQLQueryHandler(null).mustExecuteOnWorkerThread(sessionless)).isTrue();
    assertThat(new GetPromQLQueryRangeHandler(null).mustExecuteOnWorkerThread(sessionless)).isTrue();
  }

  /**
   * The counter-case that keeps the assertion above meaningful: the label-NAMES endpoint reads the schema and
   * nothing else - it enumerates the TAG columns of every TimeSeries type - so it stays on the IO thread. If
   * "everything PromQL dispatches" were the rule, the tests above would pass without saying anything about
   * scanning.
   */
  @Test
  void theSchemaOnlyDiscoveryEndpointStaysOnTheIoThread() {
    assertThat(new GetPromQLLabelsHandler(null).mustExecuteOnWorkerThread())
        .as("label names come from the schema, with no sample read at all").isFalse();
  }
}
