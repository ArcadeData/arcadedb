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
 * Issue #7859: issue #7734 gave {@code DatabaseAbstractHandler.participatesInSessionTransaction()} its
 * {@code false} answer on the three {@code /api/v1/ts} routes and left the Prometheus, Grafana and PromQL
 * routes on the inherited {@code true} - although they extend the same base class, resolve the same
 * {@code arcadedb-session-id}, publish the same "this does not participate in your transaction" contract, and
 * raise the same two pre-engine refusals: the 413 of {@code resultSetTooLarge} when the answer would exceed
 * {@code arcadedb.server.maxResultRows}, and the 400 of a malformed {@code start}/{@code end} timestamp. Either
 * of them destroyed a transaction the client had opened with {@code /begin} and still believed it owned, and
 * left the session registered so its later {@code /commit} reported nothing lost.
 * <p>
 * Read off the handlers rather than driven over HTTP: the rollback itself is
 * {@code HttpSession.execute}'s, pinned on the failure path by
 * {@link Issue7734TimeSeriesRouteKeepsSessionTransactionTest}. What was wrong here is the ANSWER each handler
 * gives, and an answer inherited by accident is exactly what a per-handler assertion catches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7859ObservabilityRoutesKeepSessionTransactionTest {

  /** Every PromQL and Grafana route. All reads, all on {@link AbstractObservabilityHandler}. */
  @Test
  void everyPromQlAndGrafanaRouteDetachesFromTheSessionTransaction() {
    assertThat(new GetPromQLQueryHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLQueryRangeHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLSeriesHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLLabelsHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLLabelValuesHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new PostGrafanaQueryHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetGrafanaMetadataHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetGrafanaHealthHandler(null).participatesInSessionTransaction()).isFalse();
  }

  /**
   * The two Prometheus routes, which answer the same way for the same reasons but cannot inherit it: their
   * binary body spends their one superclass on {@link AbstractBinaryHttpHandler}. The write route is included
   * for the reason {@code PostTimeSeriesWriteHandler} is - it commits through the shard's own transaction
   * whatever the caller has open - so it cannot acquire the exposure the moment it grows a refusal.
   */
  @Test
  void bothPrometheusRoutesDetachFromTheSessionTransaction() {
    assertThat(new PostPrometheusReadHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new PostPrometheusWriteHandler(null).participatesInSessionTransaction()).isFalse();
  }

  /**
   * The counter-case, without which the assertions above would also pass on a base class that answered
   * {@code false} for everything: a route that DOES run inside the caller's transaction still says so.
   */
  @Test
  void aCommandRouteStillParticipates() {
    assertThat(new PostCommandHandler(null).participatesInSessionTransaction()).isTrue();
  }

  /**
   * The observability reads keep the {@code requiresTransaction()} answer they had before the shared base class
   * was introduced. It is what {@code rejectsUnresolvableSession()} derives from, so a silent change here would
   * turn a degraded session-less read into a refusal.
   */
  @Test
  void theObservabilityReadsStillWantNoAutoCommitWrapper() {
    assertThat(new GetPromQLQueryRangeHandler(null).requiresTransaction()).isFalse();
    assertThat(new PostGrafanaQueryHandler(null).requiresTransaction()).isFalse();
    assertThat(new GetGrafanaHealthHandler(null).requiresTransaction()).isFalse();
  }
}
