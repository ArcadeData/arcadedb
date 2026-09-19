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

import com.arcadedb.server.http.HttpServer;

/**
 * Base handler for the database-scoped observability read routes - PromQL, Grafana and Prometheus remote-read -
 * which answer the same two questions the same way and had been answering the second one by accident (issue
 * #7859).
 * <p>
 * {@link #requiresTransaction()} is <b>false</b>. Every one of these routes is a read, so the auto-commit wrapper
 * would only add a commit with nothing to commit; as a consequence an {@code arcadedb-session-id} this server
 * cannot resolve degrades to a session-less read rather than being refused, which is what
 * {@link DatabaseAbstractHandler#rejectsUnresolvableSession()} derives from this answer.
 * <p>
 * {@link #participatesInSessionTransaction()} is <b>false</b>, which is the answer issue #7734 gave the three
 * {@code /api/v1/ts} routes and this family inherited as {@code true} by omission. These routes resolve the
 * caller's session for its LOCK, PRINCIPAL and IDLE CLOCK, and read through its transaction - but they never
 * write into it, so a refusal they raise is not evidence that the transaction is unusable. Both of the refusals
 * they actually raise are pre-engine: a 413 from {@code resultSetTooLarge} when the answer would exceed
 * {@code arcadedb.server.maxResultRows}, and a 400 from a malformed {@code start}/{@code end}/{@code step}
 * parameter. Left on the default, either of them destroyed a transaction the client had opened with
 * {@code /begin} and still believed it owned - and left the session registered, so its later {@code /commit}
 * reported nothing lost.
 * <p>
 * Both answers are properties of the FAMILY rather than of the individual failure, for the reason
 * {@link DatabaseAbstractHandler#participatesInSessionTransaction()} records: the contract these routes publish
 * is "an observability read does not touch your transaction", not "these two errors happen to be harmless". A
 * per-exception rule would have to be re-derived every time one of them grew a new refusal, which is exactly how
 * this family came to be left behind by #7734.
 * <p>
 * The two Prometheus routes answer the same way but cannot extend this class: they carry a binary body and Java
 * has one superclass to spend, so they inherit {@link AbstractBinaryHttpHandler} and override both answers
 * themselves.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractObservabilityHandler extends DatabaseAbstractHandler {
  protected AbstractObservabilityHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected boolean participatesInSessionTransaction() {
    return false;
  }
}
