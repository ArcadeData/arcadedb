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

import com.arcadedb.database.TransactionContext;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8063: follow-up to #7859. The three {@code /api/v1/vector/{database}/*} routes extend
 * {@link DatabaseAbstractHandler} directly and never overrode {@link DatabaseAbstractHandler#participatesInSessionTransaction()},
 * so they inherited its default of {@code true} - the same gap #7859 closed for the TimeSeries and observability
 * routes. They are pure reads that raise {@link IllegalArgumentException} on malformed client input (a missing or
 * non-numeric {@code k}, an unknown index name) before anything reaches the engine, which
 * {@code AbstractServerHttpHandler}'s error mapping turns into a 400 - and on the inherited default, that 400 still
 * reaches {@code HttpSession.execute}'s rollback arm and destroys a transaction the client opened with
 * {@code /begin} and still believes it owns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8063VectorSearchRoutesKeepSessionTransactionTest {

  private HttpSessionManager manager;
  private ServerSecurityUser user;
  private TransactionContext transaction;
  private HttpSession        session;

  @BeforeEach
  void setUp() {
    manager = new HttpSessionManager(60_000);
    user = mock(ServerSecurityUser.class);
    transaction = mock(TransactionContext.class);
    when(transaction.isActive()).thenReturn(true);
    session = manager.createSession(user, transaction);
  }

  @AfterEach
  void tearDown() {
    if (manager != null)
      manager.close();
  }

  @Test
  void theThreeVectorSearchRoutesDeclareThemselvesIndependentOfTheSessionTransaction() {
    assertThat(new PostVectorSearchHandler(null).participatesInSessionTransaction())
        .as("a kNN read detaches from the session").isFalse();
    assertThat(new PostVectorFullTextSearchHandler(null).participatesInSessionTransaction())
        .as("a full-text read detaches from the session").isFalse();
    assertThat(new PostVectorHybridSearchHandler(null).participatesInSessionTransaction())
        .as("a hybrid read detaches from the session").isFalse();
  }

  /**
   * The defect itself, reproduced through the same {@code HttpSession.execute} path the routes run inside: a
   * malformed-input failure on a non-participating route must not roll back the caller's session transaction.
   */
  @Test
  void aMalformedRequestOnAVectorSearchRouteLeavesTheSessionTransactionAlone() {
    assertThatThrownBy(() -> session.execute(user, () -> {
      throw new IllegalArgumentException("k must be a positive integer");
    }, new PostVectorSearchHandler(null).participatesInSessionTransaction()))
        .isInstanceOf(IllegalArgumentException.class);

    verify(transaction, never()).rollback();
  }
}
