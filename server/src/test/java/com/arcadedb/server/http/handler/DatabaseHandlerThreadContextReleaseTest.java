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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionException;
import com.arcadedb.server.security.ServerSecurityDatabaseUser;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A request that is refused while its handler binds the request to the pooled worker thread leaves nothing bound to that thread:
 * the next request served by the same thread must not find the transaction of a client session, and a client session's
 * transaction must still be usable by its owner afterwards.
 * <p>
 * The test thread plays the pooled worker thread: every request below runs on it, one after the other, as the same worker would
 * serve them.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class DatabaseHandlerThreadContextReleaseTest {
  private static final String DATABASE_NAME = "ThreadContextReleaseDb";
  private static final String DATABASE_PATH = "./target/databases/" + DATABASE_NAME;
  private static final String TYPE_NAME     = "Item";

  /** A real server user, allowed on every database, bound to no server so it answers no database user. */
  private static final class UnboundUser extends ServerSecurityUser {
    UnboundUser() {
      super(null, new JSONObject().put("name", "tester").put("databases", new JSONObject().put(SecurityManager.ANY, new JSONArray().put("admin"))));
    }

    @Override
    public ServerSecurityDatabaseUser getDatabaseUser(final Database database) {
      return null;
    }
  }

  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  private DatabaseInternal   database;
  private HttpServer         httpServer;
  private ServerSecurityUser user;
  private boolean            analysisFails;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = (DatabaseInternal) factory.create();
    database.getSchema().createVertexType(TYPE_NAME);

    final FakeArcadeDBServer server = FakeArcadeDBServer.create().online();
    server.database(DATABASE_NAME, new ServerDatabase(server, database));
    httpServer = HTTP_SERVERS.of(server);
    user = new UnboundUser();
    analysisFails = false;
    DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      if (database.isOpen())
        database.drop();
    }
  }

  /** A handler whose command creates one record in whatever transaction the request runs in. */
  private DatabaseAbstractHandler writingHandler() {
    return new DatabaseAbstractHandler(httpServer) {
      @Override
      protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database db,
          final JSONObject payload) {
        db.newVertex(TYPE_NAME).set("name", "item").save();
        return new ExecutionResponse(200, "{\"result\":\"done\"}");
      }

      /** Not the gate: it asks the connection of the exchange which thread the request is on, and a test exchange has none. */
      @Override
      protected boolean goesThroughAdmissionGate() {
        return false;
      }

      @Override
      protected boolean managesItsOwnTransactions(final DatabaseInternal db, final JSONObject payload) {
        if (analysisFails)
          throw new IllegalStateException("The command cannot be analysed");
        return false;
      }
    };
  }

  private HttpServerExchange exchange(final String sessionId) {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.addQueryParam("database", DATABASE_NAME);
    if (sessionId != null)
      exchange.getRequestHeaders().put(new HttpString(ARCADEDB_SESSION_ID), sessionId);
    return exchange;
  }

  private HttpSession beginSession() throws Exception {
    final HttpServerExchange exchange = exchange(null);
    new PostBeginHandler(httpServer).execute(exchange, user, null);
    final HttpSession session = httpServer.getSessionManager()
        .getSessionById(user, exchange.getResponseHeaders().getFirst(ARCADEDB_SESSION_ID));
    assertThat(session).isNotNull();
    return session;
  }

  private void commitSession(final HttpSession session) throws Exception {
    final PostCommitHandler handler = new PostCommitHandler(httpServer) {
      @Override
      protected boolean goesThroughAdmissionGate() {
        return false;
      }
    };
    assertThat(handler.execute(exchange(session.id), user, null).getCode()).isEqualTo(204);
  }

  private void assertThreadHoldsNoContext() {
    assertThat(DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath())).as("nothing of the request stays on the thread")
        .isNull();
  }

  /** The request fails on a value of its body, after the session was bound to the thread: the session stays usable by its owner. */
  private void assertSessionSurvives(final HttpSession session, final JSONObject invalidPayload) throws Exception {
    assertThat(writingHandler().execute(exchange(session.id), user, new JSONObject()).getCode()).isEqualTo(200);
    assertThreadHoldsNoContext();
    assertThat(session.transaction.isActive()).isTrue();

    assertThatThrownBy(() -> writingHandler().execute(exchange(session.id), user, invalidPayload)).isInstanceOf(JSONException.class);

    assertThreadHoldsNoContext();
    assertThat(session.transaction.isActive()).as("the transaction of the session is untouched by the refused request").isTrue();

    // The next request served by the same thread, whatever it is, does not meet the transaction of the session
    assertThat(writingHandler().execute(exchange(null), user, new JSONObject()).getCode()).isEqualTo(200);
    assertThat(session.transaction.isActive()).as("a request of another client leaves the session alone").isTrue();
    assertThreadHoldsNoContext();

    assertThat(writingHandler().execute(exchange(session.id), user, new JSONObject()).getCode()).isEqualTo(200);
    commitSession(session);
    // two records of the session and the one of the other request
    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(3);
  }

  @Test
  void aSessionRequestWithANonNumericRetriesValueLeavesNothingOnTheThread() throws Exception {
    assertSessionSurvives(beginSession(), new JSONObject().put("retries", "many"));
  }

  @Test
  void aSessionRequestWithANonBooleanAutoCommitValueLeavesNothingOnTheThread() throws Exception {
    assertSessionSurvives(beginSession(), new JSONObject().put("autoCommit", "maybe"));
  }

  @Test
  void aRequestOutsideASessionWithANonNumericRetriesValueLeavesNothingOnTheThread() {
    assertThatThrownBy(() -> writingHandler().execute(exchange(null), user, new JSONObject().put("retries", "many")))
        .isInstanceOf(JSONException.class);

    assertThreadHoldsNoContext();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countType(TYPE_NAME, true)).isZero();
  }

  @Test
  void aRequestOutsideASessionWithANonBooleanAutoCommitValueLeavesNothingOnTheThread() {
    assertThatThrownBy(() -> writingHandler().execute(exchange(null), user, new JSONObject().put("autoCommit", "maybe")))
        .isInstanceOf(JSONException.class);

    assertThreadHoldsNoContext();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aRequestWhoseCommandCannotBeAnalysedLeavesNothingOnTheThread() {
    analysisFails = true;

    assertThatThrownBy(() -> writingHandler().execute(exchange(null), user, new JSONObject())).isInstanceOf(IllegalStateException.class);

    assertThreadHoldsNoContext();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aRequestNamingAnUnknownSessionLeavesNothingOnTheThread() {
    assertThatThrownBy(() -> writingHandler().execute(exchange("AS-unknown"), user, new JSONObject()))
        .isInstanceOf(HttpSessionException.class);

    assertThreadHoldsNoContext();
  }

  /**
   * A handler that does not run its work in a transaction of its own binds the user of the request to the context the thread already
   * has. A transaction found open there belongs to no request in flight, so the request does not run in it.
   */
  @Test
  void aTransactionFoundOpenOnTheThreadIsNotAdoptedByAnotherRequest() throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        DatabaseContext.INSTANCE.init(database);
        database.begin();
        database.newVertex(TYPE_NAME).set("name", "left open").save();
        assertThat(database.isTransactionActive()).isTrue();

        writingHandler().bindPrincipal(user, database);

        final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
        assertThat(context).isNotNull();
        assertThat(context.hasActiveTransaction()).isFalse();
        assertThat(database.isTransactionActive()).isFalse();
      } catch (final Throwable t) {
        failure.set(t);
      } finally {
        DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
      }
    }, "pooled-worker-test");
    worker.start();
    worker.join();

    assertThat(failure.get()).isNull();
    assertThat(database.countType(TYPE_NAME, true)).as("what the abandoned transaction held was not committed").isZero();
  }

  @Test
  void validRequestsKeepTheirTransactionBehaviour() throws Exception {
    final HttpSession session = beginSession();

    assertThat(writingHandler().execute(exchange(session.id), user, new JSONObject().put("retries", 2)).getCode()).isEqualTo(200);
    assertThat(session.transaction.isActive()).isTrue();
    assertThreadHoldsNoContext();
    commitSession(session);
    assertThreadHoldsNoContext();
    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(1);
    DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

    assertThat(writingHandler().execute(exchange(null), user, new JSONObject().put("retries", 2)).getCode()).isEqualTo(200);
    assertThreadHoldsNoContext();
    assertThat(database.countType(TYPE_NAME, true)).as("committed by the request itself").isEqualTo(2);
  }
}
