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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.UnstartedHttpServers;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.security.ServerSecurityDatabaseUser;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A request that forwarded one write to the leader and then failed on this node is answered with the leader's answer: the write
 * is durable on the leader, so a retryable refusal in its place makes the client replay it, and the replay applies it twice.
 */
class DatabaseAbstractHandlerLeaderAnswerTest {

  /**
   * A real server user, allowed on every database, bound to no server so it answers no database user: the request runs as the
   * engine does when no user is bound (a hand-written fake rather than a mock of the server type, issue #9464).
   */
  private static final class UnboundUser extends ServerSecurityUser {
    UnboundUser() {
      super(null, new JSONObject().put("name", "tester").put("databases", new JSONObject().put(SecurityManager.ANY, new JSONArray().put("admin"))));
    }

    @Override
    public ServerSecurityDatabaseUser getDatabaseUser(final Database database) {
      return null;
    }
  }

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  @Test
  void aFailureAfterOneCommittedForwardIsAnsweredWithTheLeadersAnswer() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("{\"result\":[{\"k\":7}]}");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    final boolean answered = DatabaseAbstractHandler.answerWithTheLeadersAnswer(response,
        new NeedRetryException("Database 'db' is being replaced from the leader"));

    assertThat(answered).isTrue();
    assertThat(response.get().getCode()).isEqualTo(200);
    assertThat(response.get().getResponse()).isEqualTo("{\"result\":[{\"k\":7}]}");
  }

  /** The handler had rendered an answer already: it is the request's answer, the leader's is only what makes the failure moot. */
  @Test
  void anAnswerTheHandlerAlreadyRenderedIsKept() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("from the leader");
    final ExecutionResponse rendered = new ExecutionResponse(200, "rendered");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>(rendered);

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new IllegalStateException("late failure"))).isTrue();
    assertThat(response.get()).isSameAs(rendered);
  }

  @Test
  void aFailureOfARequestThatForwardedNothingIsRaisedAsItIs() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new NeedRetryException("refused"))).isFalse();
    assertThat(response.get()).isNull();
  }

  @Test
  void aFailureOfARequestThatForwardedSeveralWritesIsRaisedAsItIs() {
    ForwardedRequestIdContext.armCommittedWriteAnswer();
    ForwardedRequestIdContext.publishCommittedWriteAnswer("first");
    ForwardedRequestIdContext.publishCommittedWriteAnswer("second");
    final AtomicReference<ExecutionResponse> response = new AtomicReference<>();

    assertThat(DatabaseAbstractHandler.answerWithTheLeadersAnswer(response, new NeedRetryException("refused"))).isFalse();
    assertThat(response.get()).isNull();
  }

  // ---------------------------------------------------------------------------------------------------------------------------
  // Through DatabaseAbstractHandler.execute(): the catch, the arming and the clearing of the request's state
  // ---------------------------------------------------------------------------------------------------------------------------

  /** One more kind of failure at a time: what the handler's own execute throws after the forward published its answer. */
  @Nested
  class ThroughExecute {
    private static final String DATABASE_PATH = "./target/databases/DatabaseAbstractHandlerLeaderAnswerTest";
    private static final String LEADERS_BODY  = "{\"result\":[{\"k\":7}]}";

    @RegisterExtension
    static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

    private DatabaseInternal  database;
    private FakeArcadeDBServer server;
    private HttpServer        httpServer;
    private ServerSecurityUser user;
    private int               publishes;
    private int               refusedForwardsAfter;
    private boolean           executedLocally;
    private boolean           oneCommandRoute;
    private RuntimeException  failure;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();
      database = (DatabaseInternal) factory.create();
      server = FakeArcadeDBServer.create().online();
      // An HA database as the handler sees one: the real database behind a proxy that is also a HAReplicatedDatabase
      final DatabaseInternal haDatabase = (DatabaseInternal) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] { DatabaseInternal.class, HAReplicatedDatabase.class }, (proxy, method, args) -> {
            if (method.getDeclaringClass() == HAReplicatedDatabase.class)
              return switch (method.getName()) {
                case "isLeader" -> false;
                case "getCommitIndexBookmark", "getLastAppliedIndex" -> -1L;
                default -> method.isDefault() ? InvocationHandler.invokeDefault(proxy, method, args) : null;
              };
            try {
              return method.invoke(database, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
      server.database("ha", new ServerDatabase(server, haDatabase));
      server.database("plain", new ServerDatabase(server, database));
      httpServer = HTTP_SERVERS.of(server);
      user = new UnboundUser();
      publishes = 1;
      refusedForwardsAfter = 0;
      executedLocally = false;
      // A route whose body is one command, as POST /command: the only kind the leader's answer can stand for
      oneCommandRoute = true;
      failure = null;
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private ExecutionResponse run(final String databaseName, final String sessionId) throws Exception {
      final DatabaseAbstractHandler handler = new DatabaseAbstractHandler(httpServer) {
        @Override
        protected boolean commandForwardIsWholeRequest() {
          return oneCommandRoute;
        }

        @Override
        protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database db,
            final JSONObject payload) {
          if (executedLocally)
            ForwardedRequestIdContext.markExecutedLocally();
          // What the forward does on the way: the leader answered, the write is durable there
          for (int i = 0; i < publishes; i++) {
            ForwardedRequestIdContext.writeForwardStarted();
            ForwardedRequestIdContext.publishCommittedWriteAnswer(LEADERS_BODY);
            ForwardedRequestIdContext.writeForwardEnded();
            // As the forward does (issue #12635): the auto-commit loop around this block must not replay a write the leader committed
            DatabaseContext.INSTANCE.getContextIfExists(db.getDatabasePath()).reportWriteCommittedByAnotherNode();
          }
          // A forward the leader never answered (refused, unreachable), after the ones above
          for (int i = 0; i < refusedForwardsAfter; i++) {
            ForwardedRequestIdContext.writeForwardStarted();
            ForwardedRequestIdContext.writeForwardEnded();
          }
          if (failure != null)
            throw failure;
          return new ExecutionResponse(200, "{\"result\":\"rendered\"}");
        }
      };
      final HttpServerExchange exchange = mock(HttpServerExchange.class);
      final Deque<String> name = new ArrayDeque<>();
      name.add(databaseName);
      final Map<String, Deque<String>> parameters = new HashMap<>();
      parameters.put("database", name);
      when(exchange.getQueryParameters()).thenReturn(parameters);
      final HeaderMap requestHeaders = new HeaderMap();
      if (sessionId != null)
        requestHeaders.put(new HttpString(ARCADEDB_SESSION_ID), sessionId);
      when(exchange.getRequestHeaders()).thenReturn(requestHeaders);
      when(exchange.getResponseHeaders()).thenReturn(new HeaderMap());
      return handler.execute(exchange, user, null);
    }

    @Test
    void aRetryableRefusalAfterOneCommittedForwardIsAnsweredWithTheLeadersAnswer() throws Exception {
      failure = new NeedRetryException("Database 'ha' is being replaced from the leader");
      final ExecutionResponse response = run("ha", null);

      assertThat(response.getCode()).isEqualTo(200);
      assertThat(response.getResponse()).isEqualTo(LEADERS_BODY);
      assertThat(ForwardedRequestIdContext.takeCommittedWriteAnswer()).as("the pooled thread is left clean").isNull();
      assertThat(database.isTransactionActive()).as("the failed request left no transaction open").isFalse();
    }

    @Test
    void aDatabaseReopenedUnderTheRequestAfterOneCommittedForwardIsAnsweredWithTheLeadersAnswer() throws Exception {
      failure = new DatabaseIsReopeningException("Database 'ha' is being reopened");
      assertThat(run("ha", null).getResponse()).isEqualTo(LEADERS_BODY);
    }

    /** Not every failure after a forward says the write is done and only its answer got lost: only the retryable refusals do. */
    @Test
    void aFailureThatIsNotARetryableRefusalIsRaisedEvenAfterAForward() {
      for (final RuntimeException other : new RuntimeException[] { new SecurityException("not allowed"),
          new IllegalStateException("rendering failed"), new DatabaseIsClosedException("ha") }) {
        failure = other;
        assertThatThrownBy(() -> run("ha", null)).as("%s", other.getClass().getSimpleName()).isSameAs(other);
        assertThat(ForwardedRequestIdContext.takeCommittedWriteAnswer()).as("the pooled thread is left clean").isNull();
      }
    }

    @Test
    void aRefusalAfterSeveralForwardsOrNoneIsRaised() {
      failure = new NeedRetryException("refused");
      publishes = 2;
      assertRaisedAsNotReplayable(() -> run("ha", null));
      publishes = 0;
      assertThatThrownBy(() -> run("ha", null)).isSameAs(failure);
    }

    /**
     * A refusal reaches the request boundary under the wrapper the engine put around it, and the error mapping answers it 503 all the
     * same (it looks down the cause chain): the leader's answer covers that shape too, or the client replays the write.
     */
    @Test
    void aRetryableRefusalUnderAWrapperIsAnsweredWithTheLeadersAnswer() throws Exception {
      failure = new TransactionException("Error on commit", new NeedRetryException("Database 'ha' is being replaced from the leader"));
      assertThat(run("ha", null).getResponse()).isEqualTo(LEADERS_BODY);

      failure = new CommandExecutionException("Error on command", new DatabaseIsReopeningException("Database 'ha' is being reopened"));
      assertThat(run("ha", null).getResponse()).isEqualTo(LEADERS_BODY);
    }

    /** The wrapper is not what decides: a wrapped failure that is not a refusal to retry is raised, as it is unwrapped. */
    @Test
    void aWrappedFailureThatIsNotARetryableRefusalIsRaised() {
      failure = new TransactionException("Error on commit", new IllegalStateException("rendering failed"));
      assertThatThrownBy(() -> run("ha", null)).isSameAs(failure);
    }

    /**
     * What the error mapping answers decides, not what the cause chain holds: a security refusal that wraps a retryable one is a 403
     * there, and turning it into a 200 would hide that the request was refused.
     */
    @Test
    void aSecurityRefusalWrappingARetryableOneIsRaised() {
      failure = new SecurityException("denied", new NeedRetryException("Database 'ha' is being replaced from the leader"));
      assertThatThrownBy(() -> run("ha", null)).isSameAs(failure);
    }

    /**
     * Issue #12889, the partial success: a forward the leader answered, and then one it did not (refused at the entry gate, a
     * snapshot-install 503, a refused connection). The first answer is the answer to a part of the request, and the 200 it would give
     * hides that the second write never ran, which the client will not retry.
     */
    @Test
    void aForwardThatFailedAfterAnAnsweredOneIsRaised() {
      failure = new NeedRetryException("Database 'ha' is being replaced from the leader");
      refusedForwardsAfter = 1;
      assertRaisedAsNotReplayable(() -> run("ha", null));
      assertThat(ForwardedRequestIdContext.takeCommittedWriteAnswer()).as("the pooled thread is left clean").isNull();
    }

    /**
     * A command this node ran itself is part of the request: the leader's answer to the one write it forwarded does not say what
     * the rest did, and a 200 for it loses the local part of the request silently.
     */
    @Test
    void aRequestThatExecutedACommandLocallyIsRaised() {
      failure = new NeedRetryException("Database 'ha' is being replaced from the leader");
      executedLocally = true;
      assertRaisedAsNotReplayable(() -> run("ha", null));
    }

    /**
     * Only a route whose body is one command can be answered with the answer to one forwarded write: on any other the request asks
     * for more than that write.
     */
    @Test
    void aRouteThatIsNotOneCommandNeverArms() {
      oneCommandRoute = false;
      failure = new NeedRetryException("refused");
      assertRaisedAsNotReplayable(() -> run("ha", null));
    }

    /** The answer of one request is not the answer of the next on the same pooled thread: arming starts from nothing. */
    @Test
    void theAnswerOfOneRequestIsNotTheAnswerOfTheNextOnTheSameThread() throws Exception {
      failure = new NeedRetryException("refused");
      assertThat(run("ha", null).getResponse()).isEqualTo(LEADERS_BODY);

      publishes = 0;
      assertThatThrownBy(() -> run("ha", null)).as("the second request forwarded nothing").isSameAs(failure);
    }

    /**
     * The refusal is raised by the auto-commit loop around the request, which a committed forward stops from replaying the block:
     * it comes as the not-replayable failure with the refusal suppressed under it (issue #13251), and that is still the refusal
     * the leader's answer stands in for.
     */
    @Test
    void aRefusalTheRetryLoopRaisedAsNotReplayableIsAnsweredWithTheLeadersAnswer() throws Exception {
      final NeedRetryException refusal = new NeedRetryException("Database 'ha' is being replaced from the leader");
      failure = new TransactionException("Transaction block committed part of its work");
      failure.addSuppressed(refusal);
      assertThat(run("ha", null).getResponse()).isEqualTo(LEADERS_BODY);
    }

    /** Raised once a write the leader committed: as the failure no client replays, with the refusal under it (issue #13251). */
    private void assertRaisedAsNotReplayable(final ThrowingCallable request) {
      assertThatThrownBy(request).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class)
          .satisfies(raised -> assertThat(raised.getSuppressed()).contains(failure));
      assertThat(ForwardedRequestIdContext.takeCommittedWriteAnswer()).as("the pooled thread is left clean").isNull();
    }

    @Test
    void aRequestThatSucceedsLeavesNothingBehind() throws Exception {
      assertThat(run("ha", null).getResponse()).isEqualTo("{\"result\":\"rendered\"}");
      assertThat(ForwardedRequestIdContext.takeCommittedWriteAnswer()).isNull();
    }

    @Test
    void aDatabaseThatIsNotReplicatedNeverArms() {
      failure = new NeedRetryException("refused");
      assertRaisedAsNotReplayable(() -> run("plain", null));
    }

    @Test
    void aRequestInsideASessionNeverArms() {
      final HttpSession session = httpServer.getSessionManager().createSession(user, null);
      failure = new NeedRetryException("refused");
      assertThatThrownBy(() -> run("ha", session.id)).isSameAs(failure);
    }
  }
}
