/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.http.RecordingPathHandler;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.DeleteApiTokenHandler;
import com.arcadedb.server.http.handler.DeleteGroupHandler;
import com.arcadedb.server.http.handler.PostCommitHandler;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Regression test for issue #7621: a handler whose {@code execute()} can block on a Raft round-trip, a retried
 * compare-and-set, a peer fan-out or a full-directory hash must not run on the Undertow IO thread, or it stalls
 * every other connection on the same selector - including the kubelet readiness/liveness probes - while it waits
 * (issue #7133).
 * <p>
 * #7133 fixed three cluster-admin handlers by name; #7621 found three more with the identical shape that the
 * sweep had missed; and issue #7861 found two more still, {@link GetClusterHandler}'s {@code ?presence=true}
 * fan-out and {@link PostBootstrapStateHandler}'s SHA-256 over every database directory. The reason the #7621
 * test did not catch them is the shape of the test itself: a hand-maintained list of six classes asserts exactly
 * what somebody had already thought of, and a handler added afterwards is invisible to it.
 * <p>
 * So the list is no longer the assertion. {@link #everyHaRaftRouteIsClassifiedAndDispatchesAccordingly()} takes
 * the inventory from {@code RaftHAPlugin.registerAPI} - the routes this module actually serves - and requires a
 * {@link Dispatch} verdict for every one of them. A new route fails the build until somebody decides whether it
 * blocks, which is the decision that was skipped twice. The by-name test below it stays for the three handlers
 * that live in {@code arcadedb-server} and are therefore outside that inventory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7621BlockingHandlersWorkerThreadTest {

  /**
   * What a route is allowed to do with the IO thread. Expressed as the OBSERVABLE answer for two concrete
   * requests rather than as "declares the override", so a handler whose answer depends on the request - the one
   * shape the previous, reflection-only check could not describe at all - has a verdict of its own.
   */
  private enum Dispatch {
    /** Reads in-memory state and answers. Must NOT pay a worker handoff on a request that is already cheap. */
    NEVER(false, false),
    /** Every request blocks, so every request is dispatched. */
    ALWAYS(true, true),
    /** Only the opt-in {@code ?presence=true} peer fan-out blocks; the cheap auto-poll does not (issue #7861). */
    ONLY_WITH_PRESENCE(false, true);

    private final boolean onAPlainRequest;
    private final boolean onAPresenceRequest;

    Dispatch(final boolean onAPlainRequest, final boolean onAPresenceRequest) {
      this.onAPlainRequest = onAPlainRequest;
      this.onAPresenceRequest = onAPresenceRequest;
    }
  }

  /**
   * Every route {@code RaftHAPlugin.registerAPI} installs, and what it may do with the IO thread. The test below
   * fails when this map and that method disagree in either direction, so adding a route means classifying it.
   * <p>
   * {@code SnapshotHttpHandler} is not an {@link AbstractServerHttpHandler} - it is a raw Undertow handler that
   * does its own dispatching - so it is classified here and skipped by the per-handler assertion, rather than
   * left out of the map where it would read as an unclassified route.
   */
  private static final Map<String, Dispatch> EXPECTED_BY_ROUTE = new LinkedHashMap<>() {{
    // Reads in-memory Raft state; ClusterAlerts passes allowLoad=false so not even a closed database is opened.
    // The fan-out behind '?presence=true' is one bootstrap-state RPC per peer at 5s each, and that one dispatches.
    put("/api/v1/cluster", Dispatch.ONLY_WITH_PRESENCE);
    // Raw Undertow handler, not an AbstractServerHttpHandler: it streams a database snapshot and dispatches itself.
    put("/api/v1/ha/snapshot/", Dispatch.ALWAYS);
    put("/api/v1/cluster/peer", Dispatch.ALWAYS);        // Raft membership change: submit and wait
    put("/api/v1/cluster/peer/", Dispatch.ALWAYS);       // idem
    put("/api/v1/cluster/leader", Dispatch.ALWAYS);      // leadership transfer: waits for the new leader
    put("/api/v1/cluster/stepdown", Dispatch.ALWAYS);    // idem
    put("/api/v1/cluster/leave", Dispatch.ALWAYS);       // idem
    put("/api/v1/cluster/verify/", Dispatch.ALWAYS);     // hashes a database and dials the leader
    put("/api/v1/cluster/resync/", Dispatch.ALWAYS);     // downloads a full snapshot
    // SHA-256 over every database directory on this node, and it may open one left deliberately closed (#7861).
    put("/api/v1/cluster/bootstrap-state", Dispatch.ALWAYS);
    // Answers from constants, with no lock, no IO and no Raft round-trip: the counter-case that keeps the
    // assertion above from degenerating into "everything dispatches".
    put("/api/v1/cluster/capabilities", Dispatch.NEVER);
    // Seeds the cluster security documents through the leader, a Raft submit-and-wait.
    put(PostSecuritySeedHandler.ROUTE, Dispatch.ALWAYS);
  }};

  private RaftHAPlugin plugin;

  @AfterEach
  void tearDown() {
    if (plugin != null)
      plugin.stopService();
  }

  /**
   * The sweep. Every route this module registers must carry a verdict, and must answer accordingly for both a
   * plain request and one that asks for the presence matrix.
   */
  @Test
  void everyHaRaftRouteIsClassifiedAndDispatchesAccordingly() {
    plugin = new RaftHAPlugin();
    final RecordingPathHandler routes = new RecordingPathHandler();
    plugin.registerAPI(null, routes);
    final Map<String, HttpHandler> registered = routes.getRegisteredHandlers();

    assertThat(registered.keySet())
        .as("every route RaftHAPlugin.registerAPI() installs needs a dispatch verdict here: a route that blocks "
            + "on the Undertow IO thread stalls every other connection on that selector, the kubelet probes "
            + "included (issues #7133, #7621, #7861)")
        .containsExactlyInAnyOrderElementsOf(EXPECTED_BY_ROUTE.keySet());

    assertAll(registered.entrySet().stream()
        .filter(e -> e.getValue() instanceof AbstractServerHttpHandler)
        .<Executable>map(e -> () -> {
          final AbstractServerHttpHandler handler = (AbstractServerHttpHandler) e.getValue();
          final Dispatch expected = EXPECTED_BY_ROUTE.get(e.getKey());

          assertThat(dispatches(handler, new HttpServerExchange(null)))
              .as("%s (%s) on a plain request", handler.getClass().getSimpleName(), e.getKey())
              .isEqualTo(expected.onAPlainRequest);
          assertThat(dispatches(handler, presenceRequest()))
              .as("%s (%s) on a '?presence=true' request", handler.getClass().getSimpleName(), e.getKey())
              .isEqualTo(expected.onAPresenceRequest);
        })
        .toArray(Executable[]::new));
  }

  /**
   * The two handlers issue #7861 names, asserted by name as well as through the sweep above. The sweep proves the
   * inventory is complete; this proves the two that were wrong are right, and says so in terms of what each one
   * does, so a future reader sees why the answers differ.
   */
  @Test
  void theTwoHandlersOfIssue7861DispatchExactlyWhenTheyBlock() {
    final GetClusterHandler status = new GetClusterHandler(null, null);
    assertThat(dispatches(status, new HttpServerExchange(null)))
        .as("the cheap auto-poll reads in-memory state: it must keep the IO-thread fast path")
        .isFalse();
    assertThat(dispatches(status, presenceRequest()))
        .as("the presence fan-out is one 5s-bounded RPC per peer, so worst case it holds its thread for peers x 5s")
        .isTrue();

    assertThat(dispatches(new PostBootstrapStateHandler(null, null), new HttpServerExchange(null)))
        .as("a SHA-256 over every database directory, which may open a database left deliberately closed")
        .isTrue();
  }

  /**
   * The three handlers that live in {@code arcadedb-server} rather than on an {@code ha-raft} route, so the
   * inventory sweep cannot reach them. Still by name, and still worth pinning: each is a Raft submit-and-wait
   * with compare-and-set retries.
   */
  @Test
  void theServerModuleBlockingHandlersStillDeclareTheOverride() {
    assertAll(Arrays.stream(new Class<?>[] {
            PostCommitHandler.class, DeleteGroupHandler.class, DeleteApiTokenHandler.class })
        .<Executable>map(handlerClass -> () -> assertThat(declaresMustExecuteOnWorkerThread(handlerClass))
            .as("%s must declare its own mustExecuteOnWorkerThread() override, not rely on the %s default of false",
                handlerClass.getSimpleName(), AbstractServerHttpHandler.class.getSimpleName())
            .isTrue())
        .toArray(Executable[]::new));
  }

  /** An exchange carrying the query parameter that turns the status poll into a peer fan-out. */
  private static HttpServerExchange presenceRequest() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.addQueryParam("presence", "true");
    return exchange;
  }

  /**
   * What {@code AbstractServerHttpHandler.handleRequest} would decide for this request. Reflective because
   * {@code mustExecuteOnWorkerThread} is {@code protected} and this test is in neither the declaring package nor
   * a subclass of it, and the question is precisely about handlers in two different packages.
   */
  private static boolean dispatches(final AbstractServerHttpHandler handler, final HttpServerExchange exchange) {
    try {
      final Method method = AbstractServerHttpHandler.class
          .getDeclaredMethod("mustExecuteOnWorkerThread", HttpServerExchange.class);
      method.setAccessible(true);
      return (boolean) method.invoke(handler, exchange);
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return fail("cannot read the dispatch decision of " + handler.getClass().getSimpleName(), e);
    }
  }

  /** True when {@code handlerClass} itself - not an ancestor - declares {@code mustExecuteOnWorkerThread()}. */
  private static boolean declaresMustExecuteOnWorkerThread(final Class<?> handlerClass) {
    try {
      return handlerClass.getDeclaredMethod("mustExecuteOnWorkerThread").getReturnType() == boolean.class;
    } catch (final NoSuchMethodException e) {
      return false;
    }
  }
}
