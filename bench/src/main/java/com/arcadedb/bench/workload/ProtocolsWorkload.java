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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.protocol.Parity;
import com.arcadedb.bench.workload.protocol.Protocol;
import com.arcadedb.bench.workload.protocol.ProtocolClient;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * The same openCypher shapes over HTTP, gRPC, Bolt and PostgreSQL, with 1 and 16 clients: a point lookup, the friends at one hop,
 * a page of 1,000 users, an UNWIND MERGE of 500 rows, a parameter of 15 vectors of 768 floats and a transaction of 10 MERGEs.
 * Each case is {@code <shape>-<protocol>-<clients>c}.
 * <p>
 * Before a non-HTTP case of a read shape is measured, its rows for 20 fixed parameters are compared with the rows HTTP answers
 * ({@link Parity#diff}); a difference fails the case with the diff as its error, so a protocol is never timed while it answers
 * something else. A write shape writes 500 (or 10) rows twice, first creating and then updating them, and reads them back over
 * HTTP after each write, on every protocol. Only scalar, list and map columns are returned, never nodes or relationships.
 * <p>
 * The write shapes MERGE ids at and above {@link #WRITE_BASE}, beyond the users of any scale. Each client of a case updates its
 * own ids (500 for merge500, 10 for tx10), alternating two names so every operation changes the rows. {@code prepare} creates the
 * rows of both name variants untimed, so every timed repetition updates rows that exist, whatever the warm-up count, and the
 * dataset does not grow rep after rep. The case teardown deletes them.
 * <p>
 * Every protocol of a shape runs the same operations: the parameters come from a seed of the shape and the client count, not of
 * the protocol. Parameters are built in {@code prepare}, and clients are opened before the timed window of each repetition.
 */
public class ProtocolsWorkload implements Workload {
  public static final String DATABASE = "bench_protocols";

  /** The first id the write shapes use; the users of every scale are below it. */
  static final long   WRITE_BASE    = 100_000_000L;
  static final String CLIENT_THREAD = "bench-protocol-client-";

  private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolsWorkload.class);

  private static final int[] CLIENT_COUNTS     = { 1, 16 };
  private static final int   FRIENDS_PER_USER  = 5;
  private static final int   PAGE              = 1_000;
  private static final int   MERGE_ROWS        = 500;
  private static final int   TX_STATEMENTS     = 10;
  private static final int   VECTORS           = 15;
  private static final int   DIMENSIONS        = 768;
  private static final int   PARITY_PARAMS     = 20;
  private static final int   BATCH             = 1_000;
  private static final long  TX_BASE           = WRITE_BASE + 100_000L;
  private static final long  PARITY_WRITE_BASE = WRITE_BASE + 900_000L;
  private static final String SQL              = "sql";
  private static final String WARM_UP          = "RETURN 1 AS x";
  private static final long   JOIN_AFTER_INTERRUPT_MS = 10_000L;

  /** The openCypher shapes; the case name is the form used in case names. */
  public enum Shape {
    POINT("point", "MATCH (u:User {id: $id}) RETURN u.id AS id, u.name AS name"),
    HOP1("hop1", "MATCH (u:User {id: $id})-[:FriendOf]->(f:User) RETURN f.id AS id ORDER BY id"),
    PAGE_1000("page1000", "MATCH (u:User) WHERE u.id >= $from RETURN u.id AS id, u.name AS name ORDER BY id LIMIT " + PAGE),
    UNWIND_MERGE_500("merge500", "UNWIND $rows AS r MERGE (u:User {id: r.id}) SET u.name = r.name"),
    VECTOR_PARAM("vector-param", "UNWIND $vectors AS v RETURN size(v) AS n, v[0] AS first"),
    TX_10("tx10", "MERGE (u:User {id: $id}) SET u.name = $name");

    private final String caseName;
    private final String cypher;

    Shape(final String caseName, final String cypher) {
      this.caseName = caseName;
      this.cypher = cypher;
    }

    public String caseName() {
      return caseName;
    }

    public String cypher() {
      return cypher;
    }

    boolean writes() {
      return this == UNWIND_MERGE_500 || this == TX_10;
    }
  }

  /** A case name taken apart. */
  record Case(Shape shape, Protocol protocol, int clients) {
    String name() {
      return shape.caseName() + "-" + protocol.caseName() + "-" + clients + "c";
    }

    static Case parse(final String caseName) {
      final int clientsDash = caseName.lastIndexOf('-');
      final int protocolDash = clientsDash > 0 ? caseName.lastIndexOf('-', clientsDash - 1) : -1;
      if (protocolDash <= 0 || !caseName.endsWith("c"))
        throw new IllegalArgumentException("Unknown case: " + caseName);
      final String shapeName = caseName.substring(0, protocolDash);
      final String clientsText = caseName.substring(clientsDash + 1, caseName.length() - 1);
      for (final Shape shape : Shape.values())
        if (shape.caseName.equals(shapeName))
          for (final int clients : CLIENT_COUNTS)
            if (Integer.toString(clients).equals(clientsText)) {
              final Protocol protocol;
              try {
                protocol = Protocol.ofCaseName(caseName.substring(protocolDash + 1, clientsDash));
              } catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown case: " + caseName, e);
              }
              return new Case(shape, protocol, clients);
            }
      throw new IllegalArgumentException("Unknown case: " + caseName);
    }
  }

  // the dataset, read once per workload run
  private String             buildFailure;
  private List<List<Double>> vectors;

  // the case being run, built by prepare
  private Case                  current;
  private Map<String, Object>[] opParams;
  private Map<String, Object>[][] mergeParams;
  private List<Map<String, Object>>[][] txParams;
  private List<String>          txStatements;
  private boolean               wrote;

  /** Test-only: the read shape whose HTTP reference rows get one value changed, so the parity check of that shape must fail. */
  Shape corruptHttpReferenceOf;

  static int users(final Scale scale) {
    return switch (scale) {
      case XS -> 1_000;
      case S -> 20_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  /** The operations of one repetition, in total over the clients. */
  static int operations(final Scale scale, final Shape shape) {
    return switch (scale) {
      case XS -> shape == Shape.UNWIND_MERGE_500 ? 20 : 100;
      case S, M, L -> 1_000;
    };
  }

  /** Whether a protocol's client carries a shape's statement and parameters as they are. */
  static boolean carries(final Protocol protocol, final Shape shape) {
    // every shape binds scalars, a list of maps or a list of float lists, which all four clients carry (ProtocolClientIT); the
    // one gap measured is a top-level map parameter over PostgreSQL, which no shape uses
    return true;
  }

  /** The cases at a scale: every shape a protocol carries, at 1 and 16 clients. */
  static List<String> caseNames(final Scale scale) {
    return caseNames(scale, ProtocolsWorkload::carries);
  }

  /** The cases at a scale for the (protocol, shape) pairs {@code carries} accepts, at 1 and 16 clients. */
  static List<String> caseNames(final Scale scale, final BiPredicate<Protocol, Shape> carries) {
    final List<String> names = new ArrayList<>();
    for (final Shape shape : Shape.values())
      for (final Protocol protocol : Protocol.values())
        if (carries.test(protocol, shape))
          for (final int clients : CLIENT_COUNTS)
            names.add(new Case(shape, protocol, clients).name());
    return names;
  }

  @Override
  public String name() {
    return "protocols";
  }

  @Override
  public List<String> cases() {
    return caseNames(Scale.S);
  }

  @Override
  public Map<String, String> serverSettings() {
    return Map.of(
        "arcadedb.server.plugins", "PostgresProtocolPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin",
        "arcadedb.grpc.enabled", "true",
        "arcadedb.grpc.port", "50051",
        "arcadedb.grpc.mode", "standard");
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    final Case c = Case.parse(caseName);
    if (!carries(c.protocol(), c.shape()))
      throw new IllegalArgumentException("Case " + caseName + " is not carried by " + c.protocol().caseName());
    if (buildFailure != null)
      throw new IllegalStateException("dataset build failed earlier: " + buildFailure);
    final int users = users(ctx.scale());
    if (!datasetReady(ctx, users)) {
      try {
        buildDataset(ctx, users);
      } catch (final RuntimeException | Error e) {
        buildFailure = e.toString();
        throw e;
      }
    }
    if (vectors == null)
      vectors = loadVectors(ctx.server());
    current = c;
    if (c.shape().writes()) {
      wrote = true;
      resetWrites(ctx.server());
    }
    buildParams(ctx, c, users);
    if (c.shape().writes())
      createWriteRows(ctx.server(), c);
    checkParity(ctx, c, users);
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    try {
      if (wrote && ctx.server() != null)
        resetWrites(ctx.server());
    } finally {
      wrote = false;
      current = null;
      opParams = null;
      mergeParams = null;
      txParams = null;
      txStatements = null;
    }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final Case c = current;
    if (c == null || !c.name().equals(caseName))
      throw new IllegalStateException("Case " + caseName + " was not prepared");
    final int ops = operations(ctx.scale(), c.shape());
    final List<ProtocolClient> clients = openClients(c.clients(), () -> ProtocolClient.open(c.protocol(), ctx.server(), DATABASE));
    final LatencyRecorder latencies;
    final long elapsed;
    try {
      ctx.server().resetPeak();
      final long start = System.nanoTime();
      latencies = runClients(c.clients(), ops, op -> run(clients.get(op % c.clients()), c, op));
      elapsed = System.nanoTime() - start;
    } finally {
      closeAll(clients);
    }
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("ops_per_s", "ops/s", ops / (elapsed / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    metrics.add(Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes()));
    return Measurement.of(metrics, latencies);
  }

  /** One operation: the parameters were built by prepare, so only the round trip is in the timed window. */
  private void run(final ProtocolClient client, final Case c, final int op) {
    final int clientIndex = op % c.clients();
    final int variant = (op / c.clients()) & 1;
    switch (c.shape()) {
      case POINT, HOP1, PAGE_1000 -> client.query(c.shape().cypher(), opParams[op]);
      case VECTOR_PARAM -> client.query(c.shape().cypher(), opParams[0]);
      case UNWIND_MERGE_500 -> client.transaction(List.of(c.shape().cypher()), List.of(mergeParams[clientIndex][variant]));
      case TX_10 -> client.transaction(txStatements, txParams[clientIndex][variant]);
    }
  }

  /**
   * Runs {@code ops} operations over {@code clients} threads, operation {@code i} on thread {@code i % clients} (so the operation
   * can use a resource of its client without sharing it), and returns every latency. One client runs on the caller's thread. A
   * failing operation stops every thread at its next operation; all threads are joined before the first failure is rethrown.
   */
  static LatencyRecorder runClients(final int clients, final int ops, final IntConsumer op) throws InterruptedException {
    final LatencyRecorder all = new LatencyRecorder(ops);
    if (clients == 1) {
      for (int i = 0; i < ops; i++) {
        final long t0 = System.nanoTime();
        op.accept(i);
        all.record(System.nanoTime() - t0);
      }
      return all;
    }
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean stop = new AtomicBoolean();
    final LatencyRecorder[] recorders = new LatencyRecorder[clients];
    final Thread[] threads = new Thread[clients];
    for (int c = 0; c < clients; c++) {
      final int client = c;
      recorders[c] = new LatencyRecorder(ops / clients + 1);
      threads[c] = new Thread(() -> {
        try {
          for (int i = client; i < ops && !stop.get(); i += clients) {
            final long t0 = System.nanoTime();
            op.accept(i);
            recorders[client].record(System.nanoTime() - t0);
          }
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
          stop.set(true);
        }
      }, CLIENT_THREAD + c);
      threads[c].setDaemon(true);
    }
    for (final Thread t : threads)
      t.start();
    try {
      for (final Thread t : threads)
        t.join();
    } catch (final InterruptedException e) {
      // the case was abandoned: the client threads stop at their next operation
      stop.set(true);
      for (final Thread t : threads)
        t.interrupt();
      // bounded: a client stuck in a socket call must not hold the caller, but one that can stop should be gone before it returns
      for (final Thread t : threads)
        try {
          t.join(JOIN_AFTER_INTERRUPT_MS);
        } catch (final InterruptedException again) { // noopengrep: interrupted-exception-swallowed - added to e, which is rethrown below
          e.addSuppressed(again);
          break;
        }
      throw e;
    }
    final Throwable t = failure.get();
    if (t instanceof RuntimeException e)
      throw e;
    if (t instanceof Error e)
      throw e;
    if (t != null)
      throw new IllegalStateException(t);
    for (final LatencyRecorder r : recorders)
      all.addAll(r);
    return all;
  }

  /**
   * Opens {@code n} clients and makes each serve one untimed query: http, grpc and bolt connect on first use (JDBC at open), so
   * without it the first operation of every client would time the connection setup.
   */
  static List<ProtocolClient> openClients(final int n, final Supplier<ProtocolClient> factory) {
    final List<ProtocolClient> clients = new ArrayList<>(n);
    try {
      for (int i = 0; i < n; i++) {
        final ProtocolClient client = factory.get();
        clients.add(client);
        client.query(WARM_UP, Map.of());
      }
      return clients;
    } catch (final RuntimeException | Error e) {
      try {
        closeAll(clients);
      } catch (final RuntimeException close) {
        e.addSuppressed(close);
      }
      throw e;
    }
  }

  /** Closes every client; the first failure is thrown after all were tried, the others suppressed in it. */
  private static void closeAll(final List<ProtocolClient> clients) {
    RuntimeException failure = null;
    for (final ProtocolClient client : clients)
      try {
        client.close();
      } catch (final RuntimeException e) {
        if (failure == null)
          failure = e;
        else
          failure.addSuppressed(e);
      }
    if (failure != null)
      throw failure;
  }

  /** Untimed: both name variants of every client's rows, over HTTP, so the timed operations only ever update. */
  private void createWriteRows(final ServerUnderTest server, final Case c) {
    try (final ProtocolClient http = ProtocolClient.open(Protocol.HTTP, server, DATABASE)) {
      for (int client = 0; client < c.clients(); client++)
        for (int variant = 0; variant < 2; variant++)
          if (c.shape() == Shape.TX_10)
            http.transaction(txStatements, txParams[client][variant]);
          else
            http.transaction(List.of(c.shape().cypher()), List.of(mergeParams[client][variant]));
    }
  }

  // ---- parameters

  /** The seed of a case: of its shape and client count, so every protocol of a shape runs the same operations. */
  private static long caseSeed(final WorkloadContext ctx, final Case c) {
    return ctx.seed() + 1_009L * (c.shape().ordinal() + 1) + c.clients();
  }

  @SuppressWarnings("unchecked")
  private void buildParams(final WorkloadContext ctx, final Case c, final int users) {
    final int ops = operations(ctx.scale(), c.shape());
    final SplittableRandom random = new SplittableRandom(caseSeed(ctx, c));
    opParams = null;
    mergeParams = null;
    txParams = null;
    txStatements = null;
    switch (c.shape()) {
      case POINT, HOP1 -> {
        opParams = new Map[ops];
        for (int i = 0; i < ops; i++)
          opParams[i] = Map.of("id", (long) (1 + random.nextInt(users)));
      }
      case PAGE_1000 -> {
        opParams = new Map[ops];
        for (int i = 0; i < ops; i++)
          opParams[i] = Map.of("from", (long) (1 + random.nextInt(users - PAGE + 1)));
      }
      case VECTOR_PARAM -> opParams = new Map[] { Map.of("vectors", vectors) };
      case UNWIND_MERGE_500 -> {
        mergeParams = new Map[c.clients()][2];
        for (int client = 0; client < c.clients(); client++)
          for (int variant = 0; variant < 2; variant++)
            mergeParams[client][variant] = Map.of("rows",
                mergeRows(WRITE_BASE + (long) client * MERGE_ROWS, "m" + variant + "-"));
      }
      case TX_10 -> {
        txStatements = Collections.nCopies(TX_STATEMENTS, c.shape().cypher());
        txParams = new List[c.clients()][2];
        for (int client = 0; client < c.clients(); client++)
          for (int variant = 0; variant < 2; variant++)
            txParams[client][variant] = txRows(TX_BASE + (long) client * TX_STATEMENTS, "t" + variant + "-");
      }
    }
  }

  private static List<Map<String, Object>> mergeRows(final long firstId, final String namePrefix) {
    final List<Map<String, Object>> rows = new ArrayList<>(MERGE_ROWS);
    for (int j = 0; j < MERGE_ROWS; j++)
      rows.add(Map.of("id", firstId + j, "name", namePrefix + (firstId + j)));
    return rows;
  }

  private static List<Map<String, Object>> txRows(final long firstId, final String namePrefix) {
    final List<Map<String, Object>> rows = new ArrayList<>(TX_STATEMENTS);
    for (int j = 0; j < TX_STATEMENTS; j++)
      rows.add(Map.of("id", firstId + j, "name", namePrefix + (firstId + j)));
    return rows;
  }

  // ---- parity

  private void checkParity(final WorkloadContext ctx, final Case c, final int users) {
    if (c.shape().writes()) {
      checkWrites(ctx.server(), c);
      return;
    }
    final SplittableRandom random = new SplittableRandom(ctx.seed() + 31);
    final List<Map<String, Object>> params = new ArrayList<>(PARITY_PARAMS);
    for (int i = 0; i < PARITY_PARAMS; i++)
      params.add(switch (c.shape()) {
        case POINT, HOP1 -> Map.of("id", (long) (1 + random.nextInt(users)));
        case PAGE_1000 -> Map.of("from", (long) (1 + random.nextInt(users - PAGE + 1)));
        default -> Map.of("vectors", vectors);
      });
    final boolean ordered = c.shape() != Shape.VECTOR_PARAM;
    long referenceRows = 0;
    try (final ProtocolClient http = ProtocolClient.open(Protocol.HTTP, ctx.server(), DATABASE);
        final ProtocolClient other = c.protocol() == Protocol.HTTP ? null : ProtocolClient.open(c.protocol(), ctx.server(), DATABASE)) {
      for (final Map<String, Object> p : params) {
        List<Map<String, Object>> expected = http.query(c.shape().cypher(), p);
        if (c.shape() == corruptHttpReferenceOf && !expected.isEmpty())
          expected = corrupted(expected);
        checkReference(c, p, expected);
        referenceRows += expected.size();
        if (other == null)
          continue;
        final String diff = Parity.diff(expected, other.query(c.shape().cypher(), p), ordered);
        if (diff != null)
          throw new IllegalStateException("parity with http failed for " + c.name() + " with " + describe(p) + ": " + diff);
      }
    }
    // a check over answers that are all empty would pass whatever the protocol did
    if (referenceRows == 0)
      throw new IllegalStateException("parity of " + c.name() + ": http answered no row to any of the " + PARITY_PARAMS
          + " parameters");
  }

  /** What the HTTP reference must hold for the shape whatever the protocol, so a broken dataset fails here. */
  private static void checkReference(final Case c, final Map<String, Object> p, final List<Map<String, Object>> rows) {
    final String problem = switch (c.shape()) {
      case POINT -> rows.size() == 1 ? null : rows.size() + " rows, expected 1";
      case PAGE_1000 -> rows.size() == PAGE ? null : rows.size() + " rows, expected " + PAGE;
      case VECTOR_PARAM -> {
        if (rows.size() != VECTORS)
          yield rows.size() + " rows, expected " + VECTORS;
        for (final Map<String, Object> row : rows)
          if (!(row.get("n") instanceof Number n) || n.intValue() != DIMENSIONS)
            yield "size " + row.get("n") + ", expected " + DIMENSIONS;
        yield null;
      }
      default -> null;
    };
    if (problem != null)
      throw new IllegalStateException("http reference of " + c.name() + " with " + describe(p) + ": " + problem);
  }

  private static List<Map<String, Object>> corrupted(final List<Map<String, Object>> rows) {
    final List<Map<String, Object>> copy = new ArrayList<>(rows);
    final Map<String, Object> first = new LinkedHashMap<>(copy.getFirst());
    final String column = first.keySet().iterator().next();
    first.put(column, -1L);
    copy.set(0, first);
    return copy;
  }

  private static String describe(final Map<String, Object> params) {
    return params.containsKey("vectors") ? "the " + VECTORS + " vectors" : params.toString();
  }

  /**
   * Writes the shape's rows on the case's protocol twice, the first time creating them and the second updating them, and reads
   * them back over HTTP after each write. The ids are this protocol's own, so no case reads the rows of another.
   */
  private static void checkWrites(final ServerUnderTest server, final Case c) {
    final long first = PARITY_WRITE_BASE + c.protocol().ordinal() * 1_000L + (c.shape() == Shape.TX_10 ? 600 : 0);
    final int count = c.shape() == Shape.TX_10 ? TX_STATEMENTS : MERGE_ROWS;
    final long nonce = System.nanoTime();
    try (final ProtocolClient client = ProtocolClient.open(c.protocol(), server, DATABASE);
        final ProtocolClient http = ProtocolClient.open(Protocol.HTTP, server, DATABASE)) {
      for (int pass = 0; pass < 2; pass++) {
        final String prefix = "parity" + pass + "-" + nonce + "-";
        if (c.shape() == Shape.TX_10)
          client.transaction(Collections.nCopies(TX_STATEMENTS, c.shape().cypher()), txRows(first, prefix));
        else
          client.transaction(List.of(c.shape().cypher()), List.of(Map.of("rows", mergeRows(first, prefix))));
        final List<Map<String, Object>> expected = new ArrayList<>(count);
        for (int j = 0; j < count; j++)
          expected.add(Map.of("id", first + j, "name", prefix + (first + j)));
        final List<Map<String, Object>> got = http.query(
            "MATCH (u:User) WHERE u.id >= $lo AND u.id < $hi RETURN u.id AS id, u.name AS name ORDER BY id",
            Map.of("lo", first, "hi", first + count));
        final String diff = Parity.diff(expected, got, true);
        if (diff != null)
          throw new IllegalStateException("parity of " + c.name() + ": the rows written " + (pass == 0 ? "(created)" : "(updated)")
              + " read back over http differ: " + diff);
      }
    }
  }

  // ---- dataset

  private static RemoteServer remoteServer(final ServerUnderTest server) {
    final RemoteServer remote = new RemoteServer(server.wrapper().host(), server.wrapper().httpPort(), "root",
        ServerUnderTest.PASSWORD);
    remote.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    return remote;
  }

  private static boolean datasetReady(final WorkloadContext ctx, final int users) {
    try {
      if (!remoteServer(ctx.server()).exists(DATABASE))
        return false;
      try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
        return RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM User WHERE id < " + WRITE_BASE) == users
            && RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM FriendOf") == (long) users * FRIENDS_PER_USER
            && RemoteClients.count(db, SQL, "SELECT count(*) AS n FROM Vec") == VECTORS;
      }
    } catch (final RuntimeException e) {
      return false;
    }
  }

  /**
   * Untimed: users 1..users named {@code user<id>}, {@value #FRIENDS_PER_USER} random friendships per user, and
   * {@value #VECTORS} vectors of {@value #DIMENSIONS} floats. A database left by an earlier, different build is dropped first.
   */
  private void buildDataset(final WorkloadContext ctx, final int users) {
    LOGGER.info("Building the protocols dataset: {} users", users);
    final long start = System.nanoTime();
    final RemoteServer remote = remoteServer(ctx.server());
    if (remote.exists(DATABASE))
      remote.drop(DATABASE);
    remote.create(DATABASE);
    vectors = null;
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      db.command(SQL, "CREATE VERTEX TYPE User").close();
      db.command(SQL, "CREATE PROPERTY User.id LONG").close();
      db.command(SQL, "CREATE PROPERTY User.name STRING").close();
      db.command(SQL, "CREATE INDEX ON User (id) UNIQUE").close();
      db.command(SQL, "CREATE EDGE TYPE FriendOf").close();
      db.command(SQL, "CREATE VERTEX TYPE Vec").close();
      db.command(SQL, "CREATE PROPERTY Vec.id LONG").close();
      db.command(SQL, "CREATE PROPERTY Vec.embedding ARRAY_OF_FLOATS").close();

      final StringBuilder script = new StringBuilder(BATCH * 64);
      for (int from = 0; from < users; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = from; i < Math.min(from + BATCH, users); i++)
          script.append("CREATE VERTEX User SET id = ").append(i + 1).append(", name = 'user").append(i + 1).append("';\n");
        GraphOltpWorkload.flush(db, script);
      }
      final String[] rids = GraphOltpWorkload.loadRids(db, "User", users);
      final SplittableRandom random = new SplittableRandom(ctx.seed() + 3);
      final long friendships = (long) users * FRIENDS_PER_USER;
      for (long from = 0; from < friendships; from += BATCH) {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (long i = from; i < Math.min(from + BATCH, friendships); i++) {
          final int a = random.nextInt(users);
          int b = a;
          while (b == a)
            b = random.nextInt(users);
          script.append("CREATE EDGE FriendOf FROM ").append(rids[a]).append(" TO ").append(rids[b]).append(";\n");
        }
        GraphOltpWorkload.flush(db, script);
      }
      for (int v = 0; v < VECTORS; v++) {
        final List<Float> embedding = new ArrayList<>(DIMENSIONS);
        for (int d = 0; d < DIMENSIONS; d++)
          embedding.add((float) (random.nextDouble() * 2 - 1));
        db.command(SQL, "INSERT INTO Vec SET id = :id, embedding = :e", Map.of("id", (long) v + 1, "e", embedding)).close();
      }
    }
    LOGGER.info("Protocols dataset built in {} s", (System.nanoTime() - start) / 1e9);
  }

  /** The vectors as lists of doubles holding float values, so every protocol sends the same numbers. */
  private static List<List<Double>> loadVectors(final ServerUnderTest server) {
    try (final ProtocolClient http = ProtocolClient.open(Protocol.HTTP, server, DATABASE)) {
      final List<Map<String, Object>> rows = http.query("MATCH (v:Vec) RETURN v.embedding AS e ORDER BY v.id", Map.of());
      if (rows.size() != VECTORS)
        throw new IllegalStateException("expected " + VECTORS + " vectors, found " + rows.size());
      final List<List<Double>> result = new ArrayList<>(VECTORS);
      for (final Map<String, Object> row : rows) {
        final Object e = Parity.normalise(row.get("e"));
        if (!(e instanceof List<?> list) || list.size() != DIMENSIONS)
          throw new IllegalStateException("a vector of " + DIMENSIONS + " floats expected, found " + e);
        final List<Double> vector = new ArrayList<>(DIMENSIONS);
        for (final Object x : list)
          vector.add((double) ((Number) x).floatValue());
        result.add(Collections.unmodifiableList(vector));
      }
      return Collections.unmodifiableList(result);
    }
  }

  private static void resetWrites(final ServerUnderTest server) {
    try (final RemoteDatabase db = RemoteClients.open(server, DATABASE)) {
      db.command(SQL, "DELETE FROM User WHERE id >= " + WRITE_BASE).close();
    }
  }
}
