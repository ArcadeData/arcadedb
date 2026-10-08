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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurity;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A real, never-started {@link ArcadeDBServer} (see {@link TestServerHelper#unstartedServer(Path, ContextConfiguration)})
 * whose lifecycle state - status, security, HTTP server, plugins and registered databases - is set by the test (issue
 * #9464). It replaces a Mockito mock of {@link ArcadeDBServer} that stubbed those getters to describe a server that is
 * running, which a server that was never started cannot be.
 * <p>
 * Every overridden getter starts where the unstarted server does: {@code OFFLINE}, no security, no HTTP server, no
 * plugins, no databases. Everything else - configuration, name, paths, HA, query profiler - is the real server's.
 * <p>
 * A fake built without a root gets its own path under {@code target/} (see
 * {@link TestServerHelper#defaultUnstartedServerRoot()}). Code that persists through the server - a state machine bound
 * to it writes {@code <databases>/.raft} - lands there; a test that reads that state back roots the fake in its own
 * {@code @TempDir} with {@link #create(Path, ContextConfiguration)}.
 */
public class FakeArcadeDBServer extends ArcadeDBServer {
  private final    Map<String, ServerDatabase> databases   = new ConcurrentHashMap<>();
  private final    Set<String>                 listedNames = ConcurrentHashMap.newKeySet();
  private volatile STATUS                      status      = STATUS.OFFLINE;
  private volatile List<ServerPlugin>          plugins     = List.of();
  private volatile ServerSecurity              security;
  private volatile HttpServer                  httpServer;
  private volatile ServerQueryProfiler         queryProfiler;
  private volatile SecurityConvergenceGate     securityConvergenceGate;

  // Calls whose effect, or whose moment, a test observes: recorded, and answered by a function the test may set
  private static final Set<String> RECORDED = Set.of("getBackupCoordinator", "getDatabase", "existsDatabase",
      "getDatabaseNames", "removeDatabase", "stop", "getHA", "getConfiguration", "getServerName");
  private volatile CallLog         log     = new CallLog();
  private final    CallLog.Answers answers = new CallLog.Answers(RECORDED);

  private FakeArcadeDBServer(final ContextConfiguration configuration) {
    super(configuration);
  }

  /** A fake with the default name, a new configuration and no disk. */
  public static FakeArcadeDBServer create() {
    return create((String) null, new ContextConfiguration());
  }

  /** A fake with the default name, on {@code configuration}, with no disk. */
  public static FakeArcadeDBServer create(final ContextConfiguration configuration) {
    return create((String) null, configuration);
  }

  /** A fake named {@code serverName} (unless null), on {@code configuration}, with no disk. */
  public static FakeArcadeDBServer create(final String serverName, final ContextConfiguration configuration) {
    if (serverName != null)
      configuration.setValue(GlobalConfiguration.SERVER_NAME, serverName);
    return create(TestServerHelper.defaultUnstartedServerRoot(), configuration);
  }

  /** A fake rooted at {@code rootPath}, on {@code configuration}. */
  public static FakeArcadeDBServer create(final Path rootPath, final ContextConfiguration configuration) {
    return new FakeArcadeDBServer(TestServerHelper.rooted(rootPath, configuration));
  }

  /** Records on {@code log} from now on, which other fakes may share. Call it before the fake is used. */
  public FakeArcadeDBServer recordingOn(final CallLog log) {
    this.log = log;
    return this;
  }

  public CallLog log() {
    return log;
  }

  /** The argument lists of every call to the recorded {@code method}, in arrival order. */
  public List<List<Object>> calls(final String method) {
    return log.argsOf(this, method);
  }

  /** The recorded {@code method} answers {@code value} from now on. */
  public FakeArcadeDBServer returns(final String method, final Object value) {
    return on(method, args -> value);
  }

  /** The recorded {@code method} throws {@code failure} from now on. */
  public FakeArcadeDBServer fails(final String method, final RuntimeException failure) {
    return on(method, args -> {
      throw failure;
    });
  }

  /**
   * The recorded {@code method} runs {@code answer} on its arguments from now on. For a {@code void} method, such as
   * {@code stop} or {@code removeDatabase}, the value the function returns is ignored.
   */
  public FakeArcadeDBServer on(final String method, final Function<Object[], Object> answer) {
    answers.set(method, answer);
    return this;
  }

  private Object call(final String method, final Supplier<Object> fallback, final Object... args) {
    // The real constructor runs before this class's fields exist and may read these getters: it gets the real answer
    if (log == null || answers == null)
      return fallback.get();
    log.record(this, method, args);
    final Function<Object[], Object> answer = answers.get(method);
    return answer != null ? answer.apply(args) : fallback.get();
  }

  /** A running server ({@code ONLINE}). */
  public FakeArcadeDBServer online() {
    return status(STATUS.ONLINE);
  }

  public FakeArcadeDBServer status(final STATUS status) {
    this.status = status;
    return this;
  }

  public FakeArcadeDBServer security(final ServerSecurity security) {
    this.security = security;
    return this;
  }

  public FakeArcadeDBServer httpServer(final HttpServer httpServer) {
    this.httpServer = httpServer;
    return this;
  }

  public FakeArcadeDBServer plugins(final ServerPlugin... plugins) {
    this.plugins = List.of(plugins);
    return this;
  }

  /** The query profiler to answer instead of the server's own (which is not recording). */
  public FakeArcadeDBServer queryProfiler(final ServerQueryProfiler queryProfiler) {
    this.queryProfiler = queryProfiler;
    return this;
  }

  /** The security convergence gate to answer instead of the server's own, for a test that inspects it. */
  public FakeArcadeDBServer securityConvergenceGate(final SecurityConvergenceGate gate) {
    this.securityConvergenceGate = gate;
    return this;
  }

  /** Serves {@code database} under {@code name}. The test keeps owning it: the fake never closes it. */
  public FakeArcadeDBServer database(final String name, final ServerDatabase database) {
    databases.put(name, database);
    return this;
  }

  /**
   * Lists databases by name only: {@link #getDatabaseNames()} answers them, while {@link #existsDatabase(String)} and
   * {@link #getDatabase(String)} know only the databases served with {@link #database(String, ServerDatabase)}. That is
   * a registry entry whose database is not available, for code that reads the names and never opens one.
   */
  public FakeArcadeDBServer databaseNames(final String... names) {
    Collections.addAll(listedNames, names);
    return this;
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public ServerSecurity getSecurity() {
    return security;
  }

  @Override
  public HttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public Collection<ServerPlugin> getPlugins() {
    return plugins;
  }

  @Override
  public ServerQueryProfiler getQueryProfiler() {
    final ServerQueryProfiler set = queryProfiler;
    return set != null ? set : super.getQueryProfiler();
  }

  @Override
  SecurityConvergenceGate getSecurityConvergenceGate() {
    final SecurityConvergenceGate set = securityConvergenceGate;
    return set != null ? set : super.getSecurityConvergenceGate();
  }

  @Override
  public boolean existsDatabase(final String databaseName) {
    final Object answer = call("existsDatabase", () -> databases.containsKey(databaseName), databaseName);
    if (!(answer instanceof Boolean exists))
      throw new IllegalStateException("The answer set for 'existsDatabase' must be a Boolean, it gave " + answer);
    return exists;
  }

  @Override
  public ServerDatabase getDatabase(final String databaseName) {
    return (ServerDatabase) call("getDatabase", () -> databases.get(databaseName), databaseName);
  }

  /** Recorded, and forgets a database served here; the real registry of an unstarted server holds nothing. */
  @Override
  public void removeDatabase(final String databaseName) {
    call("removeDatabase", () -> {
      databases.remove(databaseName);
      listedNames.remove(databaseName);
      return null;
    }, databaseName);
  }

  /** Recorded: there is nothing to stop on a server that never started. */
  @Override
  public void stop() {
    call("stop", () -> null);
  }

  @Override
  public BackupCoordinator getBackupCoordinator() {
    return (BackupCoordinator) call("getBackupCoordinator", super::getBackupCoordinator);
  }

  @Override
  public HAServerPlugin getHA() {
    return (HAServerPlugin) call("getHA", super::getHA);
  }

  /** The name fixed at construction, unless a test answers another (a mock-era test that named the server late). */
  @Override
  public String getServerName() {
    return (String) call("getServerName", super::getServerName);
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return (ContextConfiguration) call("getConfiguration", super::getConfiguration);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> getDatabaseNames() {
    return (Set<String>) call("getDatabaseNames", () -> {
      final Set<String> names = new TreeSet<>(databases.keySet());
      names.addAll(listedNames);
      return Collections.unmodifiableSet(names);
    });
  }
}
