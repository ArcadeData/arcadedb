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

  private FakeArcadeDBServer(final ContextConfiguration configuration) {
    super(configuration);
  }

  /** A fake with the default name, a new configuration and no disk. */
  public static FakeArcadeDBServer create() {
    return create((String) null, new ContextConfiguration());
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
    return databases.containsKey(databaseName);
  }

  @Override
  public ServerDatabase getDatabase(final String databaseName) {
    return databases.get(databaseName);
  }

  @Override
  public Set<String> getDatabaseNames() {
    final Set<String> names = new TreeSet<>(databases.keySet());
    names.addAll(listedNames);
    return Collections.unmodifiableSet(names);
  }
}
