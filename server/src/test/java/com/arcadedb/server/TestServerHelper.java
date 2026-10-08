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

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.CallableParameterNoReturn;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.UUID;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Static utility methods for test server lifecycle and database management.
 */
public final class TestServerHelper {

  private TestServerHelper() {
    // Utility class - no instances
  }

  public static ArcadeDBServer[] startServers(final int totalServers,
      final CallableParameterNoReturn<ContextConfiguration> onServerConfigurationCallback,
      final CallableParameterNoReturn<ArcadeDBServer> onBeforeStartingCallback) {
    final ArcadeDBServer[] servers = new ArcadeDBServer[totalServers];

    int port = 2424;
    String serverURLs = "";
    for (int i = 0; i < totalServers; ++i) {
      if (i > 0)
        serverURLs += ",";

      serverURLs += "localhost:" + (port++);
    }

    for (int i = 0; i < totalServers; ++i) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, Constants.PRODUCT + "_" + i);
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases" + i);
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverURLs);
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      config.setValue(GlobalConfiguration.HA_ENABLED, totalServers > 1);
      //config.setValue(GlobalConfiguration.NETWORK_SOCKET_TIMEOUT, 2000);

      if (onServerConfigurationCallback != null)
        onServerConfigurationCallback.call(config);

      servers[i] = new ArcadeDBServer(config);

      if (onBeforeStartingCallback != null)
        onBeforeStartingCallback.call(servers[i]);

      servers[i].start();
    }

    return servers;
  }

  /**
   * A real server that is constructed and never started: {@link ArcadeDBServer#getConfiguration()} is the very instance
   * passed in, the root and config paths resolve under {@code rootPath}, and nothing binds a port, opens a database or
   * writes a file. It replaces a Mockito mock of {@link ArcadeDBServer} that only stubbed those getters (issue #9464).
   * <p>
   * It sets {@code SERVER_ROOT_PATH} and {@code SERVER_DATABASE_DIRECTORY} on {@code configuration} itself, so that
   * {@code getConfiguration()} stays the caller's instance; a configuration already rooted elsewhere is refused. Like any
   * {@link ArcadeDBServer}, the constructor registers a JVM shutdown hook, which is a no-op for a server never started.
   */
  public static ArcadeDBServer unstartedServer(final Path rootPath, final ContextConfiguration configuration) {
    return new ArcadeDBServer(rooted(rootPath, configuration));
  }

  /**
   * Roots {@code configuration} at {@code rootPath}, refusing one already rooted elsewhere, and puts the database directory
   * under it unless the configuration names its own; returns it.
   */
  static ContextConfiguration rooted(final Path rootPath, final ContextConfiguration configuration) {
    final String root = rootPath.toString();
    if (configuration.getContextKeys().contains(GlobalConfiguration.SERVER_ROOT_PATH.getKey())
        && !root.equals(configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH)))
      throw new IllegalArgumentException("The configuration already belongs to a server rooted at '"
          + configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH) + "': use a new ContextConfiguration per root");
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, root);
    // A database directory the test chose stays: only the default one is moved under the root
    if (!configuration.getContextKeys().contains(GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getKey()))
      configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, rootPath.resolve("databases").toString());
    return configuration;
  }

  /**
   * Where a server built without an explicit root is rooted: a fresh path under the module's {@code target} directory,
   * never shared with another server and not created by construction. Code under test that persists something (a state
   * machine bound to the server writes {@code <databases>/.raft}) lands there and nowhere else, and {@code mvn clean}
   * removes it; a test that reads it back passes its own {@code @TempDir} to
   * {@link #unstartedServer(Path, ContextConfiguration)}.
   */
  static Path defaultUnstartedServerRoot() {
    return Path.of("target", "unstarted-servers", UUID.randomUUID().toString()).toAbsolutePath();
  }

  /** {@link #unstartedServer(Path, ContextConfiguration)} with the default name, a new configuration and no disk. */
  public static ArcadeDBServer unstartedServer() {
    return unstartedServer(defaultUnstartedServerRoot(), new ContextConfiguration());
  }

  /** {@link #unstartedServer(Path, ContextConfiguration)} named {@code serverName}, with a new configuration and no disk. */
  public static ArcadeDBServer unstartedServer(final String serverName) {
    return unstartedServer(serverName, new ContextConfiguration());
  }

  /**
   * {@link #unstartedServer(Path, ContextConfiguration)} with no disk, named {@code serverName} (written into
   * {@code configuration}, where {@link ArcadeDBServer#getServerName()} reads it) unless that is null. Do not share one
   * configuration between servers that need different names: the last name written wins for all of them.
   */
  public static ArcadeDBServer unstartedServer(final String serverName, final ContextConfiguration configuration) {
    if (serverName != null)
      configuration.setValue(GlobalConfiguration.SERVER_NAME, serverName);
    return unstartedServer(defaultUnstartedServerRoot(), configuration);
  }

  /**
   * A real server user named {@code name}, with access to {@code databases} (none when empty), for code that reads only
   * the user's name and authorized databases. It is bound to no server, so {@code getDatabaseUser()} is out of reach:
   * a test that needs it builds the user against a real server instead (issue #9464). Every database is granted with the
   * {@code admin} group: do not use it to test group permissions.
   */
  public static ServerSecurityUser securityUser(final String name, final String... databases) {
    final JSONObject configuration = new JSONObject().put("name", name);
    if (databases.length > 0) {
      final JSONObject access = new JSONObject();
      for (final String database : databases)
        access.put(database, new JSONArray().put("admin"));
      configuration.put("databases", access);
    }
    return new ServerSecurityUser(null, configuration);
  }

  public static void stopServers(final ArcadeDBServer[] servers) {
    if (servers != null) {
      for (final ArcadeDBServer server : servers)
        if (server != null)
          server.stop();
    }
  }

  public static ArcadeDBServer getServerByName(final ArcadeDBServer[] servers, final String serverName) {
    for (final ArcadeDBServer s : servers) {
      if (s.getServerName().equals(serverName))
        return s;
    }
    return null;
  }

  public static ArcadeDBServer getLeaderServer(final ArcadeDBServer[] servers) {
    for (final ArcadeDBServer server : servers)
      if (server.isStarted()) {
        final String leaderName = server.getHA().getLeaderName();
        return getServerByName(servers, leaderName);
      }
    return null;
  }

  public static void expectException(final CallableNoReturn callback, final Class<? extends Throwable> expectedException)
      throws Exception {
    try {
      callback.call();
      fail("Expected exception of type " + expectedException.getName() + " but none was thrown");
    } catch (final Throwable e) {
      if (e.getClass().equals(expectedException))
        // EXPECTED
        return;

      if (e instanceof Exception exception)
        throw exception;

      throw new Exception(e);
    }
  }

  public static void checkActiveDatabases() {
    checkActiveDatabases(true);
  }

  public static void checkActiveDatabases(final boolean drop) {
    final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();

    if (!activeDatabases.isEmpty())
      LogManager.instance()
          .log(TestServerHelper.class, Level.SEVERE, "Found active databases: " + activeDatabases + ". Forced closing...");

    for (final Database db : activeDatabases)
      if (drop) {
        if (db.isTransactionActive())
          db.commit();

        ((DatabaseInternal) db).getEmbedded().drop();
      } else
        db.close();

    assertThat(activeDatabases.isEmpty()).as("Found active databases: " + activeDatabases).isTrue();
  }

  /**
   * Deletes the folders a server test writes into: the shared {@code ./target/databases}, the per-server
   * {@code <databaseDirectory>N} for {@code N < totalServers}, and the replication directory under the server root.
   * <p>
   * Every path is resolved <b>at call time</b> from the live configuration, so the caller must not have reset it yet.
   * That is not a style point (issue #6297): {@link GlobalConfiguration#SERVER_DATABASE_DIRECTORY} defaults to
   * {@code ${arcadedb.server.rootPath}/databases} while {@code SERVER_ROOT_PATH} defaults to {@code null}, and
   * {@code getValueAsString()} resolves an unknown variable to the empty string instead of failing - so after a
   * {@code resetAll()} the whole prefix collapses and this method is asked to recursively delete {@code /databases},
   * {@code /databases0} ... {@code /databasesN}. Those are absolute root-level paths no test ever named, the test's
   * own {@code ./target/databasesN} survives teardown untouched, and a data volume mounted at {@code /databases0}
   * would be inside the blast radius. {@link #deleteResolvedFolder} refuses exactly that shape, so a future
   * reordering degrades to a logged no-op rather than to a recursive delete at the filesystem root.
   */
  public static void deleteDatabaseFolders(final int totalServers) {
    FileUtils.deleteRecursively(new File("./target/databases/"));
    FileUtils.deleteRecursively(new File("./target/config/"));

    final String databaseDirectory = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString();
    deleteResolvedFolder(databaseDirectory, File.separator);
    for (int i = 0; i < totalServers; ++i)
      deleteResolvedFolder(databaseDirectory, i + File.separator);

    final String rootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
    // Unset by default, and the concatenation below would turn that into the literal folder "null/replication".
    if (rootPath != null)
      deleteResolvedFolder(rootPath, File.separator + "replication");
  }

  /**
   * Recursively deletes {@code prefix + suffix}, unless that did not resolve to anything a test could have written
   * to - in which case it logs and does nothing. See {@link #deleteDatabaseFolders(int)} for why a prefix arrives
   * unresolved at all.
   * <p>
   * The whole concatenation is what gets checked, not the prefix alone: the two arguments are what a caller
   * composes, so validating one of them would leave the verdict depending on an assumption about the other
   * (today: that appending a digit and a separator can neither make a relative path absolute nor change its
   * segment count). Checking what is actually about to be deleted has no such precondition to preserve.
   */
  private static void deleteResolvedFolder(final String prefix, final String suffix) {
    final String folder = prefix + suffix;
    if (!isResolvedTestPath(folder)) {
      LogManager.instance().log(TestServerHelper.class, Level.SEVERE,
          "Refusing to delete '%s': the configured path did not resolve, so this is not a test folder. The test "
              + "configuration was reset before the cleanup that reads it (issue #6297); nothing was deleted, and the "
              + "folders this call was meant to remove are still on disk.", folder);
      return;
    }
    FileUtils.deleteRecursively(new File(folder));
  }

  /**
   * Whether {@code path} can be a folder a test wrote into. Relative paths always can - every server test configures
   * {@code ./target/databasesN} - and so can an absolute path with more than one element, which is what a
   * {@code @TempDir} under the system temp directory looks like. What cannot is a blank path or an absolute one
   * naming a single root-level folder: no test writes to {@code /databases0}, and that is precisely what an
   * unresolved {@code ${arcadedb.server.rootPath}} placeholder collapses to. Package-private for testing.
   */
  static boolean isResolvedTestPath(final String path) {
    if (path == null || path.isBlank())
      return false;
    try {
      final Path normalized = Path.of(path).normalize();
      return !normalized.isAbsolute() || normalized.getNameCount() > 1;
    } catch (final InvalidPathException e) {
      // Not a path this platform can even name, so certainly not a folder a test created.
      return false;
    }
  }
}
