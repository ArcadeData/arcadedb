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
package com.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.AnsiLogFormatter;
import com.arcadedb.utility.FileUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ConfigurationSettingsTest {
  /**
   * Regression for #10599: {@code SERVER_HTTP_IO_THREADS} was declared with the key {@code arcadedb.server.httpsIoThreads}, so the natural
   * spelling {@code -Darcadedb.server.httpIoThreads} was silently ignored and an operator reading a settings dump took the setting
   * for an HTTPS-only one. A setting's key has to name the setting, because the key is the only handle a user has: the key is now
   * {@code arcadedb.server.httpIoThreads}, the old spelling is still read (and reported once) so a deployment that found it does not
   * lose its value, and a census over every setting keeps a key that names something else from arriving unnoticed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConfigurationKeyNames {
    private static final String KEY        = "arcadedb.server.httpIoThreads";
    private static final String LEGACY_KEY = "arcadedb.server.httpsIoThreads";

    /**
     * The settings whose key is a deliberate abbreviation of the constant's name (the constant is the long form, the key is the one
     * an operator types). A new setting whose key does not spell its name has to be added here on purpose, in front of a reviewer.
     */
    private static final Set<String> ABBREVIATED = new TreeSet<>(Set.of(
        "GREMLIN_COMMAND_TIMEOUT -> arcadedb.gremlin.timeout",
        "HA_K8S_DNS_SUFFIX -> arcadedb.ha.k8sSuffix",
        "INDEX_COMPACTION_FULL_SERIES -> arcadedb.indexCompactionFullSeriesThreshold",
        "INDEX_COMPACTION_RAM_MB -> arcadedb.indexCompactionRAM",
        "NETWORK_SAME_SERVER_ERROR_RETRIES -> arcadedb.network.sameServerErrorRetry",
        "NETWORK_SSL_KEYSTORE -> arcadedb.ssl.keyStore",
        "NETWORK_SSL_KEYSTORE_PASSWORD -> arcadedb.ssl.keyStorePassword",
        "NETWORK_SSL_TRUSTSTORE -> arcadedb.ssl.trustStore",
        "NETWORK_SSL_TRUSTSTORE_PASSWORD -> arcadedb.ssl.trustStorePassword",
        "NETWORK_USE_SSL -> arcadedb.ssl.enabled",
        "SERVER_SECURITY_RELOAD_EVERY -> arcadedb.server.reloadEvery",
        "SERVER_SECURITY_SALT_ITERATIONS -> arcadedb.server.saltIterations",
        "SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES -> arcadedb.server.eventBusMaxPendingBytes",
        "SERVER_WS_EVENT_BUS_QUEUE_SIZE -> arcadedb.server.eventBusQueueSize"));

    @AfterEach
    void restore() {
      System.clearProperty(KEY);
      System.clearProperty(LEGACY_KEY);
      GlobalConfiguration.resetAll();
    }

    @Test
    void theHttpServerIoThreadsKeyNamesTheHttpServer() {
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getKey()).isEqualTo(KEY);
    }

    @Test
    void bothSpellingsResolveToTheSetting() {
      assertThat(GlobalConfiguration.findByKey(KEY)).isSameAs(GlobalConfiguration.SERVER_HTTP_IO_THREADS);
      assertThat(GlobalConfiguration.findByKey(LEGACY_KEY)).isSameAs(GlobalConfiguration.SERVER_HTTP_IO_THREADS);
      assertThat(GlobalConfiguration.findByKey(LEGACY_KEY.toUpperCase(Locale.ENGLISH))).isSameAs(GlobalConfiguration.SERVER_HTTP_IO_THREADS);
    }

    @Test
    void theNaturalSystemPropertyIsApplied() {
      System.setProperty(KEY, "7");

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(7);
    }

    @Test
    void theOldSystemPropertyIsStillApplied() {
      System.setProperty(LEGACY_KEY, "9");

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(9);
    }

    @Test
    void theCurrentSpellingWinsWhenBothAreGiven() {
      System.setProperty(LEGACY_KEY, "9");
      System.setProperty(KEY, "7");

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(7);
    }

    @Test
    void theOldSpellingIsAcceptedByTheBulkSetter() {
      GlobalConfiguration.setConfiguration(Map.of(LEGACY_KEY, 6));
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(6);

      // the current key and the enum name keep working
      GlobalConfiguration.setConfiguration(Map.of(KEY, 4));
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(4);
      GlobalConfiguration.setConfiguration(Map.of("SERVER_HTTP_IO_THREADS", 3));
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(3);

      // both spellings in one map: the current one wins whatever the order, as it does for the system properties
      final Map<String, Object> currentFirst = new LinkedHashMap<>();
      currentFirst.put(KEY, 5);
      currentFirst.put(LEGACY_KEY, 9);
      GlobalConfiguration.setConfiguration(currentFirst);
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(5);

      final Map<String, Object> formerFirst = new LinkedHashMap<>();
      formerFirst.put(LEGACY_KEY, 9);
      formerFirst.put(KEY, 6);
      GlobalConfiguration.setConfiguration(formerFirst);
      assertThat(GlobalConfiguration.SERVER_HTTP_IO_THREADS.getValueAsInteger()).isEqualTo(6);
    }

    @Test
    void theOldSpellingReachesTheServerConfigurationOverlay() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.fromJSON("{\"configuration\":{\"server.httpsIoThreads\":5}}");

      assertThat(configuration.getValueAsInteger(GlobalConfiguration.SERVER_HTTP_IO_THREADS)).isEqualTo(5);
    }

    @Test
    void everyKeySpellsItsConstantExceptTheDeliberateAbbreviations() {
      final Set<String> differing = new TreeSet<>();
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        final String fromKey = setting.getKey().substring(GlobalConfiguration.PREFIX.length()).replace(".", "").toLowerCase(Locale.ENGLISH);
        final String fromName = setting.name().replace("_", "").toLowerCase(Locale.ENGLISH);
        if (!fromKey.equals(fromName))
          differing.add(setting.name() + " -> " + setting.getKey());
      }

      assertThat(differing).as("a key that does not spell its constant's name is unreachable under the natural spelling and"
          + " misleading in a settings dump: rename the key (keeping the old one as a deprecated alias) or, if it is a deliberate"
          + " abbreviation, add it to ABBREVIATED").isEqualTo(ABBREVIATED);
    }
  }

  /**
   * Regression for #7121: a systematic sweep of GlobalConfiguration found four settings that did not do what they said.
   * <ol>
   *   <li>{@code arcadedb.txWalFiles} documented {@code 0 = available cores}, but nothing translated 0, so setting
   *       the advertised value produced a zero-length WAL pool and every commit divided by it.</li>
   *   <li>{@code arcadedb.bucketReuseSpaceMode} is DATABASE-scoped and persisted, but {@code LocalBucket} read the
   *       JVM-global value, so the per-database setting never took effect.</li>
   *   <li>{@code arcadedb.explicitLockTimeout} and {@code arcadedb.ha.snapshotMaxEntrySize} had no reader at all
   *       (the third, {@code arcadedb.cypher.statementCache}, was a dead duplicate of
   *       {@link GlobalConfiguration#OPENCYPHER_STATEMENT_CACHE} and was removed).</li>
   *   <li>{@code arcadedb.server.logFormat} was only honoured on the one-shot logger initialisation, so setting it
   *       through any of its supported SERVER-scoped channels kept the default formatter.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Isolated
  @Nested
  class GlobalConfigurationSweep {
    private static final String DB_PATH = "./target/databases/Issue7121";

    @AfterEach
    void cleanUp() {
      GlobalConfiguration.TX_WAL_FILES.reset();
      GlobalConfiguration.SERVER_LOG_FORMAT.reset();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    /** 1 (end to end): with the documented value set, a database still commits. */
    @Test
    void aDatabaseWithTxWalFilesZeroStillCommits() {
      GlobalConfiguration.TX_WAL_FILES.setValue(0);
      try (final Database db = new DatabaseFactory(DB_PATH).create()) {
        db.transaction(() -> {
          db.getSchema().createDocumentType("WalPool");
          db.newDocument("WalPool").set("v", 1).save();
        });
        assertThat(db.countType("WalPool", false)).isEqualTo(1L);
      }
    }

    /** 3. The two settings that were declared, documented, settable, and read by nothing. */
    @Test
    void explicitLockTimeoutIsDeclaredAsALongInMilliseconds() {
      // The wiring itself is covered by ExplicitLockTimeoutTest; this pins the declaration the reader depends on
      assertThat(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT.getValueAsLong()).isPositive();
      assertThat(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT.getScope()).isEqualTo(GlobalConfiguration.SCOPE.DATABASE);
    }

    @Test
    void theDeadCypherStatementCacheSettingIsGone() {
      assertThat(GlobalConfiguration.findByKey("arcadedb.cypher.statementCache"))
          .as("a duplicate with no reader is worse than no setting: it looks like it tunes the Cypher plan cache")
          .isNull();
      assertThat(GlobalConfiguration.findByKey("arcadedb.opencypher.statementCache"))
          .as("the one that actually sizes the parsed-statement cache stays")
          .isNotNull();
    }

    /** 4. Setting the log format through the configuration must reach the installed console handler. */
    @Test
    void serverLogFormatSwapsTheConsoleFormatter() {
      // Force the one-shot DefaultLogger.init(), so the console handler this test needs is in place
      LogManager.instance().log(GlobalConfigurationSweep.class, Level.FINEST, "bootstrap");

      final java.util.logging.Logger root = java.util.logging.Logger.getLogger("");
      // A JVM can legitimately be configured with no console handler. Rather than skipping - which would make this
      // test silently prove nothing on such a setup - install one for the duration and take it back out.
      ConsoleHandler installedByTest = null;
      if (consoleHandler(root) == null) {
        installedByTest = new ConsoleHandler();
        installedByTest.setFormatter(new AnsiLogFormatter());
        root.addHandler(installedByTest);
      }

      try {
        assertThat(consoleFormatterName()).as("premise: the text formatter is what we start from").isEqualTo("AnsiLogFormatter");

        GlobalConfiguration.SERVER_LOG_FORMAT.setValue("json");
        assertThat(consoleFormatterName())
            .as("the setting is SCOPE.SERVER, so setting it through the configuration must take effect")
            .isEqualTo("JsonLogFormatter");

        GlobalConfiguration.SERVER_LOG_FORMAT.setValue("text");
        assertThat(consoleFormatterName()).as("and it must swap back").isEqualTo("AnsiLogFormatter");
      } finally {
        if (installedByTest != null)
          root.removeHandler(installedByTest);
      }
    }

    /**
     * The channel that was actually broken. A SCOPE.SERVER setting is authoritative in the server's own
     * {@link ContextConfiguration}, and that overlay is a plain map that never writes through to the
     * {@link GlobalConfiguration} enum - so the enum's set-callback alone could not reach the server configuration
     * file, {@code SET SERVER SETTING}, or the MCP {@code set_server_setting} tool. The overlay now runs a declared
     * SCOPE.SERVER setting's side effect itself, which is what makes all three take effect.
     */
    @Test
    void aWriteIntoTheServersOverlayAppliesTheSettingsSideEffect() {
      withOwnConsoleHandler(() -> {
        final ContextConfiguration overlay = new ContextConfiguration();
        overlay.setValue(GlobalConfiguration.SERVER_LOG_FORMAT, "json");

        assertThat(consoleFormatterName())
            .as("a value written only into the overlay must still swap the formatter")
            .isEqualTo("JsonLogFormatter");
        assertThat(GlobalConfiguration.SERVER_LOG_FORMAT.getValueAsString())
            .as("without writing through to the process-wide enum, which the overlay deliberately does not own")
            .isEqualTo("text");

        overlay.setValue(GlobalConfiguration.SERVER_LOG_FORMAT.getKey(), "text");
        assertThat(consoleFormatterName()).as("and the by-key overload is the same path").isEqualTo("AnsiLogFormatter");
      });
    }

    /** The server configuration file goes through fromJSON, which must run the side effect too. */
    @Test
    void loadingAServerConfigurationFileAppliesTheSettingsSideEffect() {
      withOwnConsoleHandler(() -> {
        final ContextConfiguration overlay = new ContextConfiguration();
        overlay.fromJSON("{\"configuration\":{\"server.logFormat\":\"json\"}}");

        assertThat(overlay.getValueAsString(GlobalConfiguration.SERVER_LOG_FORMAT)).isEqualTo("json");
        assertThat(consoleFormatterName())
            .as("the formatter is chosen at logger init, long before a configuration file is read")
            .isEqualTo("JsonLogFormatter");
      });
    }

    /** The mechanism the two server channels use: apply a value the resolver could not have found on its own. */
    @Test
    void refreshConsoleFormatterAppliesAnExplicitValue() {
      withOwnConsoleHandler(() -> {
        DefaultLogger.refreshConsoleFormatter("json");
        assertThat(consoleFormatterName())
            .as("a value read out of the server's ContextConfiguration must be applicable directly")
            .isEqualTo("JsonLogFormatter");

        DefaultLogger.refreshConsoleFormatter("text");
        assertThat(consoleFormatterName()).isEqualTo("AnsiLogFormatter");
      });
    }

    /** A callback is a side effect that must follow the value on the way back down, too. */
    @Test
    void resettingTheSettingAlsoSwapsTheFormatterBack() {
      withOwnConsoleHandler(() -> {
        GlobalConfiguration.SERVER_LOG_FORMAT.setValue("json");
        assertThat(consoleFormatterName()).isEqualTo("JsonLogFormatter");

        GlobalConfiguration.SERVER_LOG_FORMAT.reset();
        assertThat(consoleFormatterName())
            .as("reset() reports the default, so it must apply it as well")
            .isEqualTo("AnsiLogFormatter");
      });
    }

    /**
     * Runs {@code assertions} with a console handler guaranteed to exist. A JVM can legitimately be configured with
     * none; skipping in that case would let these tests silently prove nothing, so one is installed and removed.
     */
    private static void withOwnConsoleHandler(final Runnable assertions) {
      LogManager.instance().log(GlobalConfigurationSweep.class, Level.FINEST, "bootstrap");

      final java.util.logging.Logger root = java.util.logging.Logger.getLogger("");
      ConsoleHandler installedByTest = null;
      if (consoleHandler(root) == null) {
        installedByTest = new ConsoleHandler();
        installedByTest.setFormatter(new AnsiLogFormatter());
        root.addHandler(installedByTest);
      }
      try {
        assertions.run();
      } finally {
        if (installedByTest != null)
          root.removeHandler(installedByTest);
      }
    }

    private static ConsoleHandler consoleHandler(final java.util.logging.Logger root) {
      for (final Handler h : root.getHandlers())
        if (h instanceof ConsoleHandler consoleHandler)
          return consoleHandler;
      return null;
    }

    private static String consoleFormatterName() {
      final ConsoleHandler h = consoleHandler(java.util.logging.Logger.getLogger(""));
      if (h == null)
        return null;
      final Formatter f = h.getFormatter();
      return f != null ? f.getClass().getSimpleName() : null;
    }
  }

  /**
   * Regression for #7297: {@link GlobalConfiguration#findByKey(String)} is case-INSENSITIVE and the
   * {@link ContextConfiguration} overlay is not, so a configuration key written in another case resolved to the
   * right setting, passed the type coercion, the allow-list and the callback, and was then stored under the
   * caller's spelling - where no reader looks, because every reader asks for the setting's declared key.
   * <p>
   * The setting silently kept its default. That is worse than a rejected key: the operator has positive evidence
   * the line was understood - no error, and for a setting with a callback, visible side effects at startup.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ConfigurationKeyCase {

    private static final GlobalConfiguration MUTUAL_AUTH = GlobalConfiguration.HA_TLS_MUTUAL_AUTH;

    @Test
    public void theDeclaredKeyIsNotAlreadyLowercase() {
      // Guards the premise of the whole issue: without a camelCase key in the enum there is nothing to get wrong.
      assertThat(MUTUAL_AUTH.getKey()).isNotEqualTo(MUTUAL_AUTH.getKey().toLowerCase(Locale.ENGLISH));
    }

    @Test
    public void aLowercasedKeyInTheConfigurationFileTakesEffect() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"" + shortKey(MUTUAL_AUTH).toLowerCase(Locale.ENGLISH) + "\":false}}");

      // The read every component actually performs, through the enum constant.
      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
      assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).isEqualTo(Boolean.FALSE);
      // And it is in the map under the DECLARED key, so toJSON round-trips it as one entry rather than two.
      assertThat(cfg.getContextKeys()).containsExactly(MUTUAL_AUTH.getKey());
    }

    @Test
    public void anUppercasedKeyInTheConfigurationFileTakesEffectToo() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"" + shortKey(MUTUAL_AUTH).toUpperCase(Locale.ENGLISH) + "\":false}}");

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
    }

    @Test
    public void theStringKeyedWriterNormalisesToo() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(MUTUAL_AUTH.getKey().toLowerCase(Locale.ENGLISH), false);

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
      assertThat(cfg.getContextKeys()).containsExactly(MUTUAL_AUTH.getKey());
    }

    @Test
    public void theMapConstructorNormalisesToo() {
      final ContextConfiguration cfg = new ContextConfiguration(
          Map.of(MUTUAL_AUTH.getKey().toLowerCase(Locale.ENGLISH), Boolean.FALSE));

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
    }

    @Test
    public void theStringKeyedReadersAgreeWithTheWriters() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(MUTUAL_AUTH, false);

      // Same setting, three spellings, one answer - including the short form without the "arcadedb." prefix,
      // which findByKey adds and which the readers therefore have to accept as well.
      assertThat(cfg.hasValue(MUTUAL_AUTH.getKey().toLowerCase(Locale.ENGLISH))).isTrue();
      assertThat(cfg.hasValue(shortKey(MUTUAL_AUTH))).isTrue();
      assertThat(cfg.<Object>getValue(MUTUAL_AUTH.getKey().toUpperCase(Locale.ENGLISH), Boolean.TRUE)).isEqualTo(
          Boolean.FALSE);
    }

    @Test
    public void aCaseVariantRemovalRemovesTheDeclaredEntry() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(MUTUAL_AUTH, false);

      cfg.setValue(MUTUAL_AUTH.getKey().toLowerCase(Locale.ENGLISH), null);

      assertThat(cfg.getContextKeys()).isEmpty();
      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isEqualTo(MUTUAL_AUTH.getValueAsBoolean());
    }

    @Test
    public void aKeyThatNamesNoSettingKeepsItsOwnSpelling() {
      // A plugin-defined entry: this enum knows nothing about it, so there is no declared key to normalise onto.
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue("arcadedb.myPlugin.someSetting", "x");

      assertThat(cfg.getContextKeys()).containsExactly("arcadedb.myPlugin.someSetting");
      assertThat(cfg.<Object>getValue("arcadedb.myPlugin.someSetting", null)).isEqualTo("x");
    }

    private static String shortKey(final GlobalConfiguration cfg) {
      return cfg.getKey().substring(GlobalConfiguration.PREFIX.length());
    }
  }

  /**
   * Regression for #9316: nineteen of the twenty {@code arcadedb.grpc.*} keys the gRPC plugin reads were bare strings, reachable from -D
   * only. The server configuration file dropped them with a warning and the environment-variable loop never asked for them, so
   * {@code arcadedb.grpc.tls.enabled=true} written in either left a plaintext endpoint listening. They are declared settings now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GrpcSettingsDeclared {
    private static final List<String> KEYS = List.of("enabled", "port", "host", "mode", "xds.port", "tls.enabled", "tls.cert",
        "tls.key", "maxMessageSize", "maxMetadataSize", "maxConcurrentTransactions", "maxConcurrentTransactionsPerPrincipal",
        "reflection.enabled", "health.enabled", "compression.enabled", "compression.force", "compression.type", "tx.maxIdleMs",
        "tx.maxAgeMs", "tx.reaperPeriodMs");

    @Test
    void everyGrpcKeyTheServerPluginReadsIsADeclaredSetting() {
      for (final String key : KEYS)
        assertThat(GlobalConfiguration.findByKey("arcadedb.grpc." + key)).as("arcadedb.grpc." + key).isNotNull();
    }

    @Test
    void theServerConfigurationFileReachesTlsAndTheOtherGrpcKeys() {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON("{\"configuration\":{\"grpc.port\":50099,\"grpc.tls.enabled\":true,\"grpc.tls.cert\":\"/etc/ssl/server.crt\","
          + "\"grpc.tls.key\":\"/etc/ssl/server.key\",\"grpc.reflection.enabled\":false,\"grpc.tx.maxIdleMs\":1234}}");

      assertThat(cfg.getValueAsBoolean(GlobalConfiguration.GRPC_TLS_ENABLED)).isTrue();
      assertThat(cfg.getValueAsString(GlobalConfiguration.GRPC_TLS_CERT)).isEqualTo("/etc/ssl/server.crt");
      assertThat(cfg.getValueAsString(GlobalConfiguration.GRPC_TLS_KEY)).isEqualTo("/etc/ssl/server.key");
      assertThat(cfg.getValueAsBoolean(GlobalConfiguration.GRPC_REFLECTION_ENABLED)).isFalse();
      assertThat(cfg.getValueAsLong(GlobalConfiguration.GRPC_TX_MAX_IDLE_MS)).isEqualTo(1234L);
      assertThat(cfg.getValueAsInteger(GlobalConfiguration.GRPC_PORT)).isEqualTo(50099);
    }

    @Test
    void theDefaultsAreTheOnesThePluginAlwaysUsed() {
      assertThat((Boolean) GlobalConfiguration.GRPC_ENABLED.getDefValue()).isTrue();
      assertThat((Boolean) GlobalConfiguration.GRPC_TLS_ENABLED.getDefValue()).isFalse();
      assertThat(GlobalConfiguration.GRPC_HOST.getDefValue()).isEqualTo("0.0.0.0");
      assertThat(GlobalConfiguration.GRPC_MODE.getDefValue()).isEqualTo("standard");
      assertThat(GlobalConfiguration.GRPC_XDS_PORT.getDefValue()).isEqualTo(50052);
      assertThat(GlobalConfiguration.GRPC_MAX_MESSAGE_SIZE.getDefValue()).isEqualTo(100);
      assertThat(GlobalConfiguration.GRPC_MAX_METADATA_SIZE.getDefValue()).isEqualTo(16);
      assertThat(GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS.getDefValue()).isEqualTo(1000);
      assertThat(GlobalConfiguration.GRPC_MAX_CONCURRENT_TRANSACTIONS_PER_PRINCIPAL.getDefValue()).isEqualTo(100);
      assertThat(GlobalConfiguration.GRPC_COMPRESSION_TYPE.getDefValue()).isEqualTo("gzip");
      assertThat(GlobalConfiguration.GRPC_TX_MAX_IDLE_MS.getDefValue()).isEqualTo(300_000L);
      assertThat(GlobalConfiguration.GRPC_TX_MAX_AGE_MS.getDefValue()).isEqualTo(0L);
      assertThat(GlobalConfiguration.GRPC_TX_REAPER_PERIOD_MS.getDefValue()).isEqualTo(30_000L);
    }
  }

  /**
   * Regression for #7296: the third report in the strict-coercion family (#7222, #7262): admin command allow-list setting.
   * <p>
   * The allow-list check lived on {@code coerceFromConfigurationSource} - the server configuration FILE path - and
   * on {@link GlobalConfiguration#setValue(Object)}, and on neither of the ADMINISTRATIVE writers that share
   * {@link GlobalConfiguration#coerceFromAdminCommand(Object)}: {@code SET SERVER SETTING} over HTTP and over gRPC,
   * the {@code set_server_setting} MCP tool and {@code ALTER DATABASE ... SETTING}. So
   * {@code SET SERVER SETTING arcadedb.server.mode prodction} answered 200, stored {@code "prodction"}, and every
   * reader comparing the stored value with {@code "production"} served the deployment the DEVELOPMENT behaviour -
   * error-detail concealment included.
   * <p>
   * The check now lives on the strict entry point itself, so a writer cannot get the type strictness without also
   * getting the allow-list. {@link #everySettingWithAnAllowListRefusesAnUnlistedValueOnTheAdminPath()} is the guard
   * that keeps it there for settings added later, which is what the previous two rounds of this issue were missing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdminCommandAllowList {

    @AfterEach
    void restoreDefaults() {
      GlobalConfiguration.SERVER_MODE.setValue(GlobalConfiguration.SERVER_MODE.getDefValue());
      GlobalConfiguration.HA_QUORUM.setValue(GlobalConfiguration.HA_QUORUM.getDefValue());
    }

    /** The repro from the report, at the layer every administrative writer shares. */
    @Test
    void anUnlistedServerModeIsRefusedOnTheAdminCommandPath() {
      assertThatThrownBy(() -> GlobalConfiguration.SERVER_MODE.coerceFromAdminCommand("prodction"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("prodction");
    }

    /** What the existing #7262 test asserted, unchanged: a listed value still normalises to its declared spelling. */
    @Test
    void aListedValueStillNormalisesToItsDeclaredSpelling() {
      assertThat(GlobalConfiguration.SERVER_MODE.coerceFromAdminCommand("PRODUCTION")).isEqualTo("production");
      assertThat(GlobalConfiguration.SERVER_MODE.coerceFromAdminCommand("development")).isEqualTo("development");
    }

    /** A setting with no allow-list is untouched by this: anything its type accepts still passes. */
    @Test
    void aSettingWithoutAnAllowListIsUnaffected() {
      assertThat(GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT.coerceFromAdminCommand("ture")).isEqualTo("ture");
      assertThat(GlobalConfiguration.ASYNC_WORKER_THREADS.coerceFromAdminCommand("12")).isEqualTo(12);
    }

    /**
     * An allow-list built from an integer RANGE still accepts every value inside it. The set holds the range as
     * STRINGS ({@code integerRangeAsStrings}) while the coerced value is an {@code Integer}, so this pins that the
     * check still compares them the way {@code setValue} always has - {@code toString()} against the set.
     */
    @Test
    void anIntegerRangeAllowListStillAcceptsItsRange() {
      assertThat(GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.coerceFromAdminCommand("16")).isEqualTo(16);
      assertThat(GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.coerceFromAdminCommand("31")).isEqualTo(31);
      assertThatThrownBy(() -> GlobalConfiguration.OPENCYPHER_ID_BUCKET_BITS.coerceFromAdminCommand("64"))
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The guard that ends the series. EVERY declared setting that has an allow-list must refuse a value outside it
     * on the administrative path - not only the three the reports happened to name. A setting added later with an
     * allow-list and a writer that bypasses this parse fails here rather than in a fourth issue.
     * <p>
     * The probe is chosen per TYPE rather than skipping every non-String setting, which is what this swept before:
     * a numeric setting handed {@code "no-such-value"} refuses it on the type first, and a test that cannot tell
     * that refusal from the allow-list's would have passed on a numeric setting whose allow-list was never
     * consulted. So each type gets a value it CAN represent and the set does not contain.
     */
    @Test
    void everySettingWithAnAllowListRefusesAnUnlistedValueOnTheAdminPath() {
      int checked = 0;
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        if (setting.getAllowed() == null)
          continue;
        final Object unlisted = unlistedValueFor(setting);
        assertThat(unlisted)
            .as("setting '%s' declares an allow-list %s on a type this sweep has no probe for; add one rather than "
                + "leaving it unswept", setting.getKey(), setting.getAllowed())
            .isNotNull();
        ++checked;
        assertThatThrownBy(() -> setting.coerceFromAdminCommand(unlisted))
            .as("setting '%s' declares an allow-list %s and must refuse the value %s, which is outside it",
                setting.getKey(), setting.getAllowed(), unlisted)
            .isInstanceOf(IllegalArgumentException.class);
      }
      assertThat(checked).as("the sweep must actually have found allow-listed settings to check").isPositive();
    }

    /**
     * A value {@code setting}'s declared TYPE accepts and its allow-list does not, or {@code null} when this sweep
     * has no probe for that type - which the caller reports rather than skips, so a new kind of allow-listed setting
     * is noticed here instead of going unchecked.
     */
    private static Object unlistedValueFor(final GlobalConfiguration setting) {
      if (setting.getType() == String.class)
        return "no-such-value-7296";
      if (setting.getType() == Integer.class || setting.getType() == Long.class) {
        // Walk up from 0 until a whole number the set does not contain: every numeric allow-list today is a
        // contiguous range, so this stops immediately past its end and never depends on where that end is.
        for (long candidate = 0; candidate < 100_000; candidate++)
          if (!setting.getAllowed().contains(Long.toString(candidate)))
            return setting.getType() == Integer.class ? (Object) (int) candidate : (Object) candidate;
      }
      return null;
    }

    /**
     * Item 3 of the report: the STATIC {@code fromJSON} twin parses the same document shape as the instance method
     * #7262 hardened, and used {@code setValue} directly - so its {@code Boolean} arm was
     * {@code Boolean.parseBoolean} and {@code "yes"} became {@code false} without a word. Its only caller today is a
     * test, which is why this was latent rather than live; it is closed so that wiring it up later cannot
     * reintroduce the defect silently.
     */
    @Test
    void theStaticFromJsonTwinRefusesAValueItCannotRead() {
      final boolean before = GlobalConfiguration.HA_ENABLED.getValueAsBoolean();
      try {
        GlobalConfiguration.fromJSON("{\"configuration\":{\"" + shortKey(GlobalConfiguration.HA_ENABLED) + "\":\"yes\"}}");

        assertThat(GlobalConfiguration.HA_ENABLED.getValueAsBoolean())
            .as("'yes' is not boolean text: it must be refused and the setting left alone, not read as false")
            .isEqualTo(before);
        assertThat(GlobalConfiguration.HA_ENABLED.isChanged())
            .as("a refused value is not a choice anybody made")
            .isFalse();
      } finally {
        GlobalConfiguration.HA_ENABLED.setValue(GlobalConfiguration.HA_ENABLED.getDefValue());
        GlobalConfiguration.HA_ENABLED.reset();
      }
    }

    /** The same twin still applies a value it CAN read, so the hardening did not turn it into a no-op. */
    @Test
    void theStaticFromJsonTwinStillAppliesAReadableValue() {
      try {
        GlobalConfiguration.fromJSON(
            "{\"configuration\":{\"" + shortKey(GlobalConfiguration.SERVER_MODE) + "\":\"PRODUCTION\"}}");
        assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("production");
      } finally {
        GlobalConfiguration.SERVER_MODE.setValue(GlobalConfiguration.SERVER_MODE.getDefValue());
        GlobalConfiguration.SERVER_MODE.reset();
      }
    }

    /** {@code fromJSON} writes the key without the {@code arcadedb.} prefix {@code findByKey} adds back. */
    private static String shortKey(final GlobalConfiguration setting) {
      return setting.getKey().substring(GlobalConfiguration.PREFIX.length());
    }
  }

  /**
   * Regression for #10446: -Darcadedb.profile=low-ram applied every profile value except maxPageRAM, which got overwritten by its computed default.
   * <p>
   * {@code -Darcadedb.profile=low-ram} applied every value of the profile except {@code arcadedb.maxPageRAM}: the startup
   * pass reaches {@code PROFILE} first, whose callback sets the page cache to 16 MB, and then reaches {@code MAX_PAGE_RAM},
   * which has no property of its own and so applied its computed default (a quarter of the heap) over the profile's value.
   * The same pass also made the first embedded open log {@code Configuration profile PARTIALLY applied}, because the profile
   * callback called {@code PageManager.configure()} while the page manager already counted the database being opened.
   * <p>
   * The precedence is an explicit property or variable, then the profile, then the computed default.
   */
  @Nested
  class ProfileMaxPageRam {

    private static final String PROFILE_KEY      = GlobalConfiguration.PROFILE.getKey();
    private static final String MAX_PAGE_RAM_KEY = GlobalConfiguration.MAX_PAGE_RAM.getKey();

    @AfterEach
    void restore() {
      System.clearProperty(PROFILE_KEY);
      System.clearProperty(MAX_PAGE_RAM_KEY);
      GlobalConfiguration.resetAll();
    }

    @Test
    void theProfileKeepsItsPageCacheSizeThroughTheStartupPass() {
      assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

      System.setProperty(PROFILE_KEY, "low-ram");

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(16L);
    }

    @Test
    void anExplicitPageCacheSizeWinsOverTheProfile() {
      assumeTrue(computedDefaultMB() != 16, "the heap is 64 MB, so the computed default equals the profile's value");

      System.setProperty(PROFILE_KEY, "low-ram");
      System.setProperty(MAX_PAGE_RAM_KEY, "24");

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(24L);
    }

    @Test
    void withoutAProfileThePageCacheIsAQuarterOfTheHeap() {
      GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
    }

    /** A pass that reads the same profile twice must not carry the first pass's assignments into the second. */
    @Test
    void aLaterPassWithoutAProfileGoesBackToTheComputedDefault() {
      System.setProperty(PROFILE_KEY, "low-ram");
      GlobalConfiguration.readConfiguration();
      System.clearProperty(PROFILE_KEY);
      GlobalConfiguration.PROFILE.reset();
      GlobalConfiguration.MAX_PAGE_RAM.setValue(7);

      GlobalConfiguration.readConfiguration();

      assertThat(GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong()).isEqualTo(computedDefaultMB());
    }

    @Test
    void theStartupPassDoesNotWarnThatTheProfileWasPartiallyApplied() {
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      // THE FIRST EMBEDDED OPEN: THE PAGE MANAGER COUNTS THE DATABASE BEFORE THE CONFIGURATION IS READ
      PageManager.INSTANCE.acquire();
      try {
        System.setProperty(PROFILE_KEY, "low-ram");

        GlobalConfiguration.readConfiguration();

        assertThat(captured.text()).doesNotContain("PARTIALLY applied");
      } finally {
        PageManager.INSTANCE.release();
        LogManager.instance().setLogger(previousLogger);
      }
    }

    /** The warning still belongs to a profile that is changed while a database is open. */
    @Test
    void aProfileChangedAtRuntimeWithADatabaseOpenStillWarns() {
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      PageManager.INSTANCE.acquire();
      try {
        GlobalConfiguration.PROFILE.setValue("low-ram");

        assertThat(captured.text()).contains("PARTIALLY applied");
      } finally {
        PageManager.INSTANCE.release();
        LogManager.instance().setLogger(previousLogger);
      }
    }

    /**
     * The bug was not specific to {@code maxPageRAM}: any setting that has a computed default and is also listed by a profile
     * loses to that default in the startup pass. Which of them differ from the profile's value depends on the machine (the
     * time series block cache and the HTTP IO threads match their computed default on a small heap and few cores), so the
     * invariant is checked on every setting instead: what the startup pass produces for a profile is what setting the same
     * profile at runtime produces. A setting with its own property or variable is left out, since that one wins by design.
     */
    @ParameterizedTest
    @ValueSource(strings = { "low-ram", "low-cpu", "high-performance" })
    void theStartupPassLosesNoSettingOfTheProfile(final String profile) {
      GlobalConfiguration.resetAll();
      System.setProperty(PROFILE_KEY, profile);
      GlobalConfiguration.readConfiguration();
      final Map<String, String> atStartup = valuesOfEverySetting();

      System.clearProperty(PROFILE_KEY);
      GlobalConfiguration.resetAll();
      GlobalConfiguration.readConfiguration();
      GlobalConfiguration.PROFILE.setValue(profile);
      final Map<String, String> atRuntime = valuesOfEverySetting();

      final List<String> lost = new ArrayList<>();
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        final String key = setting.getKey();
        if (System.getProperty(key) != null || System.getenv(key) != null)
          continue;
        if (!atStartup.get(key).equals(atRuntime.get(key)))
          lost.add(key + " startup=" + atStartup.get(key) + " profile=" + atRuntime.get(key));
      }

      assertThat(lost).as("settings of the '%s' profile that the startup pass did not keep", profile).isEmpty();
    }

    /**
     * The startup pass runs inside the static initializer of {@link GlobalConfiguration}, so it must not wait for a lock that a
     * thread outside it can hold while waiting for that initializer. {@code PageManager.acquire()} is such a thread: it holds
     * the page manager's lifecycle lock and, on the first open of a JVM, reaches {@code GlobalConfiguration} from inside it.
     * Here one thread sits in {@code PageManager.configure()} (a profile changed at runtime with a database open) holding that
     * lock, and the startup pass has to finish all the same.
     */
    @Test
    void theStartupPassDoesNotWaitForThePageManagerLifecycleLock() throws Exception {
      final HoldingLogger holding = new HoldingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(holding);
      PageManager.INSTANCE.acquire();
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        // THE HOLDER TAKES THE LIFECYCLE LOCK IN configure() AND STAYS IN IT, BLOCKED IN THE LOGGER
        final Future<?> holder = executor.submit(() -> {
          holding.holder = Thread.currentThread();
          GlobalConfiguration.PROFILE.setValue("low-ram");
        });
        assertThat(holding.inside.await(30, TimeUnit.SECONDS)).as("the holder reached PageManager.configure()").isTrue();

        System.setProperty(PROFILE_KEY, "low-ram");
        final Future<?> pass = executor.submit(GlobalConfiguration::readConfiguration);
        // A HANG DETECTOR, NOT A LATENCY BOUND: THE PASS TAKES MILLISECONDS WHEN IT DOES NOT WAIT
        pass.get(30, TimeUnit.SECONDS);

        holding.release.countDown();
        holder.get(30, TimeUnit.SECONDS);
      } finally {
        holding.release.countDown();
        executor.shutdownNow();
        PageManager.INSTANCE.release();
        LogManager.instance().setLogger(previousLogger);
      }
    }

    /**
     * The exact shape of the report, which an in-process test cannot reproduce because {@link GlobalConfiguration} is already
     * loaded: a fresh JVM whose first use of the engine is creating a database, under {@code -Darcadedb.profile=low-ram}. The
     * heap is 512 MB, so the computed page cache is 128 MB and cannot be mistaken for the profile's 16 MB.
     */
    @Test
    void theFirstEmbeddedOpenOfAJvmUnderTheProfileKeepsThePageCacheSmallAndDoesNotWarn(@TempDir final Path directory)
        throws Exception {
      final Path result = directory.resolve("result.txt");
      final Path log = directory.resolve("child.log");

      final List<String> command = new ArrayList<>();
      command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
      command.add("-Xmx512m");
      command.add("-D" + PROFILE_KEY + "=low-ram");
      command.add("--add-exports");
      command.add("java.management/sun.management=ALL-UNNAMED");
      command.add("--add-opens");
      command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
      command.add("--add-opens");
      command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
      command.add("--add-modules");
      command.add("jdk.incubator.vector");
      command.add("--enable-native-access=ALL-UNNAMED");
      command.add("-cp");
      command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
      command.add(ProfileMaxPageRam.class.getName());
      command.add(directory.resolve("database").toString());
      command.add(result.toString());

      final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        // A HANG DETECTOR: THE CHILD CREATES AN EMPTY DATABASE AND EXITS
        assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("the child JVM exited").isTrue();
      } finally {
        if (process.isAlive())
          process.destroyForcibly();
      }

      final String output = Files.readString(log);
      assertThat(process.exitValue()).as(output).isZero();
      assertThat(output).doesNotContain("PARTIALLY applied");
      assertThat(Files.readString(result)).isEqualTo("maxPageRAM=16 pageManagerMaxRAM=16");
    }

    /** The child JVM of {@link #theFirstEmbeddedOpenOfAJvmUnderTheProfileKeepsThePageCacheSmallAndDoesNotWarn}. */
    public static void main(final String[] args) throws IOException {
      final DatabaseFactory factory = new DatabaseFactory(args[0]);
      final Database database = factory.create();
      try {
        Files.writeString(Path.of(args[1]), "maxPageRAM=" + GlobalConfiguration.MAX_PAGE_RAM.getValueAsLong() + " pageManagerMaxRAM="
            + PageManager.INSTANCE.getStats().maxRAM / 1024 / 1024);
      } finally {
        database.drop();
        factory.close();
      }
    }

    private static Map<String, String> valuesOfEverySetting() {
      final Map<String, String> values = new LinkedHashMap<>();
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        final Object value = setting.getValue();
        values.put(setting.getKey(), String.valueOf(value));
      }
      return values;
    }

    private static long computedDefaultMB() {
      return Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024;
    }

    /** Blocks the thread that logs the partial-application warning, so that thread keeps the page manager's lifecycle lock. */
    private static final class HoldingLogger implements Logger {
      final    CountDownLatch inside  = new CountDownLatch(1);
      final    CountDownLatch release = new CountDownLatch(1);
      volatile Thread         holder;

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        holdIfWarning(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        holdIfWarning(message);
      }

      @Override
      public void flush() {
        // NO-OP
      }

      private void holdIfWarning(final String message) {
        if (message == null || !message.contains("PARTIALLY applied") || Thread.currentThread() != holder)
          return;
        inside.countDown();
        try {
          release.await(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private static final class CapturingLogger implements Logger {
      private final StringBuilder buffer = new StringBuilder();

      String text() {
        return buffer.toString();
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        append(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        append(message);
      }

      @Override
      public void flush() {
        // NO-OP
      }

      private synchronized void append(final String message) {
        if (message != null)
          buffer.append(message).append('\n');
      }
    }
  }

  /**
   * Regression for #7163: {@link ContextConfiguration} ran a SCOPE.SERVER setting's callback but
   * not a SCOPE.DATABASE one, so the settings whose callback does real work were stored per database and never
   * applied - {@code arcadedb.maxPageRAM} kept an unclamped page-cache size, and {@code arcadedb.dateImplementation}
   * kept a raw class name that {@code coerce()} had no case for either.
   * <p>
   * The hook now covers every scope but JVM, its result is what the overlay stores (so a callback that normalises
   * normalises on every channel), and the class-name conversion moved into {@code coerce()} where the strict admin
   * parse reaches it too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DatabaseScopeCallback {
    private static final String DB_PATH = "./target/databases/issue-7163-database-scope-callback";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
      GlobalConfiguration.MAX_PAGE_RAM.reset();
      GlobalConfiguration.DATE_IMPLEMENTATION.reset();
      GlobalConfiguration.DATE_TIME_IMPLEMENTATION.reset();
    }

    /**
     * The clamp is the whole point of {@code MAX_PAGE_RAM}'s callback: a page cache above 80% of the heap is cut
     * to half the heap. Written into an overlay it used to be stored verbatim, callback unrun.
     */
    @Test
    void maxPageRamIsClampedWhenWrittenIntoAnOverlay() {
      final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

      final ContextConfiguration overlay = new ContextConfiguration();
      overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM, absurd);

      assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM))
          .as("clamped, as the enum's own setValue clamps it").isLessThan(absurd)
          .isEqualTo(Runtime.getRuntime().maxMemory() / 2 / 1024 / 1024);
    }

    /** The same write through the untyped overload, which is what the admin commands use. */
    @Test
    void maxPageRamIsClampedThroughTheUntypedOverloadToo() {
      final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

      final ContextConfiguration overlay = new ContextConfiguration();
      overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM.getKey(), Long.toString(absurd));

      assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isLessThan(absurd);
    }

    /** A value inside the budget is stored as it was asked for. */
    @Test
    void aMaxPageRamInsideTheBudgetIsLeftAlone() {
      final ContextConfiguration overlay = new ContextConfiguration();
      overlay.setValue(GlobalConfiguration.MAX_PAGE_RAM, 16L);

      assertThat(overlay.getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isEqualTo(16L);
    }

    /** And so does the database's own configuration, reached through ALTER DATABASE. */
    @Test
    void alterDatabaseClampsMaxPageRamAndReportsTheValueThatTookEffect() {
      final long absurd = Runtime.getRuntime().maxMemory() / 1024 / 1024 * 100;

      try (final ResultSet resultSet = database.command("sql", "ALTER DATABASE `arcadedb.maxPageRAM` " + absurd)) {
        final Result row = resultSet.next();
        assertThat(((Number) row.getProperty("newValue")).longValue())
            .as("the result row reports the value that took effect, not the one asked for").isLessThan(absurd);
      }

      assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.MAX_PAGE_RAM)).isLessThan(absurd);
    }

    /** {@code coerce()} now knows the Class case, so the enum and the strict admin parse share one conversion. */
    @Test
    void aClassTypedSettingCoercesFromItsClassName() {
      assertThat(GlobalConfiguration.DATE_IMPLEMENTATION.coerce("java.util.Date")).isEqualTo(Date.class);
      assertThat(GlobalConfiguration.DATE_IMPLEMENTATION.coerce(Date.class)).isEqualTo(Date.class);
      assertThat(GlobalConfiguration.DATE_TIME_IMPLEMENTATION.coerceFromAdminCommand("java.time.LocalDateTime"))
          .isEqualTo(LocalDateTime.class);
    }

    /** An unknown class name is refused where it enters instead of surfacing from the next reader. */
    @Test
    void anUnknownClassNameIsRefused() {
      assertThatThrownBy(() -> GlobalConfiguration.DATE_IMPLEMENTATION.coerceFromAdminCommand("no.such.Clazz"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("arcadedb.dateImplementation");

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'no.such.Clazz'").close())
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * ALTER DATABASE keeps working end to end: the class name is validated, stored by NAME so the database's
     * JSON configuration can hold it, and this database's serializer is pointed at it.
     */
    @Test
    void alterDatabaseSetsTheDateImplementationAndPersistsItByName() {
      database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'").close();

      assertThat(((DatabaseInternal) database).getSerializer().getDateImplementation()).isEqualTo(Date.class);
      assertThat(database.getConfiguration().<Object>getValue(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
      assertThat(database.getConfiguration().toJSON()).contains("java.util.Date");

      final ContextConfiguration reloaded = new ContextConfiguration();
      reloaded.fromJSON(database.getConfiguration().toJSON());
      assertThat(reloaded.<Object>getValue(GlobalConfiguration.DATE_IMPLEMENTATION)).isEqualTo("java.util.Date");
    }

    /**
     * Every write into an overlay resolves the key through {@code GlobalConfiguration.findByKey}, which this
     * change turned from a linear scan of {@code values()} into a hash lookup. A key that stopped resolving would
     * silently take a setting back to "stored and never applied", which is the whole subject of this issue.
     */
    @Test
    void everySettingResolvesByItsOwnKey() {
      for (final GlobalConfiguration setting : GlobalConfiguration.values()) {
        assertThat(GlobalConfiguration.findByKey(setting.getKey())).as(setting.getKey()).isSameAs(setting);
        final String suffix = setting.getKey().substring(GlobalConfiguration.PREFIX.length());
        assertThat(GlobalConfiguration.findByKey(GlobalConfiguration.PREFIX + suffix.toUpperCase(Locale.ENGLISH)))
            .as("case insensitive, as it always was: " + setting.getKey()).isSameAs(setting);
        assertThat(GlobalConfiguration.findByKey(suffix))
            .as("and reachable without the prefix: " + setting.getKey()).isSameAs(setting);
      }
      assertThat(GlobalConfiguration.findByKey("arcadedb.thereIsNoSuchSetting")).isNull();
    }

    /**
     * Pins which settings the overlay hook now reaches. Widening it past SCOPE.SERVER means the callback of ANY
     * non-JVM setting fires on every channel its scope advertises, which is what this issue asked for - and a
     * callback written on the assumption that it only ever runs from the enum's own {@code setValue}, the way the
     * date implementations' used to be, would quietly start running everywhere. Asserting the list makes adding
     * one a decision rather than a discovery.
     */
    @Test
    void onlyTheAuditedSettingsCarryACallbackTheOverlayHookReaches() {
      final List<String> reached = new ArrayList<>();
      final List<String> notReached = new ArrayList<>();
      for (final GlobalConfiguration setting : GlobalConfiguration.values())
        if (setting.hasCallback())
          (setting.getScope() == GlobalConfiguration.SCOPE.JVM ? notReached : reached).add(setting.getKey());

      assertThat(reached).as("reached by ContextConfiguration.setValue/fromJSON")
          .containsExactlyInAnyOrder(GlobalConfiguration.MAX_PAGE_RAM.getKey(),
              GlobalConfiguration.SERVER_LOG_FORMAT.getKey());
      assertThat(notReached).as("SCOPE.JVM: process-wide, so an overlay is not a channel for them")
          .containsExactlyInAnyOrder(GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.getKey(),
              GlobalConfiguration.DUMP_METRICS_EVERY.getKey(), GlobalConfiguration.PROFILE.getKey(),
              GlobalConfiguration.LOG_IMPL.getKey());
    }

    /**
     * ALTER DATABASE alters DATABASE settings, and now says so. Its key lookup accepts any declared key while the
     * command is gated only by the per-database UPDATE_DATABASE_SETTINGS permission, so once a declared callback
     * runs on every channel its scope advertises (this issue), reaching a SCOPE.SERVER key from here would have
     * swapped the console log format of the whole server under a database-level permission - and saved a server
     * setting into one database's configuration, where nothing reads it back.
     */
    @Test
    void alterDatabaseRefusesASettingThatIsNotDatabaseScoped() {
      final String logFormatBefore = GlobalConfiguration.SERVER_LOG_FORMAT.getValueAsString();

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.server.logFormat` 'json'").close())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("SERVER")
          .hasMessageContaining("SET SERVER SETTING");

      assertThat(GlobalConfiguration.SERVER_LOG_FORMAT.getValueAsString())
          .as("the refused command must not have run the setting's process-wide side effect").isEqualTo(logFormatBefore);
      assertThat(database.getConfiguration().hasValue(GlobalConfiguration.SERVER_LOG_FORMAT.getKey()))
          .as("nor stored a server setting in this database").isFalse();

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.profile` 'low-ram'").close())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("JVM");
    }

    /** The DATABASE-scoped settings it exists for keep working. */
    @Test
    void alterDatabaseStillSetsADatabaseScopedSetting() {
      database.command("sql", "ALTER DATABASE `arcadedb.txWAL` false").close();

      assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();
    }

    /**
     * Both halves of the result row spell a Class-typed setting the same way - as the class NAME. {@code oldValue}
     * reads the stored value, which for this setting can be the {@code Class} itself (the enum's own default is),
     * and {@code Class.toString()} is "class java.time.LocalDate": neither what anyone wrote nor anything they
     * could write back.
     */
    @Test
    void alterDatabaseReportsAClassTypedSettingByNameOnBothSides() {
      try (final ResultSet resultSet = database.command("sql",
          "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'")) {
        final Result row = resultSet.next();
        assertThat(row.<Object>getProperty("oldValue")).as("the enum default, by name").isEqualTo("java.time.LocalDate");
        assertThat(row.<Object>getProperty("newValue")).isEqualTo("java.util.Date");
      }

      try (final ResultSet resultSet = database.command("sql",
          "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Calendar'")) {
        final Result row = resultSet.next();
        assertThat(row.<Object>getProperty("oldValue")).as("and what the previous command stored").isEqualTo("java.util.Date");
        assertThat(row.<Object>getProperty("newValue")).isEqualTo("java.util.Calendar");
      }
    }

    /** A SCOPE.JVM setting is deliberately NOT reached through an overlay: it is not what an overlay holds. */
    @Test
    void aJvmScopedSettingIsNotAppliedThroughAnOverlay() {
      final String before = GlobalConfiguration.PROFILE.getValueAsString();

      final ContextConfiguration overlay = new ContextConfiguration();
      overlay.setValue(GlobalConfiguration.PROFILE, "low-ram");

      assertThat(overlay.getValueAsString(GlobalConfiguration.PROFILE)).isEqualTo("low-ram");
      assertThat(GlobalConfiguration.PROFILE.getValueAsString())
          .as("a per-database write must not reconfigure the whole JVM").isEqualTo(before);
    }
  }

  /**
   * Regression for #8038: the startup configuration dump masked secrets by setting, not by content.
   * dump printed every setting as {@code isHidden() ? "<hidden>" : getValue()}, which is the same predicate the
   * settings readers stopped using: it masks a setting that IS a secret and says nothing about a setting that
   * CONTAINS one. {@code arcadedb.server.defaultDatabases} is the second kind, so an operator who turns on
   * {@code arcadedb.dumpConfigAtStartup} - the documented way to record what a server booted with - wrote every
   * database password into the server log in clear.
   * <p>
   * The dump now renders through {@link GlobalConfiguration#publishableValue(Object)}, the rule
   * {@code GET /api/v1/server} and the MCP {@code get_server_settings} tool already publish under. The
   * {@code <hidden>} spelling a wholly hidden setting prints is unchanged.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class DumpConfigurationRedaction {

    private static final String CONFIGURED = "Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]";

    @Test
    void theStartupDumpDoesNotPrintTheDefaultDatabasesPasswords() {
      final String dump = dumpWithDefaultDatabases(CONFIGURED);

      assertThat(dump).as("every password embedded in the value").doesNotContain("einstein");
      assertThat(dump).doesNotContain("Miner").doesNotContain("Tramiel");
    }

    /**
     * What the dump is read for survives: which databases the server was told to create and who may reach them
     * with which role. Only the one field the server authenticates with is replaced.
     */
    @Test
    void theStartupDumpStillNamesTheDatabasesAndTheirUsers() {
      final String dump = dumpWithDefaultDatabases(CONFIGURED);

      assertThat(dump).contains(GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey() + " = "
          + GlobalConfiguration.SERVER_DEFAULT_DATABASES.publishableValue(CONFIGURED));
    }

    /** A wholly hidden setting keeps the spelling the dump has always used for it. */
    @Test
    void aWhollyHiddenSettingStillPrintsAsHidden() {
      final GlobalConfiguration setting = GlobalConfiguration.SERVER_ROOT_PASSWORD;
      final Object previous = setting.getValue();
      final boolean wasChanged = setting.isChanged();
      try {
        setting.setValue("s3cr3t-root-password");

        final String dump = dump();

        assertThat(dump).contains(setting.getKey() + " = <hidden>");
        assertThat(dump).doesNotContain("s3cr3t-root-password");
      } finally {
        restore(setting, previous, wasChanged);
      }
    }

    /** A setting carrying no secret is still printed, or the dump cannot say what the server booted with. */
    @Test
    void anOrdinarySettingIsStillPrinted() {
      assertThat(dump()).contains(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey() + " = ");
    }

    private static String dumpWithDefaultDatabases(final String value) {
      final GlobalConfiguration setting = GlobalConfiguration.SERVER_DEFAULT_DATABASES;
      final Object previous = setting.getValue();
      final boolean wasChanged = setting.isChanged();
      try {
        setting.setValue(value);
        return dump();
      } finally {
        restore(setting, previous, wasChanged);
      }
    }

    /**
     * Puts a process-wide setting back exactly as it was, {@link GlobalConfiguration#isChanged()} included, the way
     * {@code ConfigurationBooleanCoercionTest.ConfigurationFileBooleanStrict} already does. Neither half alone is that: {@code setValue}
     * marks a setting that was UNSET as explicitly configured, which the whole suite shares and
     * {@code GlobalConfigurationTest} asserts on, while {@code reset()} restores the COMPILED-IN default and would
     * discard a {@code -D} the surrounding run was given.
     */
    private static void restore(final GlobalConfiguration setting, final Object value, final boolean wasChanged) {
      if (wasChanged)
        setting.setValue(value);
      else
        setting.reset();
    }

    private static String dump() {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      GlobalConfiguration.dumpConfiguration(new PrintStream(out));
      return out.toString();
    }
  }

  /**
   * Regression for #7281: the startup configuration dump printed compiled-in defaults for settings declared after DUMP_CONFIG_AT_STARTUP.
   * <p>
   * The startup dump fires from {@code DUMP_CONFIG_AT_STARTUP}'s own value callback, and that setting is the FIRST
   * constant of the enum - so it used to run while {@link GlobalConfiguration#readConfiguration()} was still walking
   * {@code values()}, printing the compiled-in DEFAULT of every setting declared after it. The reporter of #7281 read
   * {@code arcadedb.serverMetrics.tracing.enabled = false} out of the startup log while {@code /api/v1/server}
   * correctly answered {@code true} for the same setting, and reasonably concluded the flag had not been applied.
   * <p>
   * The dump has to describe the configuration that {@code readConfiguration()} produced, not the one it started from.
   */
  @Nested
  class StartupConfigDumpOrder {

    /** A setting declared AFTER {@code DUMP_CONFIG_AT_STARTUP}, which is the whole point. */
    private static final GlobalConfiguration LATER_SETTING = GlobalConfiguration.SERVER_METRICS_TRACING_ENDPOINT;
    private static final String              CONFIGURED    = "http://otel-agent.issue7281:4317";

    @Test
    void theStartupDumpShowsTheValuesThatWereJustAppliedNotTheDefaults() {
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      try {
        System.setProperty(GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.getKey(), "true");
        System.setProperty(LATER_SETTING.getKey(), CONFIGURED);

        GlobalConfiguration.readConfiguration();

        assertThat(captured.text()).contains(LATER_SETTING.getKey() + " = " + CONFIGURED);
        assertThat(captured.text()).doesNotContain(LATER_SETTING.getKey() + " = " + LATER_SETTING.getDefValue());
      } finally {
        System.clearProperty(GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.getKey());
        System.clearProperty(LATER_SETTING.getKey());
        GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.reset();
        LATER_SETTING.reset();
        LogManager.instance().setLogger(previousLogger);
      }
    }

    /**
     * Turning the setting on outside {@code readConfiguration()} still dumps straight away: there is no in-flight pass
     * to wait for. That means a direct {@code setValue()} on the setting, which is the only channel that runs this
     * setting's callback at all - the {@link ContextConfiguration} overlay writes (a server configuration file,
     * {@code SET SERVER SETTING}, the MCP tool) go through {@code applyContextValue}, which returns early for
     * anything that is not {@code SCOPE.SERVER}, and {@code DUMP_CONFIG_AT_STARTUP} is {@code SCOPE.JVM}. That is
     * true before and after #7281; this test pins the behaviour the deferral had to preserve, not a new promise.
     */
    @Test
    void settingTheFlagAtRuntimeStillDumpsImmediately() {
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      try {
        GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.setValue(true);

        assertThat(captured.text()).contains("ARCADEDB").contains(" configuration:");
      } finally {
        GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.reset();
        LogManager.instance().setLogger(previousLogger);
      }
    }

    /**
     * Regression test: {@code reset()} runs the setting's callback (#7121), and the callback used to dump whatever the
     * value was, so every {@code resetAll()} - which test fixtures call - printed the whole configuration although the
     * flag was never turned on. Only {@code true} may dump.
     */
    @Test
    void resettingTheFlagToItsDefaultDoesNotDump() {
      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      try {
        GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.reset();
        GlobalConfiguration.resetAll();
        GlobalConfiguration.DUMP_CONFIG_AT_STARTUP.setValue(false);

        assertThat(captured.text()).doesNotContain(" configuration:");
      } finally {
        LogManager.instance().setLogger(previousLogger);
      }
    }

    private static final class CapturingLogger implements Logger {
      private final StringBuilder buffer = new StringBuilder();

      String text() {
        return buffer.toString();
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        append(message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        append(message);
      }

      @Override
      public void flush() {
        // NO-OP
      }

      private synchronized void append(final String message) {
        if (message != null)
          buffer.append(message).append('\n');
      }
    }
  }

  /**
   * Regression for #11119: GlobalConfiguration started a metrics dump timer thread whatever dumpMetricsEvery said.
   * <p>
   * The static initializer of {@link GlobalConfiguration} started a daemon {@code java.util.Timer} whatever
   * {@code arcadedb.dumpMetricsEvery} said, and nothing ever cancelled it. On the JVM that is one idle thread; in a native image it
   * keeps the isolate from being torn down, because {@code java.util.TimerThread} ignores interrupts. The timer must exist only
   * while a positive period is set, and a write of the setting replaces the schedule instead of adding a second one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MetricsDumpTimer {
    private static final String TIMER_THREAD_CLASS = "java.util.TimerThread";
    private static final String THREAD_NAME        = "ArcadeDB-DumpMetrics";

    @AfterEach
    void restore() {
      GlobalConfiguration.DUMP_METRICS_EVERY.reset();
    }

    /**
     * The exact shape of the report, which an in-process test cannot reproduce because {@link GlobalConfiguration} is already
     * loaded: a fresh JVM reads one setting, creates and drops a database, and must be left with no timer thread.
     */
    @Test
    void aFreshJvmThatNeverAsksForMetricsDumpsStartsNoTimerThread(@TempDir final Path directory) throws Exception {
      final Path result = directory.resolve("result.txt");
      final Path log = directory.resolve("child.log");

      final List<String> command = new ArrayList<>();
      command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
      command.add("-Xmx256m");
      command.add("--add-exports");
      command.add("java.management/sun.management=ALL-UNNAMED");
      command.add("--add-opens");
      command.add("java.base/java.util.concurrent.atomic=ALL-UNNAMED");
      command.add("--add-opens");
      command.add("java.base/java.nio.channels.spi=ALL-UNNAMED");
      command.add("--add-modules");
      command.add("jdk.incubator.vector");
      command.add("--enable-native-access=ALL-UNNAMED");
      command.add("-cp");
      command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
      command.add(MetricsDumpTimer.class.getName());
      command.add(directory.resolve("database").toString());
      command.add(result.toString());

      final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
      try {
        // A HANG DETECTOR: THE CHILD CREATES AN EMPTY DATABASE AND EXITS
        assertThat(process.waitFor(120, TimeUnit.SECONDS)).as("the child JVM exited").isTrue();
      } finally {
        if (process.isAlive())
          process.destroyForcibly();
      }

      final String output = Files.readString(log);
      assertThat(process.exitValue()).as(output).isZero();
      assertThat(Files.readString(result)).isEqualTo("afterSetting=0 afterDatabase=0");
    }

    /** The child JVM of {@link #aFreshJvmThatNeverAsksForMetricsDumpsStartsNoTimerThread}. */
    public static void main(final String[] args) throws IOException {
      final long every = GlobalConfiguration.DUMP_METRICS_EVERY.getValueAsLong();
      if (every != 0)
        throw new IllegalStateException("dumpMetricsEvery is " + every);
      final int afterSetting = timerThreads().size();

      final DatabaseFactory factory = new DatabaseFactory(args[0]);
      final Database database = factory.create();
      database.drop();
      factory.close();

      Files.writeString(Path.of(args[1]), "afterSetting=" + afterSetting + " afterDatabase=" + timerThreads().size());
    }

    @Test
    void theTimerLivesOnlyWhileAPositivePeriodIsSet() throws Exception {
      final PrintStream previousOut = System.out;
      // THE CALLBACK DUMPS THE METRICS ONCE WHEN A PERIOD IS SET: KEEP THEM OUT OF THE BUILD LOG
      System.setOut(new PrintStream(OutputStream.nullOutputStream()));
      try {
        assertThat(dumpMetricsThreads()).isEmpty();

        GlobalConfiguration.DUMP_METRICS_EVERY.setValue(3600L);
        assertThat(dumpMetricsThreads()).hasSize(1);

        // A SECOND WRITE REPLACES THE SCHEDULE: THE OLD TIMER IS CANCELLED, NOT KEPT RUNNING NEXT TO THE NEW ONE
        final Thread first = dumpMetricsThreads().getFirst();
        GlobalConfiguration.DUMP_METRICS_EVERY.setValue(1800L);
        first.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(first.isAlive()).isFalse();
        assertThat(dumpMetricsThreads()).hasSize(1);

        final Thread second = dumpMetricsThreads().getFirst();
        GlobalConfiguration.DUMP_METRICS_EVERY.setValue(0L);
        second.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(second.isAlive()).isFalse();
        assertThat(dumpMetricsThreads()).isEmpty();
      } finally {
        System.setOut(previousOut);
      }
    }

    @Test
    void resettingTheSettingCancelsTheTimer() throws Exception {
      final PrintStream previousOut = System.out;
      System.setOut(new PrintStream(OutputStream.nullOutputStream()));
      try {
        GlobalConfiguration.DUMP_METRICS_EVERY.setValue(3600L);
        final Thread thread = dumpMetricsThreads().getFirst();

        GlobalConfiguration.DUMP_METRICS_EVERY.reset();
        thread.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(thread.isAlive()).isFalse();
      } finally {
        System.setOut(previousOut);
      }
    }

    private static List<Thread> timerThreads() {
      final List<Thread> found = new ArrayList<>();
      for (final Thread t : Thread.getAllStackTraces().keySet())
        if (t.isAlive() && TIMER_THREAD_CLASS.equals(t.getClass().getName()))
          found.add(t);
      return found;
    }

    private static List<Thread> dumpMetricsThreads() {
      final List<Thread> found = new ArrayList<>();
      for (final Thread t : timerThreads())
        if (THREAD_NAME.equals(t.getName()))
          found.add(t);
      return found;
    }
  }
}
