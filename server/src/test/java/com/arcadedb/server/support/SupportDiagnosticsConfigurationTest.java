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
package com.arcadedb.server.support;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code configuration.nonDefault} lists only what the operator supplied (a -D, an environment variable, the server
 * configuration file, a SET SERVER SETTING); a value the server derived itself is {@code configuration.computed}, so a
 * vanilla server does not look customised in the portal.
 */
class SupportDiagnosticsConfigurationTest {
  private static final String PAGE_RAM = GlobalConfiguration.MAX_PAGE_RAM.getKey();
  private static final String IO_THREADS = GlobalConfiguration.SERVER_HTTP_IO_THREADS.getKey();
  private static final String QUERY_RANGE = GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getKey();

  @AfterEach
  void clearProperties() {
    System.clearProperty(PAGE_RAM);
    System.clearProperty(QUERY_RANGE);
  }

  private static JSONObject build(final ContextConfiguration cfg, final Map<String, String> operator, final Map<String, String> env) {
    return SupportDiagnostics.buildConfiguration(cfg, operator::get, env::get, new SupportRedactor.Session());
  }

  private static boolean has(final JSONArray array, final String key) {
    for (int i = 0; i < array.length(); i++)
      if (key.equals(array.getJSONObject(i).getString("key")))
        return true;
    return false;
  }

  private static JSONObject entry(final JSONArray array, final String key) {
    for (int i = 0; i < array.length(); i++)
      if (key.equals(array.getJSONObject(i).getString("key")))
        return array.getJSONObject(i);
    throw new AssertionError("no entry " + key + " in " + array);
  }

  @Test
  void aVanillaServerHasNoNonDefaultAndTheDerivedValuesAreComputed() {
    final ContextConfiguration cfg = new ContextConfiguration();
    // what the server itself does at startup: nothing here was supplied by an operator
    cfg.setValue(GlobalConfiguration.MAX_PAGE_RAM, 3072L);
    cfg.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 18);
    cfg.setValue(GlobalConfiguration.SERVER_PLUGINS, "AutoBackupSchedulerPlugin,");

    final JSONObject json = build(cfg, Map.of(), Map.of());

    // (not a count: the JVM running the tests has global settings of its own)
    for (final String key : new String[] { PAGE_RAM, IO_THREADS, GlobalConfiguration.SERVER_PLUGINS.getKey() })
      assertThat(has(json.getJSONArray("nonDefault"), key)).as(key).isFalse();
    final JSONArray computed = json.getJSONArray("computed");
    assertThat(has(computed, PAGE_RAM)).isTrue();
    assertThat(has(computed, IO_THREADS)).isTrue();
    assertThat(entry(computed, PAGE_RAM).has("source")).isFalse();
  }

  @Test
  void aSystemPropertyAnEnvironmentVariableAFileKeyAndARuntimeSettingAreNonDefaultWithTheirSource() {
    System.setProperty(PAGE_RAM, "2048");
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.MAX_PAGE_RAM, 2048L);
    cfg.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 7);
    cfg.setValue(GlobalConfiguration.QUERY_MAX_RANGE_SIZE, 123L);
    cfg.setValue(GlobalConfiguration.SERVER_NAME, "node-a");
    final Map<String, String> operator = new HashMap<>();
    operator.put(IO_THREADS, "file");
    operator.put(GlobalConfiguration.SERVER_NAME.getKey(), "runtime");
    final Map<String, String> env = Map.of(QUERY_RANGE, "123");

    final JSONObject json = build(cfg, operator, env);

    final JSONArray nonDefault = json.getJSONArray("nonDefault");
    assertThat(entry(nonDefault, PAGE_RAM).getString("source")).isEqualTo("jvm");
    assertThat(entry(nonDefault, QUERY_RANGE).getString("source")).isEqualTo("env");
    assertThat(entry(nonDefault, IO_THREADS).getString("source")).isEqualTo("file");
    assertThat(entry(nonDefault, GlobalConfiguration.SERVER_NAME.getKey()).getString("source")).isEqualTo("runtime");
    for (final String key : new String[] { PAGE_RAM, IO_THREADS, QUERY_RANGE, GlobalConfiguration.SERVER_NAME.getKey() })
      assertThat(has(json.getJSONArray("computed"), key)).as(key).isFalse();
  }

  @Test
  void aKeyThatIsOnlyInTheContextAndNotMarkedByTheOperatorIsNotNonDefault() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_NAME, "set-by-the-server-itself");

    final JSONObject json = build(cfg, Map.of(), Map.of());

    assertThat(has(json.getJSONArray("nonDefault"), GlobalConfiguration.SERVER_NAME.getKey())).isFalse();
    assertThat(has(json.getJSONArray("computed"), GlobalConfiguration.SERVER_NAME.getKey())).isTrue();
  }

  @Test
  void hiddenSettingsAreOnlyNamesInMaskedWhoeverSetThem() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "s3cret-value-123");

    final JSONObject operatorSet = build(cfg, Map.of(GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey(), "file"), Map.of());
    final JSONObject serverSet = build(cfg, Map.of(), Map.of());

    for (final JSONObject json : new JSONObject[] { operatorSet, serverSet }) {
      assertThat(json.getJSONArray("masked").toList()).contains(GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey());
      assertThat(json.toString()).doesNotContain("s3cret-value-123");
      assertThat(has(json.getJSONArray("nonDefault"), GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey())).isFalse();
      assertThat(has(json.getJSONArray("computed"), GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey())).isFalse();
    }
  }

  @Test
  void theServerRecordsWhatTheEmbedderHandedInAndWhatAnOperatorSetsWhileRunning() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_NAME, "from-the-embedder");
    final ArcadeDBServer server = new ArcadeDBServer(cfg);

    assertThat(server.getOperatorSettingSource(GlobalConfiguration.SERVER_NAME.getKey())).isEqualTo("file");
    // keys the server adds itself are not recorded
    assertThat(server.getOperatorSettingSource(GlobalConfiguration.SERVER_PLUGINS.getKey())).isNull();
    assertThat(server.getOperatorSettingSource(GlobalConfiguration.SERVER_ROOT_PATH.getKey())).isNull();

    server.markOperatorSetting(GlobalConfiguration.HA_CLUSTER_NAME.getKey());
    assertThat(server.getOperatorSettingSource(GlobalConfiguration.HA_CLUSTER_NAME.getKey())).isEqualTo("runtime");
  }
  /**
   * Issue #9836: storing a declared server setting at runtime and recording that an operator stored it are ONE operation
   * ({@link ArcadeDBServer#storeOperatorSetting}), so a surface that stores through it cannot leave a customised server
   * looking vanilla. A setting whose default is a template is dropped from the bundle altogether without the mark.
   */
  @Test
  void aSettingStoredAtRuntimeThroughTheServerIsNonDefaultEvenWhenItsDefaultIsATemplate() {
    final ArcadeDBServer server = new ArcadeDBServer(new ContextConfiguration());
    final String backup = GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getKey();
    final String name = GlobalConfiguration.SERVER_NAME.getKey();
    // a system property would make the source "jvm": cleared for the test, given back after it
    final String previousBackup = System.getProperty(backup);
    System.clearProperty(backup);
    try {
      server.storeOperatorSetting(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, "/data/backups-9836");
      server.storeOperatorSetting(GlobalConfiguration.SERVER_NAME, "node-9836");

      final Map<String, String> noEnvironment = Map.of();
      final JSONObject json = SupportDiagnostics.buildConfiguration(server.getConfiguration(), server::getOperatorSettingSource,
          noEnvironment::get, new SupportRedactor.Session());

      assertThat(entry(json.getJSONArray("nonDefault"), backup).getString("source")).isEqualTo("runtime");
      assertThat(entry(json.getJSONArray("nonDefault"), name).getString("source")).isEqualTo("runtime");
      assertThat(has(json.getJSONArray("computed"), backup)).isFalse();
      assertThat(has(json.getJSONArray("computed"), name)).isFalse();
    } finally {
      if (previousBackup != null)
        System.setProperty(backup, previousBackup);
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // Regression: #10738
  // Issue #10738: a setting the operator supplied under the key it was renamed from (issue #10599) was applied by the engine and
  // reported in the support bundle as a value the server computed, because the bundle asked for the current key only. Whether a
  // system property or an environment variable supplied a setting is now asked of the setting itself
  // ({@link GlobalConfiguration#processSuppliedValue}), which reads the keys exactly as the engine applies them.
  // <p>
  // The settings are enumerated from the former keys the engine declares, not named here: a second rename is covered on the day it
  // is added.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)
  // ------------------------------------------------------------------------------------------------------------

  /**
   * A read of a setting's system property or environment variable by the bundle itself, which would bypass the rules of
   * {@link GlobalConfiguration#processSuppliedValue}. The operator-source map is keyed by the current spelling already (the
   * configuration file and the runtime routes normalise the key), so its lookup is not one.
   */
  private static final Pattern PRIVATE_KEY_LOOKUP = Pattern.compile(
      "(?:System\\s*\\.\\s*getProperty|System\\s*\\.\\s*getenv|(?:environment|systemProperties)\\s*\\.\\s*apply)\\s*\\(\\s*cfg\\b");

  // Regression: #10738 a setting supplied under its former key was reported as computed
  @Test
  void everyRenamedSettingIsClassifiedAlikeUnderItsCurrentAndItsFormerKey() {
    final List<GlobalConfiguration> renamed = renamedSettings();
    assertThat(renamed).as("the engine declares at least one renamed setting, or this test checks nothing").isNotEmpty();

    for (final GlobalConfiguration setting : renamed) {
      final Object value = aValueOtherThanTheDefault(setting);
      final String text = String.valueOf(value);
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(setting, value);

      for (final boolean environment : new boolean[] { false, true }) {
        final String expectedSource = environment ? "env" : "jvm";
        final JSONObject underCurrent = build(configuration, setting.getKey(), text, environment);
        final JSONObject underFormer = build(configuration, setting.getFormerKey(), text, environment);

        final JSONObject currentEntry = entryOrNull(underCurrent.getJSONArray("nonDefault"), setting.getKey());
        assertThat(currentEntry).as("%s given as %s under its current key", setting.getKey(), expectedSource).isNotNull();
        assertThat(currentEntry.getString("source")).isEqualTo(expectedSource);

        final JSONObject formerEntry = entryOrNull(underFormer.getJSONArray("nonDefault"), setting.getKey());
        assertThat(formerEntry).as("%s given as %s under its former key '%s': the operator supplied it, the server did not compute it",
            setting.getKey(), expectedSource, setting.getFormerKey()).isNotNull();
        assertThat(formerEntry.toString()).as("same entry, same source, whichever accepted spelling carried the value")
            .isEqualTo(currentEntry.toString());
        assertThat(entryOrNull(underFormer.getJSONArray("computed"), setting.getKey())).isNull();
      }
    }
  }

  // Regression: #10738 a setting supplied under its former key was reported as computed
  @Test
  void theCurrentKeyWinsOverTheFormerOneAsTheEngineAppliesThem() {
    for (final GlobalConfiguration setting : renamedSettings()) {
      final GlobalConfiguration.ProcessSuppliedValue supplied = setting.processSuppliedValue(
          Map.of(setting.getFormerKey(), "from-the-former-key")::get, Map.of(setting.getKey(), "from-the-current-key")::get);
      assertThat(supplied.key()).isEqualTo(setting.getKey());
      assertThat(supplied.value()).isEqualTo("from-the-current-key");
      assertThat(supplied.fromEnvironment()).isTrue();

      assertThat(setting.processSuppliedValue(Map.<String, String>of()::get, Map.<String, String>of()::get)).isNull();
    }
  }

  // Regression: #10738 a setting supplied under its former key was reported as computed
  /**
   * The bundle used to also take {@code ARCADEDB_SERVER_HTTPIOTHREADS} as the environment variable of a setting, a spelling the
   * engine does not read: a variable that had no effect was reported as the source of a value.
   */
  @Test
  void anEnvironmentVariableTheEngineDoesNotReadIsNotReportedAsTheSource() {
    final GlobalConfiguration setting = GlobalConfiguration.QUERY_MAX_RANGE_SIZE;
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(setting, 12_345L);
    final String underscored = setting.getKey().replace('.', '_').toUpperCase(Locale.ROOT);

    final JSONObject json = SupportDiagnostics.buildConfiguration(configuration, Map.<String, String>of()::get,
        Map.<String, String>of()::get, Map.of(underscored, "12345")::get, new SupportRedactor.Session());

    assertThat(entryOrNull(json.getJSONArray("nonDefault"), setting.getKey())).isNull();
    assertThat(entryOrNull(json.getJSONArray("computed"), setting.getKey())).isNotNull();
  }

  // Regression: #10738 a setting supplied under its former key was reported as computed
  /** The second half of the guarantee: the bundle asks the setting, and keeps no lookup of its own that a rename can leave behind. */
  @Test
  void theBundleReadsNoSystemPropertyOrEnvironmentVariableOfASettingByItself() throws IOException {
    final Path source = Path.of(System.getProperty("basedir", "."), "src/main/java/com/arcadedb/server/support/SupportDiagnostics.java");
    assertThat(source).as("the source, seen from the server module surefire runs in").isRegularFile();

    final List<String> offenders = new ArrayList<>();
    final String[] lines = Files.readString(source, StandardCharsets.UTF_8).split("\n");
    for (int i = 0; i < lines.length; i++)
      if (PRIVATE_KEY_LOOKUP.matcher(lines[i]).find())
        offenders.add((i + 1) + ": " + lines[i].trim());

    assertThat(offenders).as("ask GlobalConfiguration.processSuppliedValue(), which reads a renamed setting's former key too").isEmpty();
  }

  private static List<GlobalConfiguration> renamedSettings() {
    final List<GlobalConfiguration> renamed = new ArrayList<>();
    for (final GlobalConfiguration setting : GlobalConfiguration.values())
      if (setting.getFormerKey() != null)
        renamed.add(setting);
    return renamed;
  }

  private static JSONObject build(final ContextConfiguration configuration, final String key, final String value,
      final boolean environment) {
    final Map<String, String> supplied = Map.of(key, value);
    final Map<String, String> none = Map.of();
    return SupportDiagnostics.buildConfiguration(configuration, none::get, environment ? none::get : supplied::get,
        environment ? supplied::get : none::get, new SupportRedactor.Session());
  }

  private static Object aValueOtherThanTheDefault(final GlobalConfiguration setting) {
    final Object def = setting.getDefValue();
    final Class<?> type = setting.getType();
    if (type == Boolean.class)
      return !Boolean.TRUE.equals(def);
    if (type == Integer.class)
      return def instanceof Integer i ? i + 1 : 1;
    if (type == Long.class)
      return def instanceof Long l ? l + 1 : 1L;
    return "issue10738-" + def;
  }

  private static JSONObject entryOrNull(final JSONArray array, final String key) {
    for (int i = 0; i < array.length(); i++)
      if (key.equals(array.getJSONObject(i).getString("key")))
        return array.getJSONObject(i);
    return null;
  }
}
