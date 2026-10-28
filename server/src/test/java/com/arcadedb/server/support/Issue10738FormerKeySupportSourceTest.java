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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10738: a setting the operator supplied under the key it was renamed from (issue #10599) was applied by the engine and
 * reported in the support bundle as a value the server computed, because the bundle asked for the current key only. Whether a
 * system property or an environment variable supplied a setting is now asked of the setting itself
 * ({@link GlobalConfiguration#processSuppliedValue}), which reads the keys exactly as the engine applies them.
 * <p>
 * The settings are enumerated from the former keys the engine declares, not named here: a second rename is covered on the day it
 * is added.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10738FormerKeySupportSourceTest {

  /**
   * A read of a setting's system property or environment variable by the bundle itself, which would bypass the rules of
   * {@link GlobalConfiguration#processSuppliedValue}. The operator-source map is keyed by the current spelling already (the
   * configuration file and the runtime routes normalise the key), so its lookup is not one.
   */
  private static final Pattern PRIVATE_KEY_LOOKUP = Pattern.compile(
      "(?:System\\s*\\.\\s*getProperty|System\\s*\\.\\s*getenv|(?:environment|systemProperties)\\s*\\.\\s*apply)\\s*\\(\\s*cfg\\b");

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

        final JSONObject currentEntry = entry(underCurrent.getJSONArray("nonDefault"), setting.getKey());
        assertThat(currentEntry).as("%s given as %s under its current key", setting.getKey(), expectedSource).isNotNull();
        assertThat(currentEntry.getString("source")).isEqualTo(expectedSource);

        final JSONObject formerEntry = entry(underFormer.getJSONArray("nonDefault"), setting.getKey());
        assertThat(formerEntry).as("%s given as %s under its former key '%s': the operator supplied it, the server did not compute it",
            setting.getKey(), expectedSource, setting.getFormerKey()).isNotNull();
        assertThat(formerEntry.toString()).as("same entry, same source, whichever accepted spelling carried the value")
            .isEqualTo(currentEntry.toString());
        assertThat(entry(underFormer.getJSONArray("computed"), setting.getKey())).isNull();
      }
    }
  }

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

    assertThat(entry(json.getJSONArray("nonDefault"), setting.getKey())).isNull();
    assertThat(entry(json.getJSONArray("computed"), setting.getKey())).isNotNull();
  }

  /** The second half of the guarantee: the bundle asks the setting, and keeps no lookup of its own that a rename can leave behind. */
  @Test
  void theBundleReadsNoSystemPropertyOrEnvironmentVariableOfASettingByItself() throws IOException {
    final Path source = Path.of("src/main/java/com/arcadedb/server/support/SupportDiagnostics.java");
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

  private static JSONObject entry(final JSONArray array, final String key) {
    for (int i = 0; i < array.length(); i++)
      if (key.equals(array.getJSONObject(i).getString("key")))
        return array.getJSONObject(i);
    return null;
  }
}
