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

import java.util.HashMap;
import java.util.Map;

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
}
