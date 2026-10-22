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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10599: {@code SERVER_HTTP_IO_THREADS} was declared with the key {@code arcadedb.server.httpsIoThreads}, so the natural
 * spelling {@code -Darcadedb.server.httpIoThreads} was silently ignored and an operator reading a settings dump took the setting
 * for an HTTPS-only one. A setting's key has to name the setting, because the key is the only handle a user has: the key is now
 * {@code arcadedb.server.httpIoThreads}, the old spelling is still read (and reported once) so a deployment that found it does not
 * lose its value, and a census over every setting keeps a key that names something else from arriving unnoticed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10599ConfigurationKeyNamesTest {
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
