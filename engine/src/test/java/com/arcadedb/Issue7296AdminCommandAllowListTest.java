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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7296, the third report in the strict-coercion family (#7222, #7262, this).
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
class Issue7296AdminCommandAllowListTest {

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
