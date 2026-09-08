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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7262: the server configuration file used to store its values into the {@link ContextConfiguration} overlay
 * with no coercion, and {@link ContextConfiguration#getValueAsBoolean(GlobalConfiguration)} used to read them back
 * with {@code Boolean.parseBoolean}, which maps every string that is not {@code "true"} to {@code false}.
 * <p>
 * Composed, that turned {@code "arcadedb.ha.tls.mutualAuth": "yes"} into a silent shutdown of mutual TLS
 * authentication on the inter-node Raft channel, and {@code "arcadedb.ha.peerAllowlist.enabled": "on"} into a
 * server that never installs the peer allowlist. Both settings default to {@code true}, so the operator who left
 * them alone was protected and the operator who wrote down that they wanted the protection was not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7262ConfigurationFileBooleanStrictTest {

  private static final GlobalConfiguration MUTUAL_AUTH      = GlobalConfiguration.HA_TLS_MUTUAL_AUTH;
  private static final GlobalConfiguration PEER_ALLOWLIST   = GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED;

  @Test
  public void bothSecuritySwitchesDefaultToTrue() {
    // THE WHOLE POINT OF THE ISSUE: THE DEFAULT IS THE PROTECTION, SO A MISREAD CAN ONLY LOSE IT.
    assertThat(MUTUAL_AUTH.getDefValue()).isEqualTo(Boolean.TRUE);
    assertThat(PEER_ALLOWLIST.getDefValue()).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void aSynonymInTheConfigurationFileNoLongerDisablesMutualTLS() {
    for (final String synonym : new String[] { "yes", "on", "Y", "1", "ture", "true!" }) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"" + synonym + "\""));

      // Against the setting's own value, not a hard-coded true: what a refusal keeps is "whatever it would have
      // been without this line", which a -D in the surrounding run is entitled to have chosen.
      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).as("mutual TLS auth with '%s' in the configuration file", synonym)
          .isEqualTo(MUTUAL_AUTH.getValueAsBoolean());
    }
  }

  @Test
  public void surroundingWhitespaceIsAcceptedRatherThanRefused() {
    // Trimmed before the comparison, so this is a VALID true - it is not one of the refused spellings above.
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\" TRUE \""));

    assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).isEqualTo(Boolean.TRUE);
    assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
  }

  @Test
  public void aSynonymInTheConfigurationFileNoLongerSkipsThePeerAllowlist() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(PEER_ALLOWLIST, "\"on\""));

    assertThat(cfg.getValueAsBoolean(PEER_ALLOWLIST)).isEqualTo(PEER_ALLOWLIST.getValueAsBoolean());
  }

  @Test
  public void aRefusedValueIsNotStoredAtAll() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"yes\""));

    // NOT MERELY READ AS THE DEFAULT: THE OVERLAY IS WHAT A SERVER HANDS TO ITS PLUGINS, AND A VALUE NOTHING CAN
    // READ HAS NO BUSINESS BEING IN IT.
    assertThat(cfg.hasValue(MUTUAL_AUTH.getKey())).isFalse();
  }

  @Test
  public void anExplicitFalseIsStillHonoured() {
    for (final String text : new String[] { "false", "False", "FALSE", " false " }) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"" + text + "\""));

      assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).as("explicit '%s'", text).isFalse();
      assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).as("stored as the declared type").isEqualTo(Boolean.FALSE);
    }
  }

  @Test
  public void anExplicitTrueIsStillHonoured() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(MUTUAL_AUTH, "\"true\""));

    assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
    assertThat(cfg.<Object>getValue(MUTUAL_AUTH)).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void aGenuineJSONBooleanStillWorks() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(MUTUAL_AUTH, "false"));

    assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isFalse();
  }

  @Test
  public void theConfigurationFileStoresValuesAsTheirDeclaredType() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON("{\"configuration\":{\"" + shortKey(GlobalConfiguration.COMMIT_LOCK_TIMEOUT) + "\":\"64\"}}");

    assertThat(cfg.<Object>getValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT)).isInstanceOf(Long.class);
    assertThat(cfg.getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT)).isEqualTo(64L);
  }

  @Test
  public void anUnreadableNonBooleanIsRefusedToo() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON("{\"configuration\":{\"" + shortKey(GlobalConfiguration.ASYNC_WORKER_THREADS) + "\":\"abc\"}}");

    assertThat(cfg.hasValue(GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())).isFalse();
    assertThat(cfg.getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(
        GlobalConfiguration.ASYNC_WORKER_THREADS.getValueAsInteger());
  }

  /**
   * The reader is the second half of the mechanism, and it has its own way in: the map constructor stores whatever
   * it is handed without ever passing through {@link ContextConfiguration#fromJSON(String)}.
   */
  @Test
  public void theReaderFallsBackToTheDefaultRatherThanToFalse() {
    final Map<String, Object> raw = new HashMap<>();
    raw.put(MUTUAL_AUTH.getKey(), "yes");
    raw.put(GlobalConfiguration.HA_ENABLED.getKey(), "on");

    final ContextConfiguration cfg = new ContextConfiguration(raw);

    // DEFAULT true: THE REFUSED VALUE MUST NOT TURN THE PROTECTION OFF.
    assertThat(cfg.getValueAsBoolean(MUTUAL_AUTH)).isTrue();
    // DEFAULT false: REFUSING TO GUESS IS THE PROPERTY THAT HOLDS FOR ALL OF THEM, NOT "ALWAYS true".
    assertThat(GlobalConfiguration.HA_ENABLED.getDefValue()).isEqualTo(Boolean.FALSE);
    assertThat(cfg.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isFalse();
  }

  @Test
  public void theReaderStillReadsBooleanTextInEitherCaseAndWithSpaces() {
    final Map<String, Object> raw = new HashMap<>();
    raw.put(GlobalConfiguration.HA_ENABLED.getKey(), " TRUE ");
    assertThat(new ContextConfiguration(raw).getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isTrue();

    raw.put(GlobalConfiguration.HA_ENABLED.getKey(), "False");
    assertThat(new ContextConfiguration(raw).getValueAsBoolean(GlobalConfiguration.HA_ENABLED)).isFalse();
  }

  @Test
  public void anUnknownSettingInTheConfigurationFileIsStillIgnored() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON("{\"configuration\":{\"not.a.real.setting\":\"yes\"}}");

    assertThat(cfg.getContextKeys()).isEmpty();
  }

  /**
   * The allow-list is enforced at this entry point too, and it has to be: {@code coerce} converts to the declared
   * TYPE and stops, so a {@code String} setting with an allow-list used to accept anything that was a string. A
   * server configuration file naming a mode that does not exist was stored verbatim, every reader compared it
   * against {@code "production"}, found it different, and gave a production deployment the development behaviour -
   * Studio included.
   */
  @Test
  public void aValueOutsideTheAllowListIsRefusedByTheConfigurationFile() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(GlobalConfiguration.SERVER_MODE, "\"staging\""));

    assertThat(cfg.hasValue(GlobalConfiguration.SERVER_MODE.getKey())).isFalse();
    assertThat(cfg.getValueAsString(GlobalConfiguration.SERVER_MODE)).isEqualTo(
        GlobalConfiguration.SERVER_MODE.getValueAsString());
  }

  /**
   * The allow-list is matched case-insensitively, so {@code Production} was accepted and then stored with the
   * capital - which no {@code "production".equals(mode)} reader recognises. Normalised to the declared spelling in
   * the one conversion both writers go through, so every reader sees it.
   */
  @Test
  public void anAllowListedValueIsNormalisedToItsDeclaredSpelling() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.fromJSON(configurationFile(GlobalConfiguration.SERVER_MODE, "\"Production\""));

    assertThat(cfg.getValueAsString(GlobalConfiguration.SERVER_MODE)).isEqualTo("production");
    assertThat(GlobalConfiguration.SERVER_MODE.coerceFromAdminCommand("PRODUCTION")).isEqualTo("production");
  }

  /**
   * The property path may not throw: it runs from {@code readConfiguration()} inside {@code GlobalConfiguration}'s
   * static initializer, where an escaping exception becomes an {@code ExceptionInInitializerError} that takes the
   * engine down over one mistyped variable. {@code setValue} refuses an allow-list violation by THROWING, so it
   * has to stay inside the non-throwing envelope.
   */
  @Test
  public void aValueOutsideTheAllowListIsReportedRatherThanThrownOnThePropertyPath() {
    final String before = GlobalConfiguration.SERVER_MODE.getValueAsString();
    try {
      assertThat(GlobalConfiguration.SERVER_MODE.setValueFromConfigurationSource("staging", "system property"))
          .isFalse();

      assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo(before);
      assertThat(GlobalConfiguration.SERVER_MODE.isChanged()).isFalse();
    } finally {
      GlobalConfiguration.SERVER_MODE.reset();
    }
  }

  @Test
  public void anAllowListedValueStillGoesThroughOnThePropertyPath() {
    try {
      assertThat(GlobalConfiguration.SERVER_MODE.setValueFromConfigurationSource("Test", "system property")).isTrue();
      assertThat(GlobalConfiguration.SERVER_MODE.getValueAsString()).isEqualTo("test");
    } finally {
      GlobalConfiguration.SERVER_MODE.reset();
    }
  }

  private static String configurationFile(final GlobalConfiguration setting, final String jsonValue) {
    return "{\"configuration\":{\"" + shortKey(setting) + "\":" + jsonValue + "}}";
  }

  private static String shortKey(final GlobalConfiguration setting) {
    return setting.getKey().substring(GlobalConfiguration.PREFIX.length());
  }
}
