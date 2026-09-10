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

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7297: {@link GlobalConfiguration#findByKey(String)} is case-INSENSITIVE and the
 * {@link ContextConfiguration} overlay is not, so a configuration key written in another case resolved to the
 * right setting, passed the type coercion, the allow-list and the callback, and was then stored under the
 * caller's spelling - where no reader looks, because every reader asks for the setting's declared key.
 * <p>
 * The setting silently kept its default. That is worse than a rejected key: the operator has positive evidence
 * the line was understood - no error, and for a setting with a callback, visible side effects at startup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7297ConfigurationKeyCaseTest {

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
