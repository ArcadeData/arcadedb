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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8573: a configuration parameter PostgreSQL does not know was invented rather than refused: {@code SET} stored
 * it, {@code SHOW} answered it (an empty string when never set) and {@code RESET} accepted it. PostgreSQL answers
 * {@code 42704} to all three, and so does this server now, while a dotted custom placeholder and a name the startup
 * packet named stay usable, as in PostgreSQL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8573UnknownParameterTest {

  @Test
  void unknownNameIsRefusedBySetShowAndReset() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertUndefined(() -> settings.set("bogus_param", "1"), "bogus_param");
    assertUndefined(() -> settings.show("bogus_param"), "bogus_param");
    assertUndefined(() -> settings.show("nosuchparam"), "nosuchparam");
    assertUndefined(() -> settings.apply(new PostgresSessionSettings.Assignment("nosuchparam", null, false)), "nosuchparam");
    assertUndefined(() -> settings.set("bogus_param", "1", true), "bogus_param");
  }

  @Test
  void aRefusedSetChangesNothing() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThatThrownBy(() -> settings.set("bogus_param", "1")).isInstanceOf(PostgresSessionSettings.SettingException.class);
    assertThat(settings.showAll()).extracting(row -> row[0]).doesNotContain("bogus_param");
  }

  @ParameterizedTest
  @ValueSource(strings = { "extra_float_digits", "lock_timeout", "work_mem", "bytea_output",
      "idle_in_transaction_session_timeout", "jit" })
  void everyParameterPostgresqlLetsASessionSetIsAccepted(final String name) {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set(name, "7");
    assertThat(settings.show(name)).isEqualTo("7");
    settings.set(name, null);
    assertThat(settings.show(name)).isEmpty();
  }

  @Test
  void customPlaceholderIsAcceptedAndShownOnceSet() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    // As in PostgreSQL: SHOW of a placeholder nobody defined is an error, RESET of one is not
    assertUndefined(() -> settings.show("myapp.tenant"), "myapp.tenant");
    settings.apply(new PostgresSessionSettings.Assignment("myapp.other", null, false));

    settings.set("myapp.tenant", "acme");
    assertThat(settings.show("myapp.tenant")).isEqualTo("acme");
    assertThat(settings.show("MyApp.Tenant")).isEqualTo("acme");
    // Once defined it stays defined: RESET answers the empty reset value, as PostgreSQL does
    settings.set("myapp.tenant", null);
    assertThat(settings.show("myapp.tenant")).isEmpty();

    // A dot at either end does not qualify a name
    assertUndefined(() -> settings.set(".tenant", "x"), ".tenant");
    assertUndefined(() -> settings.set("tenant.", "x"), "tenant.");
  }

  @Test
  void startupPacketNameStaysUsableOnTheConnection() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.setFromStartup("my_driver_option", "on");
    assertThat(settings.show("my_driver_option")).isEqualTo("on");
    settings.set("my_driver_option", "off");
    assertThat(settings.show("my_driver_option")).isEqualTo("off");
    settings.apply(new PostgresSessionSettings.Assignment("my_driver_option", null, false));
    assertThat(settings.show("my_driver_option")).isEqualTo("on");
  }

  @Test
  void parameterOnlyTheServerConfigurationChangesIsRefusedAsPostgresqlRefusesIt() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertCantChange(() -> settings.set("shared_buffers", "1"), "parameter \"shared_buffers\" cannot be changed without restarting the server");
    assertCantChange(() -> settings.set("archive_command", "x"), "parameter \"archive_command\" cannot be changed now");
    assertCantChange(() -> settings.set("block_size", "1"), "parameter \"block_size\" cannot be changed");
    assertCantChange(() -> settings.set("log_connections", "on"), "parameter \"log_connections\" cannot be set after connection start");
    // ...but a backend parameter is still accepted in the startup packet, as in PostgreSQL
    settings.setFromStartup("log_connections", "on");
    assertThat(settings.show("log_connections")).isEqualTo("on");
  }

  @Test
  void showAnswersUnderPostgresqlsSpellingOfTheName() {
    assertThat(PostgresSessionSettings.canonicalName("datestyle")).isEqualTo("DateStyle");
    assertThat(PostgresSessionSettings.canonicalName("TIMEZONE")).isEqualTo("TimeZone");
    assertThat(PostgresSessionSettings.canonicalName("transaction_isolation")).isEqualTo("transaction_isolation");
    assertThat(PostgresSessionSettings.canonicalName("myapp.tenant")).isEqualTo("myapp.tenant");
  }

  @Test
  void showAllListsWhatTheServerAnswersAndWhatWasSet() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("work_mem", "x");
    settings.set("myapp.tenant", "acme");
    final List<String[]> all = settings.showAll();
    assertThat(all).extracting(row -> row[0]).contains("DateStyle", "TimeZone", "transaction_isolation", "myapp.tenant");
    assertThat(all).filteredOn(row -> row[0].equals("work_mem")).extracting(row -> row[1]).containsExactly("x");
  }

  private static void assertUndefined(final Runnable action, final String name) {
    assertThatThrownBy(action::run).isInstanceOfSatisfying(PostgresSessionSettings.SettingException.class, e -> {
      assertThat(e.sqlState).isEqualTo("42704");
      assertThat(e.getMessage()).isEqualTo("unrecognized configuration parameter \"" + name + "\"");
    });
  }

  private static void assertCantChange(final Runnable action, final String message) {
    assertThatThrownBy(action::run).isInstanceOfSatisfying(PostgresSessionSettings.SettingException.class, e -> {
      assertThat(e.sqlState).isEqualTo("55P02");
      assertThat(e.getMessage()).isEqualTo(message);
    });
  }
}
