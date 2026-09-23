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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8217: a {@code SET} is session-scoped. The settings a connection establishes live in that connection's own
 * {@link PostgresSessionSettings}, and {@code SHOW} reads them back before falling back to the server defaults.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresSessionSettingsTest {

  @Test
  void showAnswersWhatSetStored() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThat(settings.show("search_path")).isEmpty();

    settings.set("search_path", "x");
    assertThat(settings.show("search_path")).isEqualTo("x");
  }

  @Test
  void showIsCaseInsensitiveOnTheParameterName() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("Application_Name", "app");
    assertThat(settings.show("application_name")).isEqualTo("app");
    assertThat(settings.show("APPLICATION_NAME")).isEqualTo("app");
  }

  @Test
  void settingsAreNotSharedBetweenInstances() {
    final PostgresSessionSettings first = new PostgresSessionSettings();
    final PostgresSessionSettings second = new PostgresSessionSettings();
    first.set("search_path", "x");
    assertThat(second.show("search_path")).isEmpty();
  }

  @Test
  void setToDefaultRestoresTheDefault() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("timezone", "Europe/Rome");
    assertThat(settings.show("timezone")).isEqualTo("Europe/Rome");
    settings.set("timezone", "DEFAULT");
    assertThat(settings.show("timezone")).isEqualTo("UTC");
  }

  @Test
  void serverDefaultsAreAnsweredWhenNothingWasSet() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThat(settings.show("server_version")).isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);
    assertThat(settings.show("standard_conforming_strings")).isEqualTo("on");
    assertThat(settings.show("integer_datetimes")).isEqualTo("on");
    assertThat(settings.show("client_encoding")).isEqualTo("UTF8");
    assertThat(settings.show("server_encoding")).isEqualTo("UTF8");
    assertThat(settings.show("timezone")).isEqualTo("UTC");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, MDY");
  }

  @Test
  void datestyleIsoKeepsTheFieldOrder() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("datestyle", "ISO");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, MDY");

    settings.set("datestyle", "DMY");
    assertThat(settings.show("datestyle")).as("an order alone keeps the output style").isEqualTo("ISO, DMY");

    settings.set("datestyle", "iso, ymd");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, YMD");

    settings.set("datestyle", "European");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, DMY");
  }

  @Test
  void datestyleNeverAdvertisesAnOutputStyleThisServerDoesNotWrite() {
    // Dates always travel as ISO on this server, so SHOW must not claim otherwise.
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("datestyle", "SQL, DMY");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, DMY");
  }

  @Test
  void datestyleRefusesAnUnknownValue() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThatThrownBy(() -> settings.set("datestyle", "nonsense"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class)
        .hasMessageContaining("DateStyle")
        .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("22023"));
    assertThat(settings.show("datestyle")).as("a refused SET changes nothing").isEqualTo("ISO, MDY");
  }

  @Test
  void readOnlyParametersAreRefused() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    for (final String name : new String[] { "server_version", "server_encoding", "integer_datetimes" }) {
      assertThatThrownBy(() -> settings.set(name, "x"))
          .isInstanceOf(PostgresSessionSettings.SettingException.class)
          .hasMessageContaining("cannot be changed")
          .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("55P02"));
    }
    assertThat(settings.show("server_version")).isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);
  }

  @Test
  void clientEncodingAlwaysAnswersWhatTheServerSends() {
    // Every text value leaves this server in UTF-8 whatever the client asks for, so SHOW answers that.
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("client_encoding", "LATIN1");
    assertThat(settings.show("client_encoding")).isEqualTo("UTF8");
  }

  @Test
  void startupParametersAreRecordedLeniently() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.setFromStartup("DateStyle", "ISO");
    settings.setFromStartup("TimeZone", "Europe/Rome");
    settings.setFromStartup("application_name", "pgjdbc");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, MDY");
    assertThat(settings.show("timezone")).isEqualTo("Europe/Rome");
    assertThat(settings.show("application_name")).isEqualTo("pgjdbc");

    // A startup value this server would refuse on a SET must not abort the connection: it is logged and ignored.
    settings.setFromStartup("DateStyle", "nonsense");
    settings.setFromStartup("server_version", "99");
    assertThat(settings.show("datestyle")).isEqualTo("ISO, MDY");
    assertThat(settings.show("server_version")).isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);
  }
}
