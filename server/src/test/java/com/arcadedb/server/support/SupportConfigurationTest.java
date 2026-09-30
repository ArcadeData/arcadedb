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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SupportConfigurationTest {
  private static final String KEY = "wsk_abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG";

  @TempDir
  Path dir;

  private SupportConfiguration store(final ContextConfiguration configuration) {
    return new SupportConfiguration(dir, configuration);
  }

  @Test
  void notRegisteredByDefault() {
    final SupportConfiguration store = store(new ContextConfiguration());
    assertThat(store.get()).isNull();
    assertThat(store.getPortalUrl()).isEqualTo("https://portal.arcadedb.com");
    assertThat(store.canWriteConfig()).isTrue();
  }

  @Test
  void saveAndReadBack() throws Exception {
    final SupportConfiguration store = store(new ContextConfiguration());
    store.save(null, "ws-123", KEY);

    final SupportConfiguration.Registration registration = store.get();
    assertThat(registration.getClientId()).isEqualTo("ws-123");
    assertThat(registration.getPortalUrl()).isEqualTo("https://portal.arcadedb.com");
    assertThat(registration.getKeyHint()).isEqualTo("…DEFG");
    assertThat(registration.isFromSettings()).isFalse();
    assertThat(registration.getRegisteredAt()).isNotEmpty();
    assertThat(registration.getKey()).isEqualTo(KEY);

    final JSONObject file = new JSONObject(Files.readString(dir.resolve("support.json")));
    assertThat(file.getString("portalUrl")).isEqualTo("https://portal.arcadedb.com");
    assertThat(file.getString("clientId")).isEqualTo("ws-123");
    assertThat(file.getString("key")).isEqualTo(KEY);
    assertThat(file.has("registeredAt")).isTrue();
  }

  @Test
  void fileHasOwnerOnlyPermissionsAndNoTemporaryFilesRemain() throws Exception {
    store(new ContextConfiguration()).save(null, "ws-123", KEY);
    final Path file = dir.resolve("support.json");
    if (Files.getFileStore(file).supportsFileAttributeView("posix"))
      assertThat(Files.getPosixFilePermissions(file)).containsExactlyInAnyOrder(PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE);
    try (final var files = Files.list(dir)) {
      assertThat(files.map(p -> p.getFileName().toString())).containsExactly("support.json");
    }
  }

  @Test
  void replacingTheRegistrationIsAtomicAndLastOneWins() throws Exception {
    final SupportConfiguration store = store(new ContextConfiguration());
    store.save(null, "ws-1", KEY);
    store.save(null, "ws-2", KEY.replace("ABCDEFG", "HIJKLMN"));
    assertThat(store.get().getClientId()).isEqualTo("ws-2");
    assertThat(store.get().getKeyHint()).isEqualTo("…KLMN");
  }

  @Test
  void settingsOverrideTheFile() throws Exception {
    store(new ContextConfiguration()).save(null, "ws-file", KEY);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "ws-setting");
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, "wsk_fromsettings0000000000000000000000000000ZZZZ");
    configuration.setValue(GlobalConfiguration.SUPPORT_URL, "https://portal.example.com/");

    final SupportConfiguration.Registration registration = store(configuration).get();
    assertThat(registration.getClientId()).isEqualTo("ws-setting");
    assertThat(registration.getKeyHint()).isEqualTo("…ZZZZ");
    assertThat(registration.getPortalUrl()).isEqualTo("https://portal.example.com");
    assertThat(registration.isFromSettings()).isTrue();
  }

  @Test
  void anExplicitSettingEqualToTheDefaultStillOverridesTheFile() throws Exception {
    store(new ContextConfiguration()).save("https://portal.test.example.com", "ws-file", KEY);

    // Not set: the file decides
    assertThat(store(new ContextConfiguration()).get().getPortalUrl()).isEqualTo("https://portal.test.example.com");

    // Explicitly set to the default portal: the operator's choice wins over a URL registered earlier in the file
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SUPPORT_URL, "https://portal.arcadedb.com");
    assertThat(store(configuration).get().getPortalUrl()).isEqualTo("https://portal.arcadedb.com");
  }

  /** The whole precedence of the portal URL: an explicit setting, else the file, else the default. */
  @Test
  void thePortalUrlPrecedenceTable() throws Exception {
    final String file = "https://portal.file.example.com";
    final String setting = "https://portal.setting.example.com";
    final String dflt = "https://portal.arcadedb.com";
    // { URL in support.json (null = no file), the setting (null = not set), expected URL }
    final String[][] table = { { null, null, dflt }, { file, null, file }, { null, setting, setting }, { file, setting, setting },
        { null, dflt, dflt }, { file, dflt, dflt }, { file, "", file } };
    for (final String[] row : table) {
      Files.deleteIfExists(dir.resolve("support.json"));
      final ContextConfiguration configuration = new ContextConfiguration();
      if (row[0] != null)
        store(configuration).save(row[0], "ws-1", KEY);
      if (row[1] != null)
        configuration.setValue(GlobalConfiguration.SUPPORT_URL, row[1]);
      final SupportConfiguration store = store(configuration);
      assertThat(store.getPortalUrl()).as("file=%s setting=%s", row[0], row[1]).isEqualTo(row[2]);
      if (row[0] != null)
        assertThat(store.get().getPortalUrl()).as("registration: file=%s setting=%s", row[0], row[1]).isEqualTo(row[2]);
    }
  }

  @Test
  void settingsAloneRegisterWithoutAFile() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "ws-setting");
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, KEY);
    assertThat(store(configuration).get()).isNotNull();
    assertThat(dir.resolve("support.json")).doesNotExist();
  }

  @Test
  void aHalfConfiguredSettingIsNotARegistration() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "ws-setting");
    assertThat(store(configuration).get()).isNull();
  }

  @Test
  void aRegistrationFromSettingsCannotBeCleared() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_ID, "ws-setting");
    configuration.setValue(GlobalConfiguration.SUPPORT_CLIENT_KEY, KEY);
    assertThatThrownBy(() -> store(configuration).clear()).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("arcadedb.support.clientKey").hasMessageNotContaining(KEY);
  }

  @Test
  void clearRemovesTheFile() throws Exception {
    final SupportConfiguration store = store(new ContextConfiguration());
    store.save(null, "ws-1", KEY);
    store.clear();
    assertThat(dir.resolve("support.json")).doesNotExist();
    assertThat(store.get()).isNull();
    store.clear();
  }

  @Test
  void aCorruptFileIsIgnoredAndItsContentIsNotLogged() throws Exception {
    Files.writeString(dir.resolve("support.json"), "{ \"key\": \"" + KEY + "\" oops");
    assertThat(store(new ContextConfiguration()).get()).isNull();
  }

  @Test
  void httpsIsEnforcedExceptForLocalhost() {
    SupportConfiguration.validatePortalUrl("https://portal.arcadedb.com");
    SupportConfiguration.validatePortalUrl("http://localhost:8080");
    SupportConfiguration.validatePortalUrl("http://127.0.0.1:1234/x");
    assertThatThrownBy(() -> SupportConfiguration.validatePortalUrl("http://portal.arcadedb.com")).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("HTTPS");
    assertThatThrownBy(() -> SupportConfiguration.validatePortalUrl("http://localhost.evil.com")).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportConfiguration.validatePortalUrl("ftp://portal.arcadedb.com")).isInstanceOf(
        IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportConfiguration.validatePortalUrl("https://user:pw@portal.arcadedb.com")).isInstanceOf(
        IllegalArgumentException.class).hasMessageNotContaining("pw");
    assertThatThrownBy(() -> SupportConfiguration.validatePortalUrl("not a url")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> store(new ContextConfiguration()).save("http://evil.example.com", "ws", KEY)).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void inputsAreValidatedWithoutEchoingTheKey() {
    assertThatThrownBy(() -> SupportConfiguration.validateClientId(" ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportConfiguration.validateClientId("a b")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportConfiguration.validateClientId("ws\r\nX-Evil: 1")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SupportConfiguration.validateKey("short")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("short");
    assertThatThrownBy(() -> SupportConfiguration.validateKey("wsk_with space in the middle")).isInstanceOf(
        IllegalArgumentException.class).hasMessageNotContaining("wsk_with");
    // surrounding whitespace (a pasted key) is accepted and trimmed
    SupportConfiguration.validateKey(KEY + "\n");
  }

  @Test
  void theKeyNeverLeaksThroughToString() throws Exception {
    final SupportConfiguration store = store(new ContextConfiguration());
    store.save(null, "ws-1", KEY);
    assertThat(store.get().toString()).doesNotContain(KEY).doesNotContain("wsk_").contains("…DEFG");
  }

  @Test
  void theKeyNeverLeaksIntoTheLog() throws Exception {
    final StringBuilder logged = new StringBuilder();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        capture(iMessage, iException, arg1, arg2, arg3, arg4, arg5, arg6);
      }

      @Override
      public void log(final Object iRequester, final Level iLevel, final String iMessage, final Throwable iException, final String context,
          final Object... args) {
        capture(iMessage, iException, args);
      }

      private void capture(final String message, final Throwable exception, final Object... args) {
        logged.append(message);
        for (final Object a : args)
          logged.append(' ').append(a);
        if (exception != null)
          logged.append(' ').append(exception);
      }

      @Override
      public void flush() {
      }
    });
    try {
      final SupportConfiguration store = store(new ContextConfiguration());
      store.save(null, "ws-1", KEY);
      store.clear();
      Files.writeString(dir.resolve("support.json"), "corrupt " + KEY);
      store.get();
    } finally {
      LogManager.instance().setLogger(previous);
    }
    assertThat(logged.toString()).contains("Support registration saved").doesNotContain(KEY).doesNotContain("wsk_");
  }

  @Test
  void theKeySettingIsMaskedWhereverSettingsAreListedOrDumped() {
    assertThat(GlobalConfiguration.SUPPORT_CLIENT_KEY.isHidden()).isTrue();
    assertThat(GlobalConfiguration.SUPPORT_CLIENT_KEY.publishableValue(KEY)).isEqualTo("*****");
    assertThat(GlobalConfiguration.SUPPORT_CLIENT_ID.isHidden()).isFalse();
    assertThat(GlobalConfiguration.SUPPORT_URL.isHidden()).isFalse();
    assertThat(GlobalConfiguration.SUPPORT_URL.getDefValue()).isEqualTo("https://portal.arcadedb.com");

    final String previous = GlobalConfiguration.SUPPORT_CLIENT_KEY.getValueAsString();
    GlobalConfiguration.SUPPORT_CLIENT_KEY.setValue(KEY);
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (final PrintStream stream = new PrintStream(out)) {
        GlobalConfiguration.dumpConfiguration(stream);
      }
      assertThat(out.toString()).doesNotContain(KEY).contains("arcadedb.support.clientKey");
    } finally {
      GlobalConfiguration.SUPPORT_CLIENT_KEY.setValue(previous);
    }
  }

  @Test
  void readOnlyDirectoryIsReported() throws Exception {
    final Path readOnly = Files.createDirectory(dir.resolve("ro"));
    if (!Files.getFileStore(readOnly).supportsFileAttributeView("posix") || System.getProperty("user.name").equals("root"))
      return;
    Files.setPosixFilePermissions(readOnly, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
    try {
      final SupportConfiguration store = new SupportConfiguration(readOnly, new ContextConfiguration());
      assertThat(store.canWriteConfig()).isFalse();
      assertThatThrownBy(() -> store.save(null, "ws", KEY)).isInstanceOf(IOException.class);
    } finally {
      Files.setPosixFilePermissions(readOnly, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE));
    }
  }

  @Test
  void theClientKeySettingIsHiddenInEveryDump() {
    // A later rename of the setting must not silently unmask it in the diagnostics, the settings listing and the logs
    assertThat(GlobalConfiguration.SUPPORT_CLIENT_KEY.isHidden()).isTrue();
  }
}
