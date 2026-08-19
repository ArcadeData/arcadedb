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
package com.arcadedb.integration.restore.format;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.restore.RestoreSettings;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6381: {@code restore database} opened its HTTP(S) input with plain
 * {@code HttpURLConnection}, which follows same-protocol 3xx redirects itself with no revalidation of the redirect
 * target. An attacker-controlled public host answering {@code 302 Location: http://169.254.169.254/...} walked
 * straight past the one-shot host check {@code PostServerCommandHandler.validateClientRestoreImportUrl} performs
 * before handing the URL to restore - the exact bypass already fixed for {@code IMPORT DATABASE}
 * (GHSA-4w2m-77c8-83mw) but left unfixed here. {@code openInputFile()} now routes through {@link
 * com.arcadedb.utility.SafeHttpFetcher}, the same per-hop-revalidating fetcher {@code ImportSecurityValidator} uses;
 * the redirect-chain behaviour itself (blocked target, blocked scheme, redirect loop, ...) is covered exhaustively by
 * {@code SafeHttpFetcherTest} and is not repeated here. These tests instead prove the restore-specific wiring: the
 * fetch is gated by {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} end to end through {@link
 * FullRestoreFormat#restoreDatabase()}, not only when {@code SafeHttpFetcher} is called directly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6381RestoreSsrfTest {

  private static final String CONTENT = "not-a-real-zip-archive";

  private HttpServer server;
  private String     baseUrl;
  private File       databaseDirectory;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();

    server.createContext("/content", exchange -> {
      final byte[] body = CONTENT.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });

    // A redirect hop, to prove restore follows redirects through the fetcher rather than refusing them outright.
    server.createContext("/redirect-to-content", exchange -> {
      exchange.getResponseHeaders().add("Location", baseUrl + "/content");
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });

    server.start();

    databaseDirectory = new File("./target/databases/Issue6381RestoreSsrfTest");
    FileUtils.deleteRecursively(databaseDirectory);
  }

  @AfterEach
  void stopServer() {
    if (server != null)
      server.stop(0);
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.reset();
    FileUtils.deleteRecursively(databaseDirectory);
  }

  @Test
  void restoreRefusesLocalAddressByDefault() {
    // Loopback is blocked by IPAddressBlocklist like any other private/local target; before the fix restore never
    // consulted the blocklist at fetch time at all (only the server's one-shot pre-check did, and only for the
    // original hostname). No redirect is even needed to prove the wiring reaches the shared blocklist.
    final RestoreSettings settings = new RestoreSettings();
    settings.format = "full";
    settings.inputFileURL = baseUrl + "/content";
    settings.databaseDirectory = databaseDirectory.getPath();

    // database is never reached: openInputFile() must throw before restoreDatabase() touches it.
    final FullRestoreFormat restore = new FullRestoreFormat(null, settings, new ConsoleLogger(0));

    assertThatThrownBy(restore::restoreDatabase)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("127.0.0.1");
  }

  @Test
  void restoreFollowsRedirectAndFetchesContentWhenLocalUrlsExplicitlyAllowed() {
    // The documented opt-out: an operator who explicitly trusts internal sources can restore from one, and a
    // redirect must actually be followed (not silently dropped) to reach the archive content.
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);

    final RestoreSettings settings = new RestoreSettings();
    settings.format = "full";
    settings.inputFileURL = baseUrl + "/redirect-to-content";
    settings.databaseDirectory = databaseDirectory.getPath();

    final FullRestoreFormat restore = new FullRestoreFormat(null, settings, new ConsoleLogger(0));

    // The redirect is followed and CONTENT reaches the archive walk, which fails for an unrelated, non-security
    // reason (it is not a real zip) - never with SecurityException.
    assertThatThrownBy(restore::restoreDatabase).isNotInstanceOf(SecurityException.class);
  }

  /**
   * {@code RestoreSettings.allowLocalUrls} is what {@code PostServerCommandHandler} sets (reflectively, via
   * {@code Restore.setAllowLocalUrls}) from its own per-server {@code ContextConfiguration} - a different
   * configuration source than the static {@link GlobalConfiguration} value. Without this explicit override taking
   * priority, a server whose operator enabled the setting only on that server's own configuration (not the JVM-wide
   * static one) would have its pre-check accept a restore that the fetch then refused anyway.
   */
  @Test
  void explicitSettingsOverrideTakesPriorityOverStaticDefault() {
    // Static default is false (blocked); the explicit per-call override must still allow it.
    final RestoreSettings allowSettings = new RestoreSettings();
    allowSettings.format = "full";
    allowSettings.inputFileURL = baseUrl + "/redirect-to-content";
    allowSettings.databaseDirectory = databaseDirectory.getPath();
    allowSettings.allowLocalUrls = true;

    assertThatThrownBy(new FullRestoreFormat(null, allowSettings, new ConsoleLogger(0))::restoreDatabase)
        .isNotInstanceOf(SecurityException.class);

    // Static default flipped to true; the explicit per-call override must still block it.
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    final RestoreSettings blockSettings = new RestoreSettings();
    blockSettings.format = "full";
    blockSettings.inputFileURL = baseUrl + "/content";
    blockSettings.databaseDirectory = databaseDirectory.getPath();
    blockSettings.allowLocalUrls = false;

    assertThatThrownBy(new FullRestoreFormat(null, blockSettings, new ConsoleLogger(0))::restoreDatabase)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("127.0.0.1");
  }
}
