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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6474: {@code import database <name> <url>} (the HTTP server command) gated on two
 * independent, independently-configured SSRF flags. {@code PostServerCommandHandler.validateClientRestoreImportUrl}
 * pre-checked the URL against {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS}, read from that
 * server's own configuration, while the actual fetch inside {@link SourceDiscovery} re-derived its own answer from
 * the different, static-only {@link GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS} - so enabling
 * the documented opt-out for one did not affect the other.
 * <p>
 * This mirrors {@code Issue6381RestoreSsrfTest}'s coverage of the analogous restore-side fix (#6381/#6449): it
 * proves an explicit {@link SourceDiscovery#SourceDiscovery(String, Boolean)} / {@link
 * Importer#setAllowLocalUrls(boolean)} override takes priority over the static default in both directions, which is
 * exactly what {@code PostServerCommandHandler} now relies on to thread its pre-check's resolved answer through.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6474ImportSsrfFlagDivergenceTest {

  private static final String LOOPBACK_URL = "http://127.0.0.1:1/unreachable";

  @AfterEach
  void reset() {
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.reset();
  }

  @Test
  void sourceDiscoveryWithNoOverrideFallsBackToStaticDefault() {
    // No explicit override (null, the SourceDiscovery(String) constructor's behaviour): must still honour the
    // static default exactly like before this fix, for every caller that never resolved its own answer.
    assertThatThrownBy(() -> new SourceDiscovery(LOOPBACK_URL).getSource())
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("127.0.0.1");
  }

  @Test
  void explicitOverrideAllowsLocalUrlEvenWhenStaticDefaultBlocks() throws IOException {
    // Static default is true (blocking); the explicit per-call override must still allow the fetch to be attempted.
    // (It still fails - port 1 refuses the connection - but with a connection error, never a SecurityException.)
    assertThatThrownBy(() -> new SourceDiscovery(LOOPBACK_URL, true).getSource())
        .isNotInstanceOf(SecurityException.class);
  }

  @Test
  void explicitOverrideBlocksLocalUrlEvenWhenStaticDefaultAllows() {
    // Static default flipped to allow local URLs; the explicit per-call override must still block it - proving the
    // override is consulted at all, not just used as a permissive fallback.
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);

    assertThatThrownBy(() -> new SourceDiscovery(LOOPBACK_URL, false).getSource())
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("127.0.0.1");
  }

  @Test
  void importerSetAllowLocalUrlsThreadsThroughToSettings() {
    // The exact reflective call PostServerCommandHandler makes on the SSE path. settings is package-visible
    // (protected in AbstractImporter, this test is in the same package), so this checks the override reaches the
    // same field SourceDiscovery(url, settings.allowLocalUrls) reads in Importer.loadFromSource() - without paying
    // for a full load() cycle (database creation, background timer) just to prove a one-field assignment.
    final Importer importer = new Importer(null, LOOPBACK_URL);
    assertThat(importer.settings.allowLocalUrls).isNull();

    importer.setAllowLocalUrls(true);
    assertThat(importer.settings.allowLocalUrls).isTrue();

    importer.setAllowLocalUrls(false);
    assertThat(importer.settings.allowLocalUrls).isFalse();
  }
}
