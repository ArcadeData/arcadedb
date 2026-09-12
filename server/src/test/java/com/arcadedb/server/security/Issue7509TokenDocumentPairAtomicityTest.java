/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7509: a cluster-wide token mint or revocation submits a document AND the compare-and-set precondition
 * of the document it was derived from, and the two have to leave {@link ApiTokenConfiguration}'s monitor
 * together.
 * <p>
 * Reading them separately - mint under the monitor, then fingerprint the live store outside it - lets a
 * replicated apply land in between, and the submission then carries a payload built from the OLD token set under
 * a precondition describing the NEW one. The compare-and-set passes and the stale payload installs, putting back
 * tokens the winning entry revoked, which is worse than the lost update it was meant to prevent.
 */
class Issue7509TokenDocumentPairAtomicityTest {

  private static final String CONFIG_PATH = "target/test-api-tokens-7509";

  private ApiTokenConfiguration config;

  @BeforeEach
  void setUp() {
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();
    config = new ApiTokenConfiguration(CONFIG_PATH);
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  @Test
  void aMintCarriesTheDocumentItWasDerivedFrom() {
    config.createToken("existing", "mydb", 0, new JSONObject());
    final String before = config.toJsonPayload();

    final ApiTokenConfiguration.MintedToken minted = config.mintToken("new", "mydb", 0, new JSONObject());

    assertThat(minted.documentBeforeJson())
        .as("the precondition half is the store as it was, not a second read of it").isEqualTo(before);
    assertThat(minted.documentJson())
        .as("and the payload half carries the new token").contains("\"name\":\"new\"");
    assertThat(config.toJsonPayload())
        .as("mintToken installs nothing locally; only the replicated apply does").isEqualTo(before);
  }

  @Test
  void aRevocationCarriesTheDocumentItWasRemovedFrom() {
    final JSONObject doomed = config.createToken("doomed", "mydb", 0, new JSONObject());
    config.createToken("kept", "mydb", 0, new JSONObject());
    final String before = config.toJsonPayload();

    final ApiTokenConfiguration.DocumentChange revocation = config.documentWithout(doomed.getString("tokenHash"));

    assertThat(revocation.before()).isEqualTo(before);
    assertThat(revocation.after())
        .as("the payload drops the revoked token and keeps the other")
        .doesNotContain(doomed.getString("tokenHash")).contains("\"name\":\"kept\"");
    assertThat(config.toJsonPayload()).as("nothing local is mutated").isEqualTo(before);
  }

  @Test
  void revokingAnUnknownTokenReportsNoChangeAtAll() {
    assertThat(config.documentWithout("no-such-hash")).isNull();
  }
}
