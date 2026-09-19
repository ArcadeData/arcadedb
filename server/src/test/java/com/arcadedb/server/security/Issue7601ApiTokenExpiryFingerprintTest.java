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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7601: API-token expiry used to be a node-LOCAL mutation of a REPLICATED document, which is the one
 * thing the compare-and-set of issue #7509 cannot survive.
 * <p>
 * The precondition a token entry carries is a fingerprint of the document its submitter holds, and since issue
 * #7693 every node judges it against the fingerprint of the last token document the CLUSTER installed. Pruning
 * an expired entry locally moved the first value without moving the second: it happened on whichever node a
 * client last presented that token to, and at whatever moment each node happened to restart, with no entry
 * applied and nothing logged. From then on that node's every token change carried a precondition no node could
 * match, so it was refused everywhere - permanently, across restarts, and while the same operations kept working
 * from its peers.
 * <p>
 * The fix takes expiry out of the document's mutation set entirely: an expired token is refused at every read
 * and removed only by the replicated write that the next token change already performs. So the two halves pinned
 * here are that nothing local moves the fingerprint, and that the removal still happens - through Raft, on every
 * node at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7601ApiTokenExpiryFingerprintTest {

  private static final String ROOT_PATH   = "target/test-security-7601";
  private static final String CONFIG_PATH = "target/test-api-tokens-7601";

  private ApiTokenConfiguration config;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
    FileUtils.deleteRecursively(new File(ROOT_PATH));
    assertThat(new File(CONFIG_PATH).mkdirs()).isTrue();
    config = new ApiTokenConfiguration(CONFIG_PATH);
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
    FileUtils.deleteRecursively(new File(ROOT_PATH));
  }

  /**
   * The repro, at the level the divergence actually happens: presenting an expired token to a node must not
   * change what that node would submit as a compare-and-set precondition.
   */
  @Test
  void presentingAnExpiredTokenDoesNotMoveTheFingerprint() {
    config.createToken("live", "mydb", 0, new JSONObject());
    final String expired = expireInPlace(config, "expired");

    final String before = SecurityDocumentFingerprint.of(config.toJsonPayload());

    assertThat(config.getToken(expired)).as("the expired token authenticates nobody").isNull();

    assertThat(SecurityDocumentFingerprint.of(config.toJsonPayload()))
        .as("and refusing it changed nothing about the replicated document")
        .isEqualTo(before);
  }

  /**
   * The second, independent trigger the issue names: the load-time prune ran at whatever moment each node
   * happened to restart, so a rolling restart alone diverged the fingerprints with no token ever presented.
   */
  @Test
  void aRestartDoesNotMoveTheFingerprintEither() {
    config.createToken("live", "mydb", 0, new JSONObject());
    expireInPlace(config, "expired");
    final String before = SecurityDocumentFingerprint.of(config.toJsonPayload());

    final ApiTokenConfiguration restarted = new ApiTokenConfiguration(CONFIG_PATH);
    restarted.load();

    assertThat(SecurityDocumentFingerprint.of(restarted.toJsonPayload()))
        .as("a node that restarted after the expiry still holds the document the cluster installed")
        .isEqualTo(before);
    assertThat(restarted.listTokens()).as("the expired entry is kept, not pruned").hasSize(2);
  }

  /**
   * The removal has not been abandoned, only moved onto the replicated path. A mint carries the pruned document
   * as its payload and the UNPRUNED one as its precondition - the latter because that is what every node holds
   * and therefore the only thing a precondition may describe.
   */
  @Test
  void aMintRetiresExpiredEntriesInThePayloadAndPinsTheUnprunedDocument() {
    config.createToken("live", "mydb", 0, new JSONObject());
    expireInPlace(config, "expired");

    final ApiTokenConfiguration.MintedToken minted = config.mintToken("fresh", "mydb", 0, new JSONObject());

    assertThat(minted.documentBeforeJson())
        .as("the precondition is the document in force, expired entry and all")
        .isEqualTo(config.toJsonPayload())
        .contains("\"name\":\"expired\"");
    assertThat(minted.documentJson())
        .as("and the payload every node installs has retired it")
        .doesNotContain("\"name\":\"expired\"")
        .contains("\"name\":\"live\"")
        .contains("\"name\":\"fresh\"");
  }

  /** The revocation half, which shares the rule. */
  @Test
  void aRevocationRetiresExpiredEntriesToo() {
    final JSONObject doomed = config.createToken("doomed", "mydb", 0, new JSONObject());
    config.createToken("live", "mydb", 0, new JSONObject());
    expireInPlace(config, "expired");

    final ApiTokenConfiguration.DocumentChange revocation = config.documentWithout(doomed.getString("tokenHash"));

    assertThat(revocation.before()).isEqualTo(config.toJsonPayload()).contains("\"name\":\"expired\"");
    assertThat(revocation.after())
        .doesNotContain("\"name\":\"doomed\"")
        .doesNotContain("\"name\":\"expired\"")
        .contains("\"name\":\"live\"");
  }

  /** And on a standalone server, where there is no Raft, the local write path retires them just the same. */
  @Test
  void aLocalTokenChangeRetiresExpiredEntries() {
    config.createToken("live", "mydb", 0, new JSONObject());
    expireInPlace(config, "expired");
    assertThat(config.listTokens()).hasSize(2);

    config.createToken("fresh", "mydb", 0, new JSONObject());

    assertThat(config.listTokens()).extracting(t -> t.getString("name"))
        .containsExactlyInAnyOrder("live", "fresh");

    final ApiTokenConfiguration reloaded = new ApiTokenConfiguration(CONFIG_PATH);
    reloaded.load();
    assertThat(reloaded.listTokens()).as("and the file was rewritten without it").hasSize(2);
  }

  /**
   * An expired entry lingers until the next token change retires it, so it must not reserve its own name. An
   * operator replacing a token that has just expired is the most likely next caller of all.
   */
  @Test
  void anExpiredTokenDoesNotReserveItsName() {
    expireInPlace(config, "rotating");

    final JSONObject replacement = config.createToken("rotating", "mydb", 0, new JSONObject());

    assertThat(replacement.getString("name")).isEqualTo("rotating");
    assertThat(config.listTokens()).as("the expired one went with the change that replaced it").hasSize(1);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .as("a LIVE token still reserves its name")
        .isThrownBy(() -> config.createToken("rotating", "mydb", 0, new JSONObject()));
  }

  /**
   * The whole of the reported failure, end to end at the level the refusal is decided: a node that has installed
   * the cluster's token document, then had an expired token presented to it, must still get its own next token
   * change accepted.
   * <p>
   * Before the fix the precondition it submitted was a fingerprint of its locally pruned document, which matched
   * the recorded one on no node at all - so {@code isSuperseded} was true everywhere and the change was refused
   * everywhere, with the retry loop rebuilding from the same pruned document and failing identically.
   */
  @Test
  void aNodeThatRefusedAnExpiredTokenCanStillAdministerTokens() {
    final ServerSecurity node = open();
    try {
      // The cluster's token document lands, which is what gives this node a recorded fingerprint to judge by.
      final ApiTokenConfiguration tokens = node.getApiTokenConfiguration();
      tokens.createToken("live", "mydb", 0, new JSONObject());
      final String expired = expireInPlace(tokens, "expired");
      node.applyReplicatedApiTokens(node.getApiTokensJsonPayload(), null);

      final String recorded = node.apiTokensFingerprint();

      // Ordinary traffic: a client presents the token after it expired.
      assertThatThrownBy(() -> node.authenticateByApiToken(expired))
          .as("an expired token authenticates nobody")
          .isInstanceOf(ServerSecurityException.class);

      assertThat(node.apiTokensFingerprint())
          .as("what this node would submit as a precondition has not moved")
          .isEqualTo(recorded);

      // The next administrative change, judged the way every node judges it.
      final ApiTokenConfiguration.MintedToken minted = tokens.mintToken("fresh", "mydb", 0, new JSONObject());
      assertThat(node.applyReplicatedApiTokens(minted.documentJson(),
          SecurityDocumentFingerprint.of(minted.documentBeforeJson())))
          .as("token administration still works on the node the expired token was presented to")
          .isTrue();
    } finally {
      node.stopService();
    }
  }

  /**
   * The listing says an expired token is expired (review of PR #7941).
   * <p>
   * Since expiry no longer removes anything, an entry whose expiry has passed can sit in the document
   * indefinitely on a cluster that mints and revokes nothing - so a listing carrying only a past
   * {@code expiresAt} would put a dead token in the same shape as a live one. Derived at read time, and never
   * written into the replicated document, which is the whole point of the fix above.
   */
  @Test
  void theTokenListingMarksAnExpiredTokenAsExpired() {
    final FixtureServer server = openServer();
    final ServerSecurity node = server.getSecurity();
    try {
      final ApiTokenConfiguration tokens = node.getApiTokenConfiguration();
      tokens.createToken("live", "mydb", 0, new JSONObject());
      expireInPlace(tokens, "expired");

      final JSONArray listing = new ServerControlPlane(server).listApiTokens();

      boolean sawLive = false;
      boolean sawExpired = false;
      for (int i = 0; i < listing.length(); i++) {
        final JSONObject entry = listing.getJSONObject(i);
        if ("live".equals(entry.getString("name"))) {
          assertThat(entry.getBoolean("expired")).as("a token with no expiry is not expired").isFalse();
          sawLive = true;
        } else if ("expired".equals(entry.getString("name"))) {
          assertThat(entry.getBoolean("expired")).as("and one whose expiry has passed says so").isTrue();
          sawExpired = true;
        }
      }
      assertThat(sawLive && sawExpired).as("both tokens are listed; neither was pruned away").isTrue();

      assertThat(node.getApiTokensJsonPayload())
          .as("and nothing derived reached the replicated document, which only Raft may change")
          .doesNotContain("\"expired\":");
    } finally {
      node.stopService();
    }
  }

  /**
   * Installs {@code name}'s entry with an expiry already in the past, through {@link ApiTokenConfiguration#applyReplicated}
   * - the one writer that takes a document verbatim.
   * <p>
   * Minting it expired would not build the fixture: every local write path retires expired entries as it goes
   * (that is the fix), so the NEXT {@code createToken} would remove the one this test needs to still be there.
   * A replicated apply is also what actually puts an about-to-expire token on a node in production.
   *
   * @return the plaintext of the now-expired token
   */
  private static String expireInPlace(final ApiTokenConfiguration config, final String name) {
    final String plaintext = config.createToken(name, "mydb", System.currentTimeMillis() + 600_000,
        new JSONObject()).getString("token");

    final JSONObject document = new JSONObject(config.toJsonPayload());
    final JSONArray tokens = document.getJSONArray("tokens");
    for (int i = 0; i < tokens.length(); i++) {
      final JSONObject token = tokens.getJSONObject(i);
      if (name.equals(token.getString("name", "")))
        token.put("expiresAt", System.currentTimeMillis() - 10_000);
    }
    assertThat(config.applyReplicated(document.toString())).isNull();

    return plaintext;
  }

  private static ServerSecurity open() {
    return openServer().getSecurity();
  }

  private static FixtureServer openServer() {
    final String configPath = ROOT_PATH + "/config";
    assertThat(new File(configPath).mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, ROOT_PATH);

    final FixtureServer server = new FixtureServer(configuration);
    final ServerSecurity security = new ServerSecurity(server, configuration, configPath);
    server.setSecurity(security);
    return server;
  }

  private static final class FixtureServer extends ArcadeDBServer {
    private ServerSecurity security;

    private FixtureServer(final ContextConfiguration configuration) {
      super(configuration);
    }

    private void setSecurity(final ServerSecurity security) {
      this.security = security;
    }

    @Override
    public ServerSecurity getSecurity() {
      return security;
    }
  }
}
