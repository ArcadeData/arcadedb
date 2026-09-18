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
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7803: what the durable API-token record contains, and when it is written.
 * <p>
 * The property the store exists to hold is that the plaintext token never reaches the disk - the server keeps
 * {@code tokenHash} and {@code tokenSuffix} and nothing else that could be replayed. Until this test the only
 * thing asserting it was {@code ApiTokenAuthenticationIT.plaintextNotPersistedOnDisk}, which drives exactly one
 * of the writers (the non-HA local mint) over HTTP. The three other paths that write the same file - the
 * replicated apply that is the ONLY writer in a cluster since issue #7373, the revocation rewrite, and the
 * legacy-plaintext migration inside {@code load()} whose entire purpose is to get a plaintext token off the
 * disk - had nothing checking what they left behind.
 * <p>
 * The last two tests are the other half, absorbed from issue #7525: {@code getToken} is the API-token
 * authentication path, reached on every request carrying one, and it used to {@code save()} when it evicted an
 * expired token - an fsync and a contended monitor on an Undertow worker thread. It now evicts in memory only,
 * which is safe because an expired entry left in the file cannot authenticate (the expiry is re-checked on
 * every lookup) and {@code load()} prunes it at the next restart.
 */
class Issue7803ApiTokenDurableRecordTest {

  private static final String CONFIG_PATH = "target/test-api-tokens-7803";

  private File                  configDir;
  private File                  tokenFile;
  private ApiTokenConfiguration config;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);

    configDir = new File(CONFIG_PATH);
    if (configDir.exists())
      FileUtils.deleteRecursively(configDir);
    assertThat(configDir.mkdirs()).isTrue();

    tokenFile = new File(configDir, ApiTokenConfiguration.FILE_NAME);
    config = new ApiTokenConfiguration(CONFIG_PATH);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  /**
   * Writer 1 of 4: the local mint, driven through {@link ServerControlPlane} - the convergence point both
   * {@code PostApiTokenHandler} and the gRPC {@code CreateApiToken} call - on a server with no HA, where
   * {@code createApiTokenClusterWide} falls through to {@code ApiTokenConfiguration.createToken}.
   */
  @Test
  void theRecordWrittenByALocalMintHoldsTheHashAndNeverThePlaintext() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");

    final FixtureServer server = new FixtureServer(configuration);
    final ServerSecurity security = new ServerSecurity(server, configuration, CONFIG_PATH);
    server.setSecurity(security);

    try {
      final JSONObject created = new ServerControlPlane(server).createApiToken("ci", "graph", 0, new JSONObject());
      assertThatTheFileHoldsOnlyTheHashOf(created.getString("token"));
    } finally {
      security.stopService();
    }
  }

  /**
   * Writer 2 of 4: the replicated apply. In a cluster this is the only writer - {@code createApiTokenClusterWide}
   * mints the document but mutates nothing, and every node including the one that served the request installs
   * the committed document through {@code applyReplicated}. The pair driven here is exactly the pair that method
   * uses: {@code mintToken} produces the document, {@code applyReplicated} installs and persists it.
   */
  @Test
  void theRecordWrittenByAReplicatedApplyHoldsTheHashAndNeverThePlaintext() throws Exception {
    final ApiTokenConfiguration.MintedToken minted = config.mintToken("ci", "graph", 0, new JSONObject());

    assertThat(minted.documentJson())
        .as("the replicated document travels over the wire and into the Raft log, so it must carry no plaintext either")
        .doesNotContain(minted.response().getString("token"));

    assertThat(config.applyReplicated(minted.documentJson())).isNull();

    assertThatTheFileHoldsOnlyTheHashOf(minted.response().getString("token"));
  }

  /** Writer 3 of 4: a revocation rewrites the whole document, so it is a writer like any other. */
  @Test
  void revokingATokenRewritesTheFileWithNeitherTheTokenNorAnyPlaintext() throws Exception {
    final JSONObject kept = config.createToken("kept", "graph", 0, new JSONObject());
    final JSONObject revoked = config.createToken("revoked", "graph", 0, new JSONObject());

    assertThat(config.deleteToken(ApiTokenConfiguration.hashToken(revoked.getString("token")))).isTrue();

    final String content = Files.readString(tokenFile.toPath());
    assertThat(content).doesNotContain(revoked.getString("token"));
    assertThat(content).doesNotContain(ApiTokenConfiguration.hashToken(revoked.getString("token")));
    assertThatTheFileHoldsOnlyTheHashOf(kept.getString("token"));
  }

  /**
   * Writer 4 of 4: the backward-compatibility migration in {@code load()}. A token file written before the
   * hashing change holds the token itself under {@code "token"}; the migration replaces it with the hash and
   * the suffix. Getting the plaintext off the disk is the whole point of that code, and nothing asserted that
   * the rewritten file no longer has it.
   */
  @Test
  void loadingALegacyPlaintextFileRewritesItWithTheHashAndRemovesThePlaintext() throws Exception {
    final String legacyPlaintext = "at-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd";

    Files.writeString(tokenFile.toPath(), new JSONObject()
        .put("version", 1)
        .put("tokens", new JSONArray().put(new JSONObject()
            .put("token", legacyPlaintext)
            .put("tokenPrefix", "at-")
            .put("name", "legacy")
            .put("database", "graph")
            .put("expiresAt", 0)
            .put("createdAt", 1L)
            .put("permissions", new JSONObject())))
        .toString(2));

    config.load();

    assertThat(config.getToken(legacyPlaintext))
        .as("the migration must not invalidate a token that was already issued")
        .isNotNull();

    assertThatTheFileHoldsOnlyTheHashOf(legacyPlaintext);
    assertThat(Files.readString(tokenFile.toPath()))
        .as("the legacy key pair goes with the plaintext it described")
        .doesNotContain("tokenPrefix");
  }

  /**
   * The authentication read path writes nothing (issue #7525).
   * <p>
   * Probed by deleting the file and then driving the eviction: a {@code save()} anywhere under {@code getToken}
   * re-creates it, so an absent file afterwards is proof that the lookup did no I/O at all. A timestamp
   * comparison would not be proof - the granularity is coarser than the two calls are apart.
   */
  @Test
  void anExpiredTokenIsEvictedInMemoryWithoutRewritingTheFile() {
    final JSONObject expired = config.createToken("expired", "graph", System.currentTimeMillis() - 10_000,
        new JSONObject());
    assertThat(tokenFile).isFile();
    assertThat(tokenFile.delete()).isTrue();

    assertThat(config.getToken(expired.getString("token")))
        .as("an expired token is refused whether or not the eviction was made durable")
        .isNull();

    assertThat(tokenFile)
        .as("the API-token authentication path must not write the credential file on a request thread")
        .doesNotExist();

    assertThat(config.listTokens())
        .as("the eviction still happens, it is just not persisted from the read path")
        .isEmpty();
  }

  /**
   * What makes not persisting the eviction safe: an expired entry that survives in the file authenticates
   * nobody, because every lookup re-checks the expiry, and the next {@code load()} drops it and rewrites the
   * file without it.
   */
  @Test
  void anExpiredEntryLeftInTheFileNeverAuthenticatesAndIsPrunedAtTheNextLoad() throws Exception {
    final JSONObject expired = config.createToken("expired", "graph", System.currentTimeMillis() - 10_000,
        new JSONObject());
    final JSONObject valid = config.createToken("valid", "graph", 0, new JSONObject());

    final String expiredHash = ApiTokenConfiguration.hashToken(expired.getString("token"));
    assertThat(Files.readString(tokenFile.toPath()))
        .as("the expired entry is on disk: createToken persisted it before it expired")
        .contains(expiredHash);

    final ApiTokenConfiguration reloaded = new ApiTokenConfiguration(CONFIG_PATH);
    reloaded.load();

    assertThat(reloaded.getToken(expired.getString("token"))).isNull();
    assertThat(reloaded.getToken(valid.getString("token"))).isNotNull();
    assertThat(Files.readString(tokenFile.toPath()))
        .as("load() prunes the expired entry and rewrites the file, so the store self-cleans at restart")
        .doesNotContain(expiredHash);
  }

  /**
   * The shape every writer has to leave behind: the hash and the suffix of {@code plaintext}, and no trace of
   * {@code plaintext} itself under any key.
   */
  private void assertThatTheFileHoldsOnlyTheHashOf(final String plaintext) throws Exception {
    assertThat(tokenFile).isFile();

    final String content = Files.readString(tokenFile.toPath());
    assertThat(content)
        .as("a token recoverable from the file is a credential an attacker with read access can replay")
        .doesNotContain(plaintext);
    assertThat(content).contains(ApiTokenConfiguration.hashToken(plaintext));
    assertThat(content).contains("tokenSuffix");
    assertThat(content)
        .as("'token' is the key the plaintext used to be stored under")
        .doesNotContain("\"token\":");

    final JSONArray stored = new JSONObject(content).getJSONArray("tokens");
    boolean found = false;
    for (int i = 0; i < stored.length(); i++) {
      final JSONObject tokenJson = stored.getJSONObject(i);
      assertThat(tokenJson.has("token")).isFalse();
      if (ApiTokenConfiguration.hashToken(plaintext).equals(tokenJson.getString("tokenHash", ""))) {
        found = true;
        assertThat(tokenJson.getString("tokenSuffix"))
            .isEqualTo(plaintext.substring(plaintext.length() - 4));
      }
    }
    assertThat(found).as("the record for this token must actually be in the file").isTrue();
  }

  /** An {@link ArcadeDBServer} that is never started, so the security store can be supplied by the fixture. */
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
