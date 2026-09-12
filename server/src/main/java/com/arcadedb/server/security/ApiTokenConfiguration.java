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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ApiTokenConfiguration {
  public static final  String                                FILE_NAME    = "server-api-tokens.json";
  private static final String                                TOKEN_PREFIX = "at-";
  private static final int                                   SUFFIX_LEN   = 4;
  private static final int                                   TOKEN_BYTES  = 32;
  private static final SecureRandom                          SECURE_RANDOM = new SecureRandom();
  private final        String                                filePath;
  // Volatile reference to a map that is REPLACED, never cleared in place, whenever the whole token set is
  // rebuilt (load, and the replicated apply of issue #7373). Readers - authenticateByApiToken, on Undertow
  // worker threads - grab the current map lock-free and can therefore never observe the empty window a
  // clear()-then-repopulate leaves behind, which would read as "this token does not exist" and fail an
  // authentication that should have succeeded. Single-entry mutations still go through the live map.
  private volatile     ConcurrentHashMap<String, JSONObject> tokens       = new ConcurrentHashMap<>();

  public ApiTokenConfiguration(final String configPath) {
    this.filePath = Paths.get(configPath, FILE_NAME).toString();
  }

  public synchronized void load() {
    final File file = new File(filePath);
    if (!file.exists()) {
      tokens = new ConcurrentHashMap<>();
      return;
    }

    final ConcurrentHashMap<String, JSONObject> loaded = new ConcurrentHashMap<>();

    try (final FileInputStream fis = new FileInputStream(file)) {
      final JSONObject json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
      if (!json.has("tokens")) {
        tokens = loaded;
        return;
      }

      final JSONArray tokenArray = json.getJSONArray("tokens");
      final long now = System.currentTimeMillis();
      boolean needsSave = false;

      for (int i = 0; i < tokenArray.length(); i++) {
        final JSONObject tokenJson = tokenArray.getJSONObject(i);
        final long expiresAt = tokenJson.getLong("expiresAt", 0);
        if (expiresAt > 0 && expiresAt < now) {
          needsSave = true;
          continue;
        }

        // Backward compatibility: migrate plaintext tokens to hashed
        if (tokenJson.has("token") && !tokenJson.has("tokenHash")) {
          final String plaintext = tokenJson.getString("token");
          final String hash = hashToken(plaintext);
          tokenJson.put("tokenHash", hash);
          tokenJson.put("tokenSuffix", plaintext.length() > SUFFIX_LEN
              ? plaintext.substring(plaintext.length() - SUFFIX_LEN) : plaintext);
          tokenJson.remove("token");
          tokenJson.remove("tokenPrefix");
          needsSave = true;
        }

        loaded.put(tokenJson.getString("tokenHash"), tokenJson);
      }

      tokens = loaded;

      if (needsSave)
        save();

    } catch (final IOException e) {
      // The partially-built map is discarded: a file this node could not read through must not silently
      // narrow the token set it was already serving.
      LogManager.instance().log(this, Level.WARNING, "Error loading API tokens from '%s'", e, filePath);
    }
  }

  public synchronized void save() {
    final Exception failure = persist(documentOf(tokens.values()));
    if (failure != null)
      LogManager.instance().log(this, Level.SEVERE, "Error saving API tokens to '%s'", failure, filePath);
  }

  /**
   * Writes {@code document} to {@link #FILE_NAME}, returning the failure instead of throwing it so a caller
   * that has already published the change in memory can report the failure after finishing the apply
   * (issue #7373, same shape as {@code ServerSecurity.trySaveUsers}).
   *
   * @return the failure, or {@code null} when the document reached the disk
   */
  private Exception persist(final JSONObject document) {
    final File file = new File(filePath);
    if (!file.getParentFile().exists())
      file.getParentFile().mkdirs();

    Exception failure = null;
    try (final OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file), UTF_8)) {
      writer.write(document.toString(2));
    } catch (final IOException | RuntimeException e) {
      failure = e;
    }

    // Set file permissions to owner-only (mode 600) on POSIX systems
    try {
      final PosixFileAttributeView posixView = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);
      if (posixView != null)
        posixView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (final IOException | UnsupportedOperationException e) {
      // Non-POSIX system (e.g., Windows) — skip
    }

    return failure;
  }

  /** The on-disk document shape, built from an explicit token collection rather than from the live map. */
  private static JSONObject documentOf(final Collection<JSONObject> tokenDocuments) {
    final JSONObject json = new JSONObject();
    json.put("version", 1);

    final JSONArray tokenArray = new JSONArray();
    for (final JSONObject tokenJson : tokenDocuments)
      tokenArray.put(tokenJson);

    json.put("tokens", tokenArray);
    return json;
  }

  /**
   * The whole token store as the JSON document {@link #applyReplicated} takes, for seeding a peer that has just
   * joined the cluster (issue #7373). It carries token HASHES, never token material - the plaintext exists only
   * in the one-time {@link MintedToken#response()}.
   */
  public String toJsonPayload() {
    return documentOf(tokens.values()).toString();
  }

  /**
   * A token minted but NOT yet installed: the one-time response for the caller, and the whole token document to
   * replicate (issue #7373).
   *
   * @param response     the created token including its plaintext under {@code "token"}
   * @param documentJson the complete token store with the new token in it, for {@link #applyReplicated}
   */
  public record MintedToken(JSONObject response, String documentJson) {
  }

  /**
   * Generates a token and returns it together with the token document that would result, <b>without</b>
   * touching this node's map or file (issue #7373).
   * <p>
   * The cluster-wide half of {@link #createToken}: in an HA cluster the store is mutated only by the replicated
   * apply, so a mint whose Raft entry never commits leaves no token behind on the node that served the request -
   * which is the same ordering {@code ServerSecurity.createUserClusterWide} uses for users.
   *
   * @throws IllegalArgumentException when a token of that name has already been issued
   */
  public synchronized MintedToken mintToken(final String name, final String database, final long expiresAt,
      final JSONObject permissions) {
    final NewToken minted = newTokenDocument(name, database, expiresAt, permissions);

    final List<JSONObject> next = new ArrayList<>(tokens.values());
    next.add(minted.document());

    final JSONObject response = minted.document().copy();
    response.put("token", minted.plaintext());

    return new MintedToken(response, documentOf(next).toString());
  }

  /**
   * The token document with {@code tokenHash} removed, or {@code null} when no token has that hash - the
   * cluster-wide half of {@link #deleteToken} (issue #7373). Nothing local is mutated.
   *
   * @throws IllegalArgumentException when handed a plaintext token instead of a hash
   */
  public synchronized String documentWithout(final String tokenHash) {
    if (tokenHash.startsWith(TOKEN_PREFIX))
      throw new IllegalArgumentException("Use token hash instead of plaintext token for deletion");
    if (!tokens.containsKey(tokenHash))
      return null;

    final List<JSONObject> next = new ArrayList<>(tokens.size());
    for (final java.util.Map.Entry<String, JSONObject> entry : tokens.entrySet())
      if (!entry.getKey().equals(tokenHash))
        next.add(entry.getValue());

    return documentOf(next).toString();
  }

  /**
   * Installs a replicated token document: published in memory FIRST, then persisted, with the write failure
   * RETURNED rather than thrown (issue #7373).
   * <p>
   * The ordering is the point, and it is the users half of issue #7137 applied to tokens. A replicated document
   * has already been committed by a quorum and applied elsewhere; returning early on a write failure would leave
   * THIS node honouring the previous token set - so a token the operator has just revoked would keep
   * authenticating here for as long as the config volume stayed full or read-only. Revocation that silently
   * applies to a subset of the cluster is the security failure issue #7373 was filed for.
   * <p>
   * Everything that can throw - parsing the document, reading each entry's hash - happens BEFORE the swap, so a
   * payload this node cannot read leaves the store untouched.
   *
   * @return the persistence failure, or {@code null} when the document reached the disk
   */
  public synchronized Exception applyReplicated(final String documentJson) {
    final JSONObject document = new JSONObject(documentJson);

    final ConcurrentHashMap<String, JSONObject> next = new ConcurrentHashMap<>();
    if (document.has("tokens")) {
      final JSONArray tokenArray = document.getJSONArray("tokens");
      for (int i = 0; i < tokenArray.length(); i++) {
        final JSONObject tokenJson = tokenArray.getJSONObject(i);
        next.put(tokenJson.getString("tokenHash"), tokenJson);
      }
    }

    // Published in a single reference swap, so a concurrent authentication never sees a half-rebuilt store.
    tokens = next;

    return persist(documentOf(next.values()));
  }

  /**
   * Mints and installs a token on THIS node only, with no replication. The local half of
   * {@code ServerSecurity.createApiTokenClusterWide} (issue #7373); every caller that is not that method wants
   * the cluster-aware one, or a token minted behind a load balancer authenticates against one node out of three.
   */
  public synchronized JSONObject createToken(final String name, final String database, final long expiresAt, final JSONObject permissions) {
    final NewToken minted = newTokenDocument(name, database, expiresAt, permissions);

    tokens.put(minted.document().getString("tokenHash"), minted.document());
    save();

    // Return a response that includes the plaintext token (one-time display)
    final JSONObject response = minted.document().copy();
    response.put("token", minted.plaintext());
    return response;
  }

  /**
   * Builds the stored document for a new token, rejecting a duplicate name. The plaintext comes back beside the
   * document rather than inside it: it must never reach the file or a Raft entry, only the one-time response.
   */
  private NewToken newTokenDocument(final String name, final String database, final long expiresAt,
      final JSONObject permissions) {
    for (final JSONObject existing : tokens.values()) {
      if (name.equals(existing.getString("name", "")))
        throw new IllegalArgumentException("A token with name '" + name + "' already exists");
    }

    final byte[] randomBytes = new byte[TOKEN_BYTES];
    SECURE_RANDOM.nextBytes(randomBytes);
    final String tokenValue = TOKEN_PREFIX + HexFormat.of().formatHex(randomBytes);
    final String hash = hashToken(tokenValue);
    final String suffix = tokenValue.substring(tokenValue.length() - SUFFIX_LEN);

    final JSONObject tokenJson = new JSONObject();
    tokenJson.put("tokenHash", hash);
    tokenJson.put("tokenSuffix", suffix);
    tokenJson.put("name", name);
    tokenJson.put("database", database);
    tokenJson.put("expiresAt", expiresAt);
    tokenJson.put("createdAt", System.currentTimeMillis());
    tokenJson.put("permissions", permissions);

    return new NewToken(tokenJson, tokenValue);
  }

  /**
   * A freshly generated token: the document that gets stored and replicated, and the plaintext that does not.
   * Two fields rather than one document with the plaintext inside it, so there is no key anyone has to remember
   * to strip before the document reaches the file or a Raft entry.
   */
  private record NewToken(JSONObject document, String plaintext) {
  }

  /**
   * Revokes a token on THIS node only, with no replication. The local half of
   * {@code ServerSecurity.deleteApiTokenClusterWide} (issue #7373); a revocation that reaches one node is not a
   * revocation, so every caller that is not that method wants the cluster-aware one.
   */
  public boolean deleteToken(final String tokenHash) {
    if (tokenHash.startsWith(TOKEN_PREFIX))
      throw new IllegalArgumentException("Use token hash instead of plaintext token for deletion");
    if (tokens.remove(tokenHash) != null) {
      save();
      return true;
    }
    return false;
  }

  public JSONObject getToken(final String plaintextToken) {
    final String hash = hashToken(plaintextToken);
    final JSONObject tokenJson = tokens.get(hash);
    if (tokenJson == null)
      return null;

    final long expiresAt = tokenJson.getLong("expiresAt", 0);
    if (expiresAt > 0 && expiresAt < System.currentTimeMillis()) {
      tokens.remove(hash);
      save();
      return null;
    }

    return tokenJson;
  }

  public List<JSONObject> listTokens() {
    return new ArrayList<>(tokens.values());
  }

  public static boolean isApiToken(final String token) {
    return token != null && token.startsWith(TOKEN_PREFIX);
  }

  // Salt is not needed here because API tokens are generated with 32 bytes of SecureRandom entropy
  // (256 bits), making rainbow tables and precomputation attacks infeasible. Salting is essential
  // for user-chosen passwords (low entropy) but unnecessary for high-entropy random tokens.
  public static String hashToken(final String plaintext) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest(plaintext.getBytes(UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }
}
