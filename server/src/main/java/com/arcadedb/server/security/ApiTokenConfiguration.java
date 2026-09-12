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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
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
    } catch (final RuntimeException e) {
      // A file that exists but does not parse - truncated by a crash mid-write, hand-edited, or restored from
      // the wrong backup - raises JSONException, which is UNCHECKED. Letting it out of here aborted server
      // startup: load() is called from ServerSecurity.loadUsers(), whose own catch is IOException-only, and
      // that is called straight from ArcadeDBServer.start(). The users and group stores both degrade to a
      // default instead of refusing to boot; this one crashed the server. It now degrades too, and to the EMPTY
      // store rather than to anything else, because empty is the fail-CLOSED direction for credentials: no
      // token authenticates until the operator restores the file.
      onLoadFailure(e);
    }
  }

  /** Owner-only (mode 600) on POSIX systems; silently skipped elsewhere (e.g. Windows). */
  private static void restrictToOwner(final Path file) {
    try {
      final PosixFileAttributeView posixView = Files.getFileAttributeView(file, PosixFileAttributeView.class);
      if (posixView != null)
        posixView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (final IOException | UnsupportedOperationException e) {
      // Non-POSIX system - skip
    }
  }

  /**
   * Preserves an unparseable token file beside itself and leaves the store empty. Copying it aside matters more
   * here than the log line does: the very next {@link #save} - a token minted, or any replicated token entry -
   * overwrites the live file with the empty document, and the evidence of what the node used to hold would be
   * gone with it.
   */
  private void onLoadFailure(final RuntimeException failure) {
    tokens = new ConcurrentHashMap<>();

    final Path corrupt = Paths.get(filePath);
    final Path preserved = corrupt.resolveSibling(FILE_NAME.replace(".json", "-error.json"));
    try {
      Files.copy(corrupt, preserved, StandardCopyOption.REPLACE_EXISTING);
      // The copy is created fresh, so it takes the umask's permissions rather than the 0600 the live file is
      // written with. It holds the same token hashes, names, scopes and expiry, so it gets the same treatment -
      // preserving evidence must not mean publishing it to every account on the host.
      restrictToOwner(preserved);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(this, Level.WARNING, "Could not preserve the unreadable API-token file as '%s'", e,
          preserved);
    }

    LogManager.instance().log(this, Level.SEVERE,
        "API-token file '%s' could not be parsed; it has been copied to '%s' and this node starts with NO API "
            + "tokens, so none of them authenticates until the file is restored. Users and groups are unaffected",
        failure, filePath, preserved);
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
    // Serialised BEFORE the try, so only the write is classified as a persistence failure. A RuntimeException out
    // of toString() is a bug in the document, not a full disk, and reporting it with disk-full wording would send
    // an operator to look at the volume.
    final byte[] bytes = document.toString(2).getBytes(UTF_8);

    final File file = new File(filePath);
    final File dir = file.getParentFile();
    if (dir != null && !dir.exists())
      dir.mkdirs();

    try {
      writeAtomically(file.toPath(), bytes);
      return null;
    } catch (final IOException e) {
      return e;
    }
  }

  /**
   * Writes the token document through a sibling temp file that is fsynced, chmodded and only then renamed over
   * the target, the same way {@link SecurityGroupFileRepository} writes the group document.
   * <p>
   * A direct write to the live file leaves a truncated {@code server-api-tokens.json} behind if the process dies
   * mid-write, and this file is now written on every node on every replicated token change rather than only on
   * the one that served a request - many more chances to be killed in that window. A truncated file is not a
   * degraded token store, it is an unparseable one, which is why {@link #load} also has to survive it.
   * <p>
   * The permissions are set on the TEMP file, before the rename, so the live path never exists even briefly with
   * the default umask: this file is the closest thing the server has to a credential store, and a window in
   * which it is group-readable is a window an attacker can wait for.
   */
  private static void writeAtomically(final Path target, final byte[] bytes) throws IOException {
    final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
    try {
      try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(bytes));
        channel.force(true);
      }

      restrictToOwner(tmp);

      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tmp);
    }
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
   * @param response           the created token including its plaintext under {@code "token"}
   * @param documentBeforeJson the complete token store as it was when the mint was computed, read in the SAME
   *                           critical section as {@code documentJson} so a cluster-wide caller can use it as
   *                           the compare-and-set precondition of {@code documentJson} (issue #7509). Reading it
   *                           separately would let a concurrent replicated apply slip between the two, and the
   *                           precondition would then describe a token set the payload was not built from
   * @param documentJson       the complete token store with the new token in it, for {@link #applyReplicated}
   */
  public record MintedToken(JSONObject response, String documentBeforeJson, String documentJson) {
  }

  /**
   * A prospective change to the token document: what it is NOW and what it would become, both read in one
   * critical section (issue #7509). See {@link MintedToken#documentBeforeJson()} for why the pair has to be
   * atomic.
   *
   * @param before the document the change was computed from, usable as a compare-and-set precondition
   * @param after  the document to replicate
   */
  public record DocumentChange(String before, String after) {
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

    return new MintedToken(response, documentOf(tokens.values()).toString(), documentOf(next).toString());
  }

  /**
   * The token document with {@code tokenHash} removed - beside the document it was removed FROM - or
   * {@code null} when no token has that hash. The cluster-wide half of {@link #deleteToken} (issue #7373).
   * Nothing local is mutated.
   *
   * @throws IllegalArgumentException when handed a plaintext token instead of a hash
   */
  public synchronized DocumentChange documentWithout(final String tokenHash) {
    if (tokenHash.startsWith(TOKEN_PREFIX))
      throw new IllegalArgumentException("Use token hash instead of plaintext token for deletion");
    if (!tokens.containsKey(tokenHash))
      return null;

    final List<JSONObject> next = new ArrayList<>(tokens.size());
    for (final Map.Entry<String, JSONObject> entry : tokens.entrySet())
      if (!entry.getKey().equals(tokenHash))
        next.add(entry.getValue());

    // The before/after pair leaves this monitor together, so a cluster-wide revocation can pin its
    // compare-and-set to the very document it removed the token from (issue #7509).
    return new DocumentChange(documentOf(tokens.values()).toString(), documentOf(next).toString());
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

    // A document with no 'tokens' key is NOT "every token revoked" - an empty set is spelled "tokens": [], and
    // every writer here emits the key unconditionally. Treating the absent key as an empty set would let a
    // truncated or foreign payload revoke every token on every peer, which is the loudest possible way to fail
    // an entry nobody meant to send. Checked before the swap, so the store is left alone.
    //
    // The version is deliberately NOT pinned, unlike the group document's: documentOf() stamps "version": 1 on
    // every write regardless of what arrived, so there is no equivalent "a versionless file is discarded at the
    // next restart" hazard here, and refusing an unknown version would make this node halt on an entry a newer
    // peer will eventually write.
    if (!document.has("tokens"))
      throw new IllegalArgumentException(
          "Replicated API-token document has no 'tokens' array; refusing to install it, because treating that as "
              + "an empty set would revoke every token on this node");

    final JSONArray tokenArray = document.getJSONArray("tokens");
    final ConcurrentHashMap<String, JSONObject> next = new ConcurrentHashMap<>();
    for (int i = 0; i < tokenArray.length(); i++) {
      final JSONObject tokenJson = tokenArray.getJSONObject(i);
      next.put(tokenJson.getString("tokenHash"), tokenJson);
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
  public synchronized boolean deleteToken(final String tokenHash) {
    // synchronized, like every other writer in this class: the remove and the save() that records it have to be
    // one step against a concurrent load()/applyReplicated() swapping the map between them, or the revocation
    // reaches memory and the file keeps the token.
    if (tokenHash.startsWith(TOKEN_PREFIX))
      throw new IllegalArgumentException("Use token hash instead of plaintext token for deletion");
    if (tokens.remove(tokenHash) != null) {
      save();
      return true;
    }
    return false;
  }

  /**
   * Resolves a plaintext token, evicting it if it has expired.
   * <p>
   * Deliberately NOT {@code synchronized}: this is the API-token authentication path, reached on every request
   * carrying one, and the writers it would contend with hold their monitor across a file write. The map
   * generation is read ONCE into a local, so the lookup and the eviction cannot straddle a
   * {@link #applyReplicated} swap and remove from a generation the hit did not come from. When they do straddle
   * one, the eviction lands on a map that is no longer live and {@link #save} writes the newer generation
   * instead - which is the right outcome: the replicated document wins, and the expired token is refused here
   * either way.
   */
  public JSONObject getToken(final String plaintextToken) {
    final ConcurrentHashMap<String, JSONObject> current = tokens;
    final String hash = hashToken(plaintextToken);
    final JSONObject tokenJson = current.get(hash);
    if (tokenJson == null)
      return null;

    final long expiresAt = tokenJson.getLong("expiresAt", 0);
    if (expiresAt > 0 && expiresAt < System.currentTimeMillis()) {
      current.remove(hash);
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
