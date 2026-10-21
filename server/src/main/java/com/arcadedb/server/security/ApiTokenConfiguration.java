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
import com.arcadedb.serializer.json.JSONException;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ApiTokenConfiguration {
  public static final  String       FILE_NAME       = "server-api-tokens.json";
  /** Where a {@link #FILE_NAME} that cannot be loaded is preserved. */
  public static final  String       ERROR_FILE_NAME = "server-api-tokens-error.json";
  /** Prefix of the watcher thread's name; the watched file's path is appended to it. See {@link #startWatching}. */
  public static final  String       WATCHER_PREFIX  = "arcadedb-api-tokens-watcher";
  private static final String       TOKEN_PREFIX    = "at-";
  private static final int          SUFFIX_LEN      = 4;
  private static final int          TOKEN_BYTES     = 32;
  private static final SecureRandom SECURE_RANDOM   = new SecureRandom();
  private final        String                                filePath;
  // Volatile reference to a map that is REPLACED, never cleared in place, whenever the whole token set is
  // rebuilt (load, and the replicated apply of issue #7373). Readers - authenticateByApiToken, on Undertow
  // worker threads - grab the current map lock-free and can therefore never observe the empty window a
  // clear()-then-repopulate leaves behind, which would read as "this token does not exist" and fail an
  // authentication that should have succeeded. Single-entry mutations still go through the live map.
  private volatile     ConcurrentHashMap<String, JSONObject> tokens       = new ConcurrentHashMap<>();
  // The reload policy and the watcher, shared with the users and group stores (issue #10404). Locked on THIS monitor, so
  // a reload serializes with every synchronized writer below.
  private final        Policy                                policy;
  // Whether the tokens in force are NEWER than the file: the last write failed (a replicated install publishes before
  // it persists, issue #7373). Guarded by the monitor; reload() refuses to re-read an older file while it is set.
  private              boolean                               diskBehind;

  public ApiTokenConfiguration(final String configPath) {
    this.filePath = Paths.get(configPath, FILE_NAME).toString();
    this.policy = new Policy();
  }

  /**
   * The FIRST load, at startup: publishes {@link #FILE_NAME}, or the empty store when there is none. A file that cannot
   * be parsed, or is not a token document, also leaves the store EMPTY (fail-closed): a file that exists but does not
   * parse - truncated by a crash mid-write, hand-edited, or restored from the wrong backup - must not abort server
   * startup, and empty is the fail-CLOSED direction for credentials, so no token authenticates until the operator
   * restores the file. Copying it aside as {@link #ERROR_FILE_NAME} matters more than the log line: the very next
   * {@link #save} - a token minted, or any replicated token entry - overwrites the live file with the empty document.
   * <p>
   * A file that cannot be READ (an I/O error) leaves the tokens already in memory alone instead: it must not silently
   * narrow a token set this node was already serving.
   * <p>
   * Not what a running server calls when the file changes: that is {@link #reload()}, whose policy is the opposite one.
   */
  public synchronized void load() {
    policy.firstLoad();
  }

  /**
   * Re-reads {@link #FILE_NAME} on a running server, from {@link #startWatching}'s watcher (issue #10383). The token half
   * of the rule issue #10326 set for the users file: a file that is missing, unreadable, unparseable or not a token
   * document keeps the tokens already in force. A missing file is logged at WARNING; any other failure at SEVERE, and a
   * file that could be read is preserved as {@link #ERROR_FILE_NAME}.
   * <p>
   * {@link #load()}'s fail-closed EMPTY store is right at startup, when there is nothing in force to keep. As a reload
   * it revoked every API token of a running server because of one bad read - and before this watcher existed it ran
   * only as a side effect of a users-file reload, so nothing re-read the file afterwards and the revocation outlasted
   * the restore of the file for the rest of the process lifetime.
   * <p>
   * Also refuses while the tokens in force have not reached the disk ({@link #diskBehind}): the file then holds an
   * OLDER token set - say, one with a token a replicated revocation has since removed - and re-reading it would put
   * that token back. The #7752 retry ({@link #persistCurrent}) clears the condition.
   *
   * @return {@code true} when the file's token set was published, {@code false} when the tokens in force were kept
   */
  synchronized boolean reload() {
    return policy.reload();
  }

  /**
   * Reads {@code file} into a token set, migrating legacy plaintext entries in the returned copy. Publishes nothing.
   *
   * @throws IOException   if the file cannot be read
   * @throws JSONException if it does not parse, has no {@code tokens} array, or an entry has no {@code tokenHash}
   */
  private LoadedTokens readFile(final File file) throws IOException {
    final JSONObject json;
    try (final FileInputStream fis = new FileInputStream(file)) {
      json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
    }

    // A document with no 'tokens' key is NOT "no tokens" - an empty set is spelled "tokens": [], and every writer here
    // emits the key unconditionally (the same reading applyReplicated() makes). It used to load as the empty set with
    // nothing logged and no copy kept, and the next save() then wrote the empty set over it.
    if (!json.has("tokens"))
      throw new JSONException("'" + FILE_NAME + "' has no 'tokens' array");

    final ConcurrentHashMap<String, JSONObject> loaded = new ConcurrentHashMap<>();
    final JSONArray tokenArray = json.getJSONArray("tokens");
    boolean needsSave = false;

    // EXPIRED ENTRIES ARE LOADED like any other, and refused like any other by isExpired() at every read
    // (issue #7601). Dropping them here is what this used to do, and in a cluster that was a node-local edit
    // of a REPLICATED document: it ran at whatever moment each node happened to restart, so the nodes' token
    // sets - and with them the #7509 compare-and-set fingerprints computed from them - drifted apart with no
    // entry applied and nothing logged. #pruneExpired is where the removal lives now: on the write paths,
    // which reach every node in one entry. The only rewrite this loop still asks for is the legacy plaintext
    // migration below.
    for (int i = 0; i < tokenArray.length(); i++) {
      final JSONObject tokenJson = tokenArray.getJSONObject(i);

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

    return new LoadedTokens(loaded, needsSave);
  }

  /** A token set read from the file, and whether it carried legacy plaintext entries that have to be saved hashed. */
  private record LoadedTokens(ConcurrentHashMap<String, JSONObject> tokens, boolean migrated) {
  }

  /**
   * Schedules the {@link #FILE_NAME} modification watcher, which calls {@link #reload()} (issue #10383). Idempotent and
   * one-shot per instance: see {@link ReloadableSecurityArtifact#startWatching}.
   * <p>
   * The token file used to have no watcher of its own: it was re-read only as a side effect of a
   * {@code server-users.jsonl} reload, so an operator's edit of it stayed invisible until that unrelated file changed.
   * <p>
   * The server's own writes bump the file's stamp without advancing the baseline, so each costs one redundant reload of
   * the document just written - the trade {@link SecurityGroupFileRepository#save} explains: advancing the baseline after
   * a write would swallow an operator edit landing in the same window.
   *
   * @param checkEveryMs the polling period; nothing is scheduled when it is not positive
   */
  public synchronized void startWatching(final int checkEveryMs) {
    policy.startWatching(checkEveryMs);
  }

  /** Cancels the watcher, for good: see {@link ReloadableSecurityArtifact#stop()}. */
  public synchronized void stop() {
    policy.stop();
  }

  private File errorFile() {
    return new File(new File(filePath).getParentFile(), ERROR_FILE_NAME);
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
   * The tokens half of the shared reload policy (issue #10404). Two answers differ from the default policy and are
   * pinned by tests: a first load that cannot READ the file keeps what is in memory
   * ({@code Issue7373ClusterWideGroupsAndTokensTest}), and a reload refuses while the tokens in force are newer than the
   * file ({@link #diskBehind}).
   */
  private final class Policy extends ReloadableSecurityArtifact<LoadedTokens> {
    private Policy() {
      super(ApiTokenConfiguration.this, ApiTokenConfiguration.this, new File(filePath), errorFile(), WATCHER_PREFIX,
          "API-token configuration");
    }

    @Override
    protected LoadedTokens read() throws IOException {
      return readFile(file);
    }

    /** The empty store: fail-closed, no token authenticates. */
    @Override
    protected LoadedTokens fallback() {
      return new LoadedTokens(new ConcurrentHashMap<>(), false);
    }

    /**
     * One reference swap, so a concurrent authentication never sees a half-rebuilt store. A legacy file is saved hashed
     * right after; that write does not advance the watcher's baseline (see {@link SecurityGroupFileRepository#save}), so
     * the next tick re-reads the file once and publishes the same, now hashed, token set.
     */
    @Override
    protected boolean publish(final LoadedTokens loaded, final boolean isFallback) {
      tokens = loaded.tokens();
      if (loaded.migrated())
        save();
      return true;
    }

    /**
     * Always: an EMPTY token set is a token set in force too. A reload never turns into a first load here, which is
     * what makes a revocation by hand-edit safe from a later unreadable file.
     */
    @Override
    protected boolean hasInForce() {
      return true;
    }

    /**
     * Says outright that the previous tokens stay VALID: an operator who edited the file to revoke one must not read
     * "keeping" as "your revocation is pending".
     */
    @Override
    protected String keeping() {
      return "keeping the " + tokens.size() + " API tokens already in force, which keep authenticating until a "
          + "valid file is read (a token removed by this edit is NOT revoked)";
    }

    @Override
    protected String fallingBack() {
      return "this node starts with NO API tokens, so none of them authenticates until the file is restored (users and groups "
          + "are unaffected)";
    }

    @Override
    protected LoadedTokens onFirstLoadFailure(final Exception failure) {
      if (failure instanceof IOException) {
        // The file could not be read through: whatever is in memory stays, and there is nothing that could be copied.
        LogManager.instance().log(ApiTokenConfiguration.this, Level.WARNING, "Error loading API tokens from '%s'", failure, filePath);
        return null;
      }
      return super.onFirstLoadFailure(failure);
    }

    @Override
    protected String refusal() {
      if (!diskBehind)
        return null;
      return "the API tokens in force have not been written to it yet (the last write failed), so it is not read: make the "
          + "configuration directory writable again - the next successful write lifts this and writes the tokens in force OVER "
          + "this edit - or, to apply the edit instead, restart the server once the directory is writable";
    }
  }

  public synchronized void save() {
    final Exception failure = persist(documentOf(tokens.values()));
    if (failure != null)
      LogManager.instance().log(this, Level.SEVERE, "Error saving API tokens to '%s'", failure, filePath);
  }

  /**
   * Writes the token set in force again, for a replicated install whose own write failed (issue #7752).
   *
   * @return the failure, or {@code null} when the document reached the disk
   */
  synchronized Exception persistCurrent() {
    return persist(documentOf(tokens.values()));
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
      diskBehind = false;
      return null;
    } catch (final IOException e) {
      diskBehind = true;
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
        // Looped, because FileChannel.write() is only obliged to consume SOME of what remains: a short write here
        // would fsync and publish a truncated server-api-tokens.json. load() already survives an unparseable file
        // by degrading to the empty token store (fail-closed), so a silent short write here would cost every API
        // token until the file is restored (issue #7825).
        FileUtils.writeFully(channel, ByteBuffer.wrap(bytes));
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

    // The document that WILL be in force drops whatever has expired; the one pinned as the precondition is the
    // unpruned document actually in force right now (issue #7601). Both are read in this one critical section,
    // so the pair cannot straddle a replicated apply.
    final List<JSONObject> next = pruneExpired(tokens.values());
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

    // Same pairing as mintToken: the revocation carries whatever expired away with it, and the precondition is
    // the unpruned document in force (issue #7601).
    final List<JSONObject> next = new ArrayList<>(tokens.size());
    for (final JSONObject live : pruneExpired(tokens.values()))
      if (!tokenHash.equals(live.getString("tokenHash")))
        next.add(live);

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

    // A whole-map swap rather than a put, so the expired entries this change also retires leave memory and the
    // file in the same step (issue #7601). Readers grab the map reference once, so they never see the window.
    final ConcurrentHashMap<String, JSONObject> next = new ConcurrentHashMap<>();
    for (final JSONObject live : pruneExpired(tokens.values()))
      next.put(live.getString("tokenHash"), live);
    next.put(minted.document().getString("tokenHash"), minted.document());
    tokens = next;
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
    // Expired entries linger in the document until the next token change retires them (issue #7601), and an
    // expired token authenticates nobody - so it must not reserve its name against the replacement an operator
    // is minting for exactly that reason.
    final long now = System.currentTimeMillis();
    for (final JSONObject existing : tokens.values()) {
      if (name.equals(existing.getString("name", "")) && !isExpired(existing, now))
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
    if (!tokens.containsKey(tokenHash))
      return false;

    // Swapped whole, for the same reason createToken does (issue #7601): the revocation and the retirement of
    // whatever expired are one change to the document, not a removal followed by a drifting set.
    final ConcurrentHashMap<String, JSONObject> next = new ConcurrentHashMap<>();
    for (final JSONObject live : pruneExpired(tokens.values()))
      if (!tokenHash.equals(live.getString("tokenHash")))
        next.put(live.getString("tokenHash"), live);
    tokens = next;
    save();
    return true;
  }

  /**
   * Resolves a plaintext token, refusing it when it has expired.
   * <p>
   * Deliberately NOT {@code synchronized}: this is the API-token authentication path, reached on every request
   * carrying one, and the writers it would contend with hold their monitor across a file write. The map
   * generation is read ONCE into a local, so a concurrent {@link #applyReplicated} swap cannot be observed
   * half-applied.
   * <p>
   * <b>An expired token is refused, never removed</b> (issues #7525, #7601). The removal went in two steps.
   * Issue #7525 dropped the {@link #save} that came with it: since issue #7513 a save writes through a temp file
   * that is <i>fsynced</i> before the rename, so an expiry turned a request thread into an I/O thread and made it
   * queue behind every other writer on this object's monitor - with a tail of tokens minted on one TTL all
   * fsyncing on Undertow workers at the same instant. Issue #7601 dropped the in-memory removal too, which is
   * the half that mattered in a cluster:
   * <ul>
   *   <li>the token document is REPLICATED, and issue #7509 gates every replicated change on a fingerprint of
   *       the document in force. Removing an entry here changed that fingerprint on one node - the node a client
   *       happened to present the expired token to - with no Raft entry applied and nothing logged;</li>
   *   <li>from then on that node submitted a precondition no node could match against the last document the
   *       cluster installed, so its every later token change was refused, permanently and across restarts,
   *       while the same operations kept working from its peers.</li>
   * </ul>
   * Leaving the entry in place costs nothing: {@link #isExpired} is re-read on every lookup, so an expired entry
   * authenticates nobody, and {@link #pruneExpired} takes it out of the document the next token change writes -
   * through Raft, on every node at once, which is the only way a replicated document may change.
   */
  public JSONObject getToken(final String plaintextToken) {
    final ConcurrentHashMap<String, JSONObject> current = tokens;
    final JSONObject tokenJson = current.get(hashToken(plaintextToken));
    if (tokenJson == null || isExpired(tokenJson, System.currentTimeMillis()))
      return null;

    return tokenJson;
  }

  /**
   * Whether {@code tokenJson} carries an expiry that has passed at {@code now}. A zero or absent one never has.
   * <p>
   * Public because the expiry rule has two readers and must not come to mean two things: this class refuses and
   * retires on it, and {@code ServerControlPlane.listApiTokens} reports it (review of PR #7941).
   */
  public static boolean isExpired(final JSONObject tokenJson, final long now) {
    final long expiresAt = tokenJson.getLong("expiresAt", 0);
    return expiresAt > 0 && expiresAt < now;
  }

  /**
   * {@code current} without the entries whose expiry has passed, in iteration order (issue #7601).
   * <p>
   * The one place an expired token is physically removed, and it is only ever called to build the document a
   * token change is about to write - locally by {@link #createToken} and {@link #deleteToken}, and through Raft
   * by {@link #mintToken} and {@link #documentWithout}. That is what keeps expiry out of the compare-and-set
   * premise: the "before" document a cluster-wide change pins is always the unpruned one in force, so it matches
   * what every node holds, and the "after" document is computed once by the submitter and installed verbatim
   * everywhere, so the prune reaches every node in the same entry rather than at each node's own clock.
   * <p>
   * A cluster that never administers a token therefore keeps its expired entries, which is the right trade: they
   * authenticate nobody, their number is bounded by the tokens ever minted, and the alternative is a read path
   * that writes.
   */
  private static List<JSONObject> pruneExpired(final Collection<JSONObject> current) {
    final long now = System.currentTimeMillis();
    final List<JSONObject> live = new ArrayList<>(current.size());
    for (final JSONObject tokenJson : current)
      if (!isExpired(tokenJson, now))
        live.add(tokenJson);
    return live;
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
