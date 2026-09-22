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
package com.arcadedb.server.ai;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * File-based storage for AI chat conversations.
 * Chats are stored as JSON files under {serverRoot}/chats/{@link #hashUsername(String) hash(username)}/.
 */
public class ChatStorage {
  // Fixed stripe of locks giving concurrent writers to the same (user, chatId) a deterministic
  // last-writer-wins ordering. The "never a partial/spliced file" guarantee actually comes from
  // atomicWriteFile()'s atomic rename, not from this lock; the read-modify-write cycle in the
  // handler reads outside the lock, so this stripe does not prevent lost updates. Sized as a power
  // of two so the index masks cleanly; collisions across unrelated chats are harmless (they wait).
  private static final int            LOCK_STRIPES = 64;
  private final        ReentrantLock[] writeLocks   = new ReentrantLock[LOCK_STRIPES];

  // Names emitted by hashUsername(): a legacy directory matching this shape is somebody's live
  // hashed store, never a pre-hash directory, so the migration must not touch it. Matched
  // case-insensitively because NTFS and the default macOS APFS/HFS+ configuration are: there,
  // "chats/ABC...DEF" and "chats/abc...def" are one directory, so an upper-case spelling of a
  // digest would otherwise pass this test and still resolve onto the victim's store.
  private static final Pattern HASHED_DIR_NAME = Pattern.compile("[0-9a-fA-F]{64}");

  private final String rootPath;

  // The server's registered account names, consulted only to resolve an AMBIGUOUS legacy directory
  // name (issue #8078) - never to decide whether a request is allowed, which is the security layer's
  // job and not this class's. Null in the single-argument constructor, which is what every existing
  // caller (and every test that does not care about this) gets: with no known-user set to consult,
  // ambiguity is refused exactly as it was before #8078, which is the safe direction to be wrong in.
  private final Supplier<Set<String>> knownUsernames;

  // Legacy directory NAMES whose refused migration has already been reported, so the operator gets
  // the message once per name instead of once per request. Names only, and never cleared: if an
  // operator resolves a flagged directory and a later one sanitizes to the same name, that one is
  // refused silently until the next restart. The refusal itself is always re-evaluated - this set
  // gates only the logging - so resolving a directory still takes effect immediately.
  private final Set<String> reportedLegacyDirectories = ConcurrentHashMap.newKeySet();

  public ChatStorage(final String rootPath) {
    this(rootPath, null);
  }

  /**
   * @param knownUsernames Supplies the server's current registered account names, so an ambiguous legacy
   *                        directory name (see {@link #migrateLegacyDirectoryIfPresent}) can be resolved
   *                        against who could actually have written it rather than refused on the
   *                        theoretical preimage alone. May be {@code null}, which keeps the fully
   *                        conservative behaviour of always refusing an ambiguous name.
   */
  public ChatStorage(final String rootPath, final Supplier<Set<String>> knownUsernames) {
    this.rootPath = rootPath;
    this.knownUsernames = knownUsernames;
    for (int i = 0; i < LOCK_STRIPES; i++)
      writeLocks[i] = new ReentrantLock();
  }

  /**
   * Lists all chats for a user, returning metadata only (no full messages).
   */
  public List<JSONObject> listChats(final String username) {
    final File userDir = getUserDir(username);
    if (!userDir.exists())
      return List.of();

    final File[] files = userDir.listFiles((dir, name) -> name.endsWith(".json"));
    if (files == null)
      return List.of();

    final List<JSONObject> chats = new ArrayList<>();
    for (final File file : files) {
      try {
        final String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        final JSONObject chat = new JSONObject(content);
        // Return metadata only
        final JSONObject meta = new JSONObject();
        meta.put("id", chat.getString("id"));
        meta.put("title", chat.getString("title", "Untitled"));
        meta.put("database", chat.getString("database", ""));
        meta.put("created", chat.getString("created", ""));
        meta.put("updated", chat.getString("updated", ""));
        chats.add(meta);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error reading chat file %s: %s", file.getName(), e.getMessage());
      }
    }

    // Sort by updated date descending
    chats.sort(Comparator.comparing((JSONObject c) -> c.getString("updated", "")).reversed());
    return chats;
  }

  /**
   * Gets a full chat by ID.
   */
  public JSONObject getChat(final String username, final String chatId) {
    final File file = getChatFile(username, chatId);
    if (!file.exists())
      return null;

    try {
      final String content = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
      return new JSONObject(content);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error reading chat %s: %s", chatId, e.getMessage());
      return null;
    }
  }

  /**
   * Saves a chat. Creates the user directory if needed.
   *
   * <p>The write is atomic (temp file + atomic rename) and serialized per {@code (username, chatId)}
   * so concurrent writers never produce a spliced file and a reader never observes a partial one.
   * A write failure is propagated as an unchecked exception rather than swallowed, so the caller does
   * not report success while nothing persisted.
   *
   * @throws IllegalStateException if the chat could not be persisted.
   */
  public void saveChat(final String username, final JSONObject chat) {
    // No explicit mkdirs here: atomicWriteFile() creates the target's parent directory (the user
    // dir) before writing, so a separate mkdirs would be redundant.
    final String chatId = chat.getString("id");
    final File file = getChatFile(username, chatId);
    final String content = chat.toString(2);

    final ReentrantLock lock = lockFor(username, chatId);
    lock.lock();
    try {
      FileUtils.atomicWriteFile(file, content);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error saving chat %s: %s", chatId, e.getMessage());
      throw new IllegalStateException("Error saving chat " + chatId, e);
    } finally {
      lock.unlock();
    }
  }

  private ReentrantLock lockFor(final String username, final String chatId) {
    final int idx = (username + '/' + chatId).hashCode() & (LOCK_STRIPES - 1);
    return writeLocks[idx];
  }

  /**
   * Deletes a chat by ID.
   */
  public boolean deleteChat(final String username, final String chatId) {
    final File file = getChatFile(username, chatId);
    return file.exists() && file.delete();
  }

  /**
   * Creates a new chat object with a generated ID.
   */
  public static JSONObject createNewChat(final String database, final String title) {
    final JSONObject chat = new JSONObject();
    chat.put("id", UUID.randomUUID().toString());
    chat.put("title", title);
    chat.put("database", database);
    final String now = Instant.now().toString();
    chat.put("created", now);
    chat.put("updated", now);
    chat.put("messages", new JSONArray());
    return chat;
  }

  /**
   * Generates a title from the first user message.
   */
  public static String generateTitle(final String message) {
    if (message == null || message.isEmpty())
      return "Untitled";
    final String cleaned = message.replaceAll("\\s+", " ").trim();
    if (cleaned.length() <= 40)
      return cleaned;
    return cleaned.substring(0, 37) + "...";
  }

  private File getUserDir(final String username) {
    final File hashedDir = Paths.get(rootPath, "chats", hashUsername(username)).toFile();
    migrateLegacyDirectoryIfPresent(username, hashedDir);
    return hashedDir;
  }

  /**
   * One-time lazy migration from the pre-hash directory layout ({@link #sanitizeFilename(String)}
   * of the username) to the current one ({@link #hashUsername(String)}), so a server upgrading from
   * a release that predates the hash keeps serving each user's existing chat history instead of
   * silently orphaning it under the old directory name.
   *
   * <p>The legacy directory name is derived from user-controlled text, so before moving anything
   * this checks that the name identifies exactly one user and is not already somebody's live store.
   * Two directories are refused (see #7620), and a refusal is not an error: the directory stays
   * where it is, intact, and the user simply starts with an empty store under their hash.
   *
   * <ol>
   * <li><b>A name with the shape of a hashed store</b> ({@code [0-9a-fA-F]{64}}) is never moved.
   * {@link #sanitizeFilename(String)} leaves hex digits alone, and {@link #hashUsername(String)}
   * emits exactly 64 lowercase hex characters, so a user who names themselves another user's digest
   * would otherwise have the victim's current, live directory moved into their own on first access.
   * This needs no legacy layout to have ever existed - it is reachable on a fresh install. Matched
   * case-insensitively on purpose: an upper-case spelling of a digest is a different string but, on
   * a case-insensitive filesystem, the same directory.</li>
   * <li><b>A name more than one account could have written</b> is never moved, unless the server's
   * actual registered accounts prove the ambiguity is only theoretical (issue #8078). A name can be
   * shared two ways, and the check has to answer both:
   * <ul>
   * <li>{@code sanitizeFilename} rewrites every character outside {@code [a-zA-Z0-9_-]} <i>to</i>
   * {@code '_'}, so a name containing one has infinitely many possible preimages in the abstract
   * ({@code user@corp.com}, {@code user.corp.com} and {@code user_corp_com} all produce
   * {@code user_corp_com}).</li>
   * <li>{@code sanitizeFilename} does not fold case, but NTFS and the default macOS APFS/HFS+
   * configuration do, so {@code Alice} and {@code alice} - each the only preimage of its own
   * sanitized name, neither containing an underscore - still resolve to ONE directory under
   * {@code chats/}, which both of them wrote to before the upgrade (issue #8154). Asking only about
   * preimages of {@code sanitizeFilename} answers a question about the function when the question is
   * about the filesystem.</li>
   * </ul>
   * Most preimages are not accounts that exist, so {@link #knownUsernames} is filtered through
   * {@link #sanitizeFilename(String)} to find how many REAL accounts produced this name, comparing
   * case-insensitively so both shapes of sharing are caught: if exactly one does - necessarily
   * {@code username} itself, since {@code legacyName} was derived from it two lines above - the
   * migration is unambiguous in practice and proceeds. If another one does, awarding the directory to
   * whoever is looked up first would hand that user read and delete access to the other's chats,
   * which is the cross-user access #7113 set out to remove rather than a fix for it - so the
   * migration is refused.
   * <p>
   * When the accounts cannot be consulted at all ({@link #knownUsernames} is {@code null}, or the
   * supplier throws, returns {@code null}, or does not list {@code username}), neither answer can be
   * given, and the pre-#8078 rule stands in: a name containing {@code '_'} is refused on the
   * theoretical preimage alone, a name without one is claimed. That last case is the one path this
   * class cannot make safe from the file tree alone - see {@link #migrationRefusedByAccountList}.
   * <p>
   * The moment two REAL accounts are seen to collide on {@code legacyName}, that fact is recorded on
   * disk as an {@link #ambiguityMarkerFile(String) ambiguity marker} and checked before every later
   * attempt, so the refusal survives one of those two accounts being deleted afterwards. Deciding
   * purely from the CURRENT account list would not: if account B is deleted after B and A were both
   * seen to collide on this name, a later lookup for A would see only one current account mapping
   * onto it and migrate the directory - including B's chats - into A's store, reopening the very
   * cross-user access this guard exists to prevent. The marker is the directory's memory of a fact
   * the current account list alone cannot represent (code review on PR #8126).</li>
   * </ol>
   *
   * <p>Resolving a directory that stays ambiguous even against the real account list needs to know
   * which chat belonged to whom, which is not recoverable from the file tree, so it is left to an
   * operator and reported once per directory.
   */
  private void migrateLegacyDirectoryIfPresent(final String username, final File hashedDir) {
    if (hashedDir.exists())
      return;
    final String legacyName = sanitizeFilename(username);
    final File legacyDir = Paths.get(rootPath, "chats", legacyName).toFile();
    if (!legacyDir.exists() || legacyDir.equals(hashedDir))
      return;

    if (HASHED_DIR_NAME.matcher(legacyName).matches()) {
      warnOncePerLegacyDirectory(legacyName,
          "Refusing to migrate legacy chat directory '%s': the name has the shape of a hashed chat store, so it is or could become another "
              + "user's live directory. The account using this name starts with an empty chat store.");
      return;
    }

    // Asked for EVERY name, not only one containing '_' (issue #8154): a case-folded twin shares the
    // legacy directory without either name containing an underscore, and the pre-#8154 gate skipped
    // the account-list check - and with it the ambiguity marker - for exactly those names.
    final File ambiguityMarker = ambiguityMarkerFile(legacyName);
    if (ambiguityMarker.exists() || migrationRefusedByAccountList(legacyName, username, ambiguityMarker)) {
      // migrationRefusedByAccountList() may have just created the marker, so this is re-checked rather than
      // reusing the boolean above: only mention a file in the operator-facing message once it is actually there.
      if (ambiguityMarker.exists())
        warnOncePerLegacyDirectory(legacyName,
            "Refusing to migrate legacy chat directory '%s': more than one user name maps onto it, so its chats cannot be attributed "
                + "to a single user. This is recorded at '%s' and will keep refusing the migration even if one of the colliding "
                + "accounts is later deleted; delete that file once you have manually moved each chat under the owner's hashed "
                + "directory.", ambiguityMarker.getAbsolutePath());
      else
        warnOncePerLegacyDirectory(legacyName,
            "Refusing to migrate legacy chat directory '%s': more than one user name maps onto it, so its chats cannot be attributed "
                + "to a single user. It has been left untouched - move each chat under the owner's hashed directory by hand to "
                + "restore it.");
      return;
    }

    if (!isSpelledExactlyOnDisk(legacyDir, legacyName)) {
      warnOncePerLegacyDirectory(legacyName,
          "Refusing to migrate legacy chat directory '%s': the directory that name resolves to is spelled differently on disk, so on this "
              + "case-insensitive filesystem it belongs to a different user name. It has been left untouched.");
      return;
    }

    try {
      Files.move(legacyDir.toPath(), hashedDir.toPath());
    } catch (final IOException e) {
      // Lost a race with a concurrent migration of the same user, or the legacy directory vanished/
      // the hashed one appeared between the exists() checks above and this move: either way, whatever
      // directory is left standing is authoritative and the caller just proceeds with hashedDir.
      LogManager.instance().log(this, Level.FINE, "Could not migrate legacy chat directory: %s", e.getMessage());
    }
  }

  /**
   * How many of the server's currently registered accounts could have written the legacy directory
   * called {@code legacyName} - the question {@link #migrateLegacyDirectoryIfPresent} has to answer
   * before moving it onto anyone's hashed store.
   */
  private enum LegacyNameOwnership {
    /**
     * Exactly one registered account's sanitized name is {@code legacyName}, and it is
     * {@code username} itself. Nobody else can have written the directory, so it is safe to claim.
     */
    SOLE,
    /**
     * A second registered account's sanitized name is {@code legacyName} too, ignoring case. Which
     * of the two wrote which chat is not recoverable from the file tree, so the directory belongs to
     * neither and is never migrated.
     */
    COLLISION,
    /**
     * The registered accounts could not be consulted, so neither answer can be given from them.
     * {@link #migrationRefusedByAccountList} decides what to do about that.
     */
    UNKNOWN
  }

  /**
   * Which of the three {@link LegacyNameOwnership} answers the server's registered accounts give for
   * {@code legacyName} (issue #8078). {@code username} is guaranteed to be one account mapping onto
   * it, since {@code legacyName} is always {@code sanitizeFilename(username)} at the one call site -
   * so this really asks "does any OTHER registered account map onto it as well", and answers
   * {@link LegacyNameOwnership#UNKNOWN} rather than guessing whenever the accounts cannot be read:
   * no known-user supplier, a supplier that throws or returns {@code null}, or {@code username} not
   * itself among the accounts it returns.
   * <p>
   * A pure query: {@link #migrationRefusedByAccountList} is what records a
   * {@link LegacyNameOwnership#COLLISION} on disk, so the fact survives the colliding account being
   * deleted later - see the class javadoc's second migration rule.
   * <p>
   * A candidate account collides when its sanitized name matches {@code legacyName} CASE-INSENSITIVELY,
   * not only exactly (code review on PR #8126). {@code ServerSecurity} keys accounts by exact name, so
   * {@code John_Doe} and {@code john_doe} - or {@code Alice} and {@code alice} - can both be registered,
   * and {@code sanitizeFilename} does not fold case; but {@code legacyDir.exists()} at the one call site
   * does, on the case-insensitive filesystems this class already treats specially
   * ({@link #HASHED_DIR_NAME}'s own case-insensitive match, {@link #isSpelledExactlyOnDisk}). Comparing
   * case-sensitively here would miss that the two accounts' sanitized names name the very same on-disk
   * directory and let one of them claim it as if only it had ever written there.
   * <p>
   * {@code equalsIgnoreCase} rather than a {@code toLowerCase()} without a {@code Locale}, and the
   * usual objection to it - that it only approximates a filesystem's Unicode case-folding, so a
   * dotless i or a sharp s could fold differently - cannot arise here (review on PR #8186): BOTH
   * operands are outputs of {@link #sanitizeFilename(String)}, which rewrites every character outside
   * {@code [a-zA-Z0-9_-]} to {@code '_'}. The comparison therefore only ever runs on ASCII, where
   * {@code equalsIgnoreCase}'s folding is exact and locale-independent.
   * <p>
   * That comparison is deliberately filesystem-blind, as it has been since #8126: on a case-SENSITIVE
   * filesystem {@code chats/Alice} and {@code chats/alice} really are two directories and the refusal
   * costs each account an automatic migration it could have had. Refusing leaves both directories
   * intact on disk for an operator to move by hand; the other way round leaks one account's chats to
   * the other, so the conservative answer is the one worth being wrong with.
   */
  private LegacyNameOwnership legacyNameOwnership(final String legacyName, final String username) {
    if (knownUsernames == null)
      return LegacyNameOwnership.UNKNOWN;

    final Set<String> accounts;
    try {
      accounts = knownUsernames.get();
    } catch (final Exception e) {
      return LegacyNameOwnership.UNKNOWN;
    }
    if (accounts == null || !accounts.contains(username))
      return LegacyNameOwnership.UNKNOWN;

    for (final String account : accounts)
      if (!account.equals(username) && legacyName.equalsIgnoreCase(sanitizeFilename(account)))
        return LegacyNameOwnership.COLLISION;

    return LegacyNameOwnership.SOLE;
  }

  /**
   * Whether the registered-account list refuses the migration of {@code legacyName} onto
   * {@code username}'s hashed store.
   *
   * <p>Consulted for every legacy name. Before #8154 it ran only when {@code legacyName} contained
   * {@code '_'}, on the reasoning that {@code sanitizeFilename} only ever rewrites a character TO
   * {@code '_'} so a name without one has a single preimage. True of the function, false of the
   * filesystem: {@code Alice} and {@code alice} each have a single preimage and still share one
   * directory wherever names are compared case-insensitively, which left the case-insensitive
   * comparison in {@link #legacyNameOwnership} unreachable for the commonest shape of username.
   *
   * <p>{@link LegacyNameOwnership#UNKNOWN} is where the old underscore test survives, as the
   * fallback it always was rather than as a gate in front of the real check: with no account list to
   * consult there is nothing better to go on, so a name with {@code '_'} is refused on the
   * theoretical preimage alone (the pre-#8078 behaviour every single-argument-constructor caller
   * gets) and a name without one is claimed. A case-folded twin cannot be detected in that state at
   * all - it is a fact about the account registry, not about the file tree - so the residual exposure
   * is a caller that supplies no accounts on a case-insensitive filesystem. {@code HttpServer}, the
   * only thing that constructs a {@code ChatStorage} outside tests, supplies them.
   */
  private boolean migrationRefusedByAccountList(final String legacyName, final String username, final File ambiguityMarker) {
    // Exhaustive over the enum with no default branch, deliberately (review on PR #8186): SOLE is the
    // fail-OPEN answer here, so a fourth LegacyNameOwnership value must not be able to inherit it by
    // falling through. Without a default this stops compiling instead, which is the loudest a future
    // change to this security control can be told to come back and decide.
    return switch (legacyNameOwnership(legacyName, username)) {
      case COLLISION -> {
        // Recorded here rather than inside the classifier, so the one side effect on this path sits at
        // the point that acts on the answer instead of hiding behind a query.
        markPermanentlyAmbiguous(ambiguityMarker, legacyName);
        yield true;
      }
      case UNKNOWN -> legacyName.indexOf('_') >= 0;
      case SOLE -> false;
    };
  }

  /**
   * The marker {@link #migrationRefusedByAccountList} writes the instant it proves two real accounts collide on
   * {@code legacyName}, and {@link #migrateLegacyDirectoryIfPresent} checks before ever consulting the
   * current account list again. An empty file is enough: its only meaning is that it exists. Named from
   * {@code legacyName} rather than kept in memory (contrast {@link #reportedLegacyDirectories}, which is
   * purely a once-per-name log gate) because it has to survive a server restart - the whole point is to
   * outlive the account whose later deletion would otherwise make the collision invisible again.
   * <p>
   * Two colliding usernames' first requests can race and both attempt the write; harmless, since
   * {@link #markPermanentlyAmbiguous} writes an empty file whose only meaning is that it exists, so a
   * double write says nothing a single one did not already say.
   */
  private File ambiguityMarkerFile(final String legacyName) {
    return Paths.get(rootPath, "chats", "." + legacyName + ".ambiguous-migration").toFile();
  }

  private void markPermanentlyAmbiguous(final File marker, final String legacyName) {
    if (marker.exists())
      return;
    try {
      FileUtils.writeFile(marker, "");
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not record legacy chat directory '%s' as permanently ambiguous: %s. A later deletion of one of "
              + "the colliding accounts could make the collision look resolved.", legacyName, e.getMessage());
    }
  }

  /**
   * Whether {@code legacyDir} is really the entry named {@code legacyName}, rather than one whose
   * name merely matches it under the filesystem's own comparison.
   *
   * <p>{@link File#exists()} asks the filesystem, and NTFS and the default macOS APFS/HFS+
   * configuration compare names case-insensitively, so {@code chats/Alice} "exists" whenever
   * {@code chats/alice} does. Two user names differing only in case hash to two different, correct
   * directories, but sanitize to two spellings of one legacy directory. Comparing against the
   * parent's own listing is the only portable way to ask what the entry is actually called.
   *
   * <p>This is a ONE-SIDED test, and #8154 is what came of reading it as more: it refuses the
   * account whose spelling differs from the entry's real name, and says nothing at all about the
   * account whose spelling matches it - which migrates the shared directory, the other account's
   * chats included, no matter who was looked up first. Order never entered into it. What actually
   * decides a case-folded collision is {@link #migrationRefusedByAccountList}, which runs before
   * this and asks the account registry; this check's own job is narrower, and is the reason a
   * refused-by-registry pair does not get one of its two spellings quietly re-admitted here.
   *
   * <p>It is O(entries in {@code chats/}) rather than O(1), which is why it is placed last of the
   * three checks: only a candidate that has already passed the other two reaches it. Where the
   * migration then succeeds the cost is paid once, because the hashed directory now exists and every
   * later lookup returns at the top of {@code migrateLegacyDirectoryIfPresent}. Where this check is
   * what refuses the move, though, nothing changes on disk, so it runs again on every request from
   * that user until an operator resolves the directory - a flat per-server listing each time, not a
   * one-off.
   *
   * <p>Package-private rather than private so the comparison can be exercised directly: reaching it
   * through the migration needs a case-insensitive filesystem, and CI runs on a case-sensitive one.
   */
  static boolean isSpelledExactlyOnDisk(final File legacyDir, final String legacyName) {
    final String[] entries = legacyDir.getParentFile().list();
    if (entries == null)
      return false;
    for (final String entry : entries)
      if (entry.equals(legacyName))
        return true;
    return false;
  }

  /**
   * Reports a refused migration once per legacy directory name rather than once per request: the
   * refusal is re-evaluated on every read for a user who never writes, and an operator needs the
   * message once, not in a loop.
   *
   * <p>The set's SIZE is bounded - an entry is only ever added for a directory that exists on disk,
   * and every directory this class creates is hash-named, so an API caller cannot grow it. That
   * bound assumes nothing else writes arbitrary directory names under {@code chats/}, which is true
   * today; a feature that let an admin or an import job create named directories there would have to
   * revisit it. Its CONTENTS go
   * stale: entries are never removed, so a name whose directory an operator has since resolved stays
   * marked as reported for the life of the server. That costs a log line, not a decision.
   *
   * @param extraArgs further {@code %s} arguments beyond {@code legacyName}, which is always the first.
   */
  private void warnOncePerLegacyDirectory(final String legacyName, final String message, final String... extraArgs) {
    if (!reportedLegacyDirectories.add(legacyName))
      return;
    final Object[] args = new Object[extraArgs.length + 1];
    args[0] = legacyName;
    System.arraycopy(extraArgs, 0, args, 1, extraArgs.length);
    LogManager.instance().log(this, Level.WARNING, message, args);
  }

  private File getChatFile(final String username, final String chatId) {
    return new File(getUserDir(username), sanitizeFilename(chatId) + ".json");
  }

  /**
   * Sanitizes a string for use as a filename, preventing path traversal.
   */
  static String sanitizeFilename(final String input) {
    if (input == null || input.isEmpty())
      return "default";
    return input.replaceAll("[^a-zA-Z0-9_\\-]", "_");
  }

  /**
   * Maps a username to its chat-store directory name.
   *
   * <p>{@link #sanitizeFilename(String)} maps every character outside {@code [a-zA-Z0-9_-]} to
   * {@code '_'}, which is not injective: usernames are essentially unconstrained (only non-blank is
   * enforced), so e.g. {@code user@corp.com}, {@code user.corp.com} and {@code user_corp_com} all
   * sanitize to the same string. Using that as the directory identity let two distinct users share
   * one chat store, each able to read and delete the other's chats. A SHA-256 hex digest of the
   * username is collision-resistant and is itself already filename-safe, so it is used as the
   * identity directly rather than sanitized.
   */
  static String hashUsername(final String username) {
    final String normalized = username == null || username.isEmpty() ? "default" : username;
    try {
      final byte[] hash = MessageDigest.getInstance("SHA-256").digest(normalized.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is a JCE-mandated algorithm on every JVM; unreachable in practice.
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
