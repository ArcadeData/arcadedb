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

  // Legacy directories whose refused migration has already been reported, so the operator gets the
  // message once per directory instead of once per request.
  private final Set<String> reportedLegacyDirectories = ConcurrentHashMap.newKeySet();

  public ChatStorage(final String rootPath) {
    this.rootPath = rootPath;
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
   * <li><b>A name with the shape of a hashed store</b> ({@code [0-9a-f]{64}}) is never moved.
   * {@link #sanitizeFilename(String)} leaves hex digits alone, and {@link #hashUsername(String)}
   * emits exactly 64 lowercase hex characters, so a user who names themselves another user's digest
   * would otherwise have the victim's current, live directory moved into their own on first access.
   * This needs no legacy layout to have ever existed - it is reachable on a fresh install.</li>
   * <li><b>A name more than one username could have produced</b> is never moved. Because
   * {@code sanitizeFilename} only ever rewrites a character <i>to</i> {@code '_'}, a name containing
   * no {@code '_'} has exactly one preimage - itself - and is safe to claim; a name containing one
   * has infinitely many ({@code user@corp.com}, {@code user.corp.com} and {@code user_corp_com} all
   * produced {@code user_corp_com}). Awarding such a shared directory to whichever of them is looked
   * up first would hand that user read and delete access to the others' chats, which is the
   * cross-user access #7113 set out to remove rather than a fix for it.</li>
   * </ol>
   *
   * <p>Resolving an ambiguous directory needs to know which chat belonged to whom, which is not
   * recoverable from the file tree, so it is left to an operator and reported once per directory.
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

    if (legacyName.indexOf('_') >= 0) {
      warnOncePerLegacyDirectory(legacyName,
          "Refusing to migrate legacy chat directory '%s': more than one user name maps onto it, so its chats cannot be attributed to a "
              + "single user. It has been left untouched - move each chat under the owner's hashed directory by hand to restore it.");
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
   * Whether {@code legacyDir} is really the entry named {@code legacyName}, rather than one whose
   * name merely matches it under the filesystem's own comparison.
   *
   * <p>{@link File#exists()} asks the filesystem, and NTFS and the default macOS APFS/HFS+
   * configuration compare names case-insensitively, so {@code chats/Alice} "exists" whenever
   * {@code chats/alice} does. Two user names differing only in case hash to two different, correct
   * directories, but sanitize to two spellings of one legacy directory - and whichever of them is
   * looked up first would otherwise migrate the other's chats. Comparing against the parent's own
   * listing is the only portable way to ask what the entry is actually called.
   *
   * <p>Runs only for a candidate that has already passed the other two checks and is about to be
   * moved, so the listing is off the hot path: a successful migration makes the hashed directory
   * exist, and every later lookup returns before reaching here.
   */
  private static boolean isSpelledExactlyOnDisk(final File legacyDir, final String legacyName) {
    final String[] entries = legacyDir.getParentFile().list();
    if (entries == null)
      return false;
    for (final String entry : entries)
      if (entry.equals(legacyName))
        return true;
    return false;
  }

  /**
   * Reports a refused migration once per legacy directory rather than once per request: the refusal
   * is re-evaluated on every read for a user who never writes, and an operator needs the message
   * once, not in a loop. The set is bounded by the number of legacy directories that exist on disk -
   * nothing creates one any more, so it cannot be grown by a caller.
   */
  private void warnOncePerLegacyDirectory(final String legacyName, final String message) {
    if (reportedLegacyDirectories.add(legacyName))
      LogManager.instance().log(this, Level.WARNING, message, legacyName);
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
