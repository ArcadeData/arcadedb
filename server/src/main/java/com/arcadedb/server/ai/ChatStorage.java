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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * File-based storage for AI chat conversations.
 * Chats are stored as JSON files under {serverRoot}/chats/{username}/.
 */
public class ChatStorage {
  // Fixed stripe of locks giving concurrent writers to the same (user, chatId) a deterministic
  // last-writer-wins ordering. The "never a partial/spliced file" guarantee actually comes from
  // atomicWriteFile()'s atomic rename, not from this lock; the read-modify-write cycle in the
  // handler reads outside the lock, so this stripe does not prevent lost updates. Sized as a power
  // of two so the index masks cleanly; collisions across unrelated chats are harmless (they wait).
  private static final int            LOCK_STRIPES = 64;
  private final        ReentrantLock[] writeLocks   = new ReentrantLock[LOCK_STRIPES];

  private final String rootPath;

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
    return Paths.get(rootPath, "chats", sanitizeFilename(username)).toFile();
  }

  private File getChatFile(final String username, final String chatId) {
    return Paths.get(rootPath, "chats", sanitizeFilename(username), sanitizeFilename(chatId) + ".json").toFile();
  }

  /**
   * Sanitizes a string for use as a filename, preventing path traversal.
   */
  static String sanitizeFilename(final String input) {
    if (input == null || input.isEmpty())
      return "default";
    return input.replaceAll("[^a-zA-Z0-9_\\-]", "_");
  }
}
