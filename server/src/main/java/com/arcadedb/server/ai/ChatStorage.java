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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

/**
 * File-based storage for AI chat conversations.
 * Chats are stored as JSON files under {serverRoot}/chats/{username}/.
 */
public class ChatStorage {
  private final String rootPath;

  public ChatStorage(final String rootPath) {
    this.rootPath = rootPath;
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
   */
  public void saveChat(final String username, final JSONObject chat) {
    final File userDir = getUserDir(username);
    if (!userDir.exists())
      userDir.mkdirs();

    final String chatId = chat.getString("id");
    final File file = getChatFile(username, chatId);

    try (final OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
      writer.write(chat.toString(2));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error saving chat %s: %s", chatId, e.getMessage());
    }
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
    final String now = java.time.Instant.now().toString();
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
