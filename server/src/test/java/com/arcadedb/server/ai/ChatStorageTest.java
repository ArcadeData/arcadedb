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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChatStorageTest {
  private static final String TEST_ROOT = "target/test-chat-storage";
  private ChatStorage          chatStorage;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(TEST_ROOT));
    chatStorage = new ChatStorage(TEST_ROOT);
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_ROOT));
  }

  @Test
  void createAndRetrieveChat() {
    final JSONObject chat = ChatStorage.createNewChat("testdb", "Test chat");
    assertThat(chat.getString("id")).isNotEmpty();
    assertThat(chat.getString("title")).isEqualTo("Test chat");
    assertThat(chat.getString("database")).isEqualTo("testdb");
    assertThat(chat.getJSONArray("messages").length()).isZero();

    chatStorage.saveChat("root", chat);

    final JSONObject loaded = chatStorage.getChat("root", chat.getString("id"));
    assertThat(loaded).isNotNull();
    assertThat(loaded.getString("id")).isEqualTo(chat.getString("id"));
    assertThat(loaded.getString("title")).isEqualTo("Test chat");
  }

  @Test
  void listChats() {
    final JSONObject chat1 = ChatStorage.createNewChat("db1", "First chat");
    final JSONObject chat2 = ChatStorage.createNewChat("db2", "Second chat");

    chatStorage.saveChat("root", chat1);
    chatStorage.saveChat("root", chat2);

    final List<JSONObject> chats = chatStorage.listChats("root");
    assertThat(chats).hasSize(2);

    // Verify metadata only (no messages)
    for (final JSONObject meta : chats) {
      assertThat(meta.has("id")).isTrue();
      assertThat(meta.has("title")).isTrue();
      assertThat(meta.has("messages")).isFalse();
    }
  }

  @Test
  void listChatsReturnsEmptyForNonExistentUser() {
    final List<JSONObject> chats = chatStorage.listChats("nonexistent");
    assertThat(chats).isEmpty();
  }

  @Test
  void deleteChat() {
    final JSONObject chat = ChatStorage.createNewChat("testdb", "To delete");
    chatStorage.saveChat("root", chat);

    assertThat(chatStorage.getChat("root", chat.getString("id"))).isNotNull();

    final boolean deleted = chatStorage.deleteChat("root", chat.getString("id"));
    assertThat(deleted).isTrue();
    assertThat(chatStorage.getChat("root", chat.getString("id"))).isNull();
  }

  @Test
  void deleteNonExistentChat() {
    final boolean deleted = chatStorage.deleteChat("root", "nonexistent-id");
    assertThat(deleted).isFalse();
  }

  @Test
  void chatWithMessages() {
    final JSONObject chat = ChatStorage.createNewChat("testdb", "Chat with messages");
    final JSONArray messages = chat.getJSONArray("messages");

    final JSONObject userMsg = new JSONObject();
    userMsg.put("role", "user");
    userMsg.put("content", "Hello AI");
    userMsg.put("timestamp", java.time.Instant.now().toString());
    messages.put(userMsg);

    final JSONObject assistantMsg = new JSONObject();
    assistantMsg.put("role", "assistant");
    assistantMsg.put("content", "Hello! How can I help?");
    assistantMsg.put("timestamp", java.time.Instant.now().toString());
    messages.put(assistantMsg);

    chatStorage.saveChat("root", chat);

    final JSONObject loaded = chatStorage.getChat("root", chat.getString("id"));
    assertThat(loaded.getJSONArray("messages").length()).isEqualTo(2);
    assertThat(loaded.getJSONArray("messages").getJSONObject(0).getString("role")).isEqualTo("user");
    assertThat(loaded.getJSONArray("messages").getJSONObject(1).getString("role")).isEqualTo("assistant");
  }

  @Test
  void generateTitle() {
    assertThat(ChatStorage.generateTitle("Short title")).isEqualTo("Short title");
    assertThat(ChatStorage.generateTitle("This is a very long message that should be truncated to forty characters"))
        .hasSize(40);
    assertThat(ChatStorage.generateTitle(null)).isEqualTo("Untitled");
    assertThat(ChatStorage.generateTitle("")).isEqualTo("Untitled");
  }

  @Test
  void sanitizeFilename() {
    assertThat(ChatStorage.sanitizeFilename("root")).isEqualTo("root");
    assertThat(ChatStorage.sanitizeFilename("user@domain.com")).isEqualTo("user_domain_com");
    assertThat(ChatStorage.sanitizeFilename("../../../etc/passwd")).isEqualTo("_________etc_passwd");
    assertThat(ChatStorage.sanitizeFilename(null)).isEqualTo("default");
    assertThat(ChatStorage.sanitizeFilename("")).isEqualTo("default");
  }

  @Test
  void userIsolation() {
    final JSONObject chat1 = ChatStorage.createNewChat("db1", "Root's chat");
    final JSONObject chat2 = ChatStorage.createNewChat("db1", "John's chat");

    chatStorage.saveChat("root", chat1);
    chatStorage.saveChat("john", chat2);

    assertThat(chatStorage.listChats("root")).hasSize(1);
    assertThat(chatStorage.listChats("john")).hasSize(1);
    assertThat(chatStorage.getChat("root", chat2.getString("id"))).isNull();
    assertThat(chatStorage.getChat("john", chat1.getString("id"))).isNull();
  }
}
