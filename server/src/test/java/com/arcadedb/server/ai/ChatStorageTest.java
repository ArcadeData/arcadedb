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
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    userMsg.put("timestamp", Instant.now().toString());
    messages.put(userMsg);

    final JSONObject assistantMsg = new JSONObject();
    assistantMsg.put("role", "assistant");
    assistantMsg.put("content", "Hello! How can I help?");
    assistantMsg.put("timestamp", Instant.now().toString());
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
  void concurrentSavesNeverCorruptTheChatFile() throws Exception {
    // Writers repeatedly persist a growing chat while readers repeatedly load it. With the atomic,
    // per-(user, chatId) serialized write, a reader must always observe a complete, valid JSON file
    // (never null / partial), and the final file must be readable.
    final JSONObject chat = ChatStorage.createNewChat("db", "Concurrent chat");
    final String chatId = chat.getString("id");
    chatStorage.saveChat("root", chat);

    final int threads = 8;
    final int iterations = 200;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean corruption = new AtomicBoolean(false);
    final AtomicReference<Throwable> failure = new AtomicReference<>();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations; i++) {
            if (threadId % 2 == 0) {
              final JSONObject copy = new JSONObject(chat.toString());
              final JSONArray messages = copy.getJSONArray("messages");
              final JSONObject msg = new JSONObject();
              msg.put("role", "user");
              msg.put("content", "message " + threadId + "-" + i);
              messages.put(msg);
              copy.put("updated", Instant.now().toString());
              chatStorage.saveChat("root", copy);
            } else {
              final JSONObject loaded = chatStorage.getChat("root", chatId);
              // A partial/spliced write would surface as null (unparseable) or a wrong id.
              if (loaded == null || !chatId.equals(loaded.getString("id", "")))
                corruption.set(true);
            }
          }
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

    assertThat(failure.get()).isNull();
    assertThat(corruption.get()).isFalse();

    // Final file is still valid and complete.
    final JSONObject finalChat = chatStorage.getChat("root", chatId);
    assertThat(finalChat).isNotNull();
    assertThat(finalChat.getString("id")).isEqualTo(chatId);
  }

  @Test
  void saveChatPropagatesWriteFailure() {
    // Place a regular file where the user's chat directory should be so the write cannot create the
    // target directory. The failure must surface (not be silently swallowed as a false success).
    final File chatsDir = Path.of(TEST_ROOT, "chats").toFile();
    chatsDir.mkdirs();
    final File blocker = Path.of(TEST_ROOT, "chats", "blockeduser").toFile();
    assertThat(writeEmptyFile(blocker)).isTrue();

    final JSONObject chat = ChatStorage.createNewChat("db", "Will fail");

    assertThatThrownBy(() -> chatStorage.saveChat("blockeduser", chat))
        .isInstanceOf(IllegalStateException.class);
  }

  private static boolean writeEmptyFile(final File file) {
    try {
      FileUtils.writeFile(file, "");
      return file.isFile();
    } catch (final Exception e) {
      return false;
    }
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
