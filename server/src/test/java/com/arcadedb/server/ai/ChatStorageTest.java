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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ChatStorageTest {
  private static final String TEST_ROOT = "target/test-chat-storage";

  // Issue #8340: whether two spellings are one directory is now asked of the filesystem. These two
  // stand-ins answer as a case-insensitive (NTFS, default macOS APFS/HFS+) and a case-sensitive (ext4,
  // XFS) filesystem would, so a test pins the answer it depends on instead of inheriting whatever the
  // machine running it happens to do - CI is case-sensitive, a developer's Mac is not.
  private static final BiPredicate<File, File> FOLDS_CASE        = (dir, other) -> dir.exists()
      && dir.getParentFile().equals(other.getParentFile()) && dir.getName().equalsIgnoreCase(other.getName());
  private static final BiPredicate<File, File> KEEPS_CASE_APART = (dir, other) -> false;
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
    final File chatsDir = Paths.get(TEST_ROOT, "chats").toFile();
    chatsDir.mkdirs();
    final File blocker = Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername("blockeduser")).toFile();
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

  @Test
  void hashUsername() {
    // Deterministic, filename-safe (hex), and full-width (SHA-256 = 64 hex chars).
    assertThat(ChatStorage.hashUsername("root")).isEqualTo(ChatStorage.hashUsername("root"));
    assertThat(ChatStorage.hashUsername("root")).matches("[0-9a-f]{64}");
    assertThat(ChatStorage.hashUsername(null)).isEqualTo(ChatStorage.hashUsername(""));
  }

  @Test
  void usersWhoseNamesCollideUnderSanitizeFilenameGetSeparateChatStores() {
    // Regression test for #7113: sanitizeFilename maps every character outside [a-zA-Z0-9_-] to
    // '_', which is not injective. These three usernames all used to sanitize to "user_corp_com" and
    // therefore shared one chat store; each could list, read and delete the others' chats.
    final String[] collidingUsernames = { "user@corp.com", "user.corp.com", "user_corp_com" };
    for (final String username : collidingUsernames)
      assertThat(ChatStorage.sanitizeFilename(username)).isEqualTo("user_corp_com");

    for (final String username : collidingUsernames) {
      final JSONObject chat = ChatStorage.createNewChat("db", "Chat for " + username);
      chatStorage.saveChat(username, chat);
    }

    for (final String username : collidingUsernames)
      assertThat(chatStorage.listChats(username)).as("chats visible to %s", username).hasSize(1);
  }

  @Test
  void chatsWrittenUnderTheLegacySanitizeFilenameLayoutSurviveTheUpgrade() throws Exception {
    // Regression test for the CodeRabbit-flagged migration gap in #7113's fix: a server that already
    // shipped the old ChatStorage.sanitizeFilename(username) directory layout must not orphan a
    // user's existing chats when it upgrades to the new hashUsername(username) layout.
    final File legacyDir = Paths.get(TEST_ROOT, "chats", ChatStorage.sanitizeFilename("legacyuser")).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Pre-upgrade chat");
    final File legacyFile = new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json");
    FileUtils.writeFile(legacyFile, chat.toString());

    // No account list, so on a case-FOLDING filesystem an unseen "LegacyUser" could share the directory
    // and the migration is refused (#8340); this test is about the ordinary migration, so it pins a
    // filesystem that keeps spellings apart.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, null, KEEPS_CASE_APART);
    final List<JSONObject> chats = storage.listChats("legacyuser");

    assertThat(chats).hasSize(1);
    assertThat(storage.getChat("legacyuser", chat.getString("id"))).isNotNull();
    // The legacy directory itself must be gone: this user's data now lives only under the hash.
    assertThat(legacyDir).doesNotExist();
  }

  @Test
  void legacyMigrationCannotReproduceTheSanitizeFilenameCollision() throws Exception {
    // Two usernames that used to collide under sanitizeFilename must not both end up reading the
    // migrated legacy directory.
    //
    // Updated for #7620: the original assertion here was that the FIRST of the two looked up after
    // the upgrade claims the shared directory by renaming it away. That is not a fix, it is a
    // one-directional version of the same cross-user access #7113 reported - the winner gains read
    // and delete access to every chat the loser ever wrote. An ambiguous legacy directory is now
    // claimed by nobody and left on disk for an operator to split by hand.
    final String sharedLegacyName = ChatStorage.sanitizeFilename("user@corp.com");
    assertThat(sharedLegacyName).isEqualTo(ChatStorage.sanitizeFilename("user.corp.com"));

    final File legacyDir = Paths.get(TEST_ROOT, "chats", sharedLegacyName).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Whoever migrates first owns this");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    // Neither colliding user may read the other's history through the migration.
    assertThat(chatStorage.listChats("user@corp.com")).isEmpty();
    assertThat(chatStorage.listChats("user.corp.com")).isEmpty();
    // ...and neither may destroy it: the ambiguous directory is still on disk, untouched.
    assertThat(legacyDir).exists();
    assertThat(chatStorage.getChat("user@corp.com", chat.getString("id"))).isNull();
    assertThat(chatStorage.getChat("user.corp.com", chat.getString("id"))).isNull();
  }

  @Test
  void anAmbiguousLegacyDirectoryIsNeverClaimedByAnyOfTheUsersThatCouldHaveProducedIt() throws Exception {
    // Regression test for #7620, finding 1. sanitizeFilename turns every character outside
    // [a-zA-Z0-9_-] into '_', so any legacy directory name containing an underscore has an infinite
    // preimage: "user_corp_com" could have been written by "user@corp.com", "user.corp.com",
    // "user_corp_com" and countless others. Awarding it to whoever is looked up first hands that
    // user the others' chats.
    final String sharedLegacyName = ChatStorage.sanitizeFilename("user@corp.com");
    final File legacyDir = Paths.get(TEST_ROOT, "chats", sharedLegacyName).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Belongs to one of them, we cannot tell which");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    // Even the user whose name is byte-identical to the legacy directory name gets nothing: that
    // name is still the image of every other username that sanitizes onto it.
    for (final String username : new String[] { "user@corp.com", "user.corp.com", "user_corp_com" }) {
      assertThat(chatStorage.listChats(username)).as("chats visible to %s", username).isEmpty();
      assertThat(chatStorage.deleteChat(username, chat.getString("id"))).as("%s can delete", username).isFalse();
    }

    // Nothing was moved and nothing was deleted: the history is intact for an operator to resolve.
    assertThat(legacyDir).exists();
    assertThat(legacyDir.listFiles()).hasSize(1);
  }

  @Test
  void aUsernameShapedLikeAHashCannotListAnotherUsersLiveChatStore() {
    // Regression test for #7620, finding 2. A 64-character lowercase hex username passes through
    // sanitizeFilename byte-identical, and hashUsername emits exactly that shape, so the legacy
    // directory the migration looks for IS the victim's current, live hashed directory. This needs
    // no legacy layout to have ever existed: it is reachable on a fresh install.
    final String victim = "victim@corp.com";
    final String attacker = ChatStorage.hashUsername(victim);
    assertThat(attacker).matches("[0-9a-f]{64}");
    assertThat(ChatStorage.sanitizeFilename(attacker)).isEqualTo(attacker);

    final JSONObject victimChat = ChatStorage.createNewChat("db", "Private");
    chatStorage.saveChat(victim, victimChat);
    final File victimDir = Paths.get(TEST_ROOT, "chats", attacker).toFile();
    assertThat(victimDir).exists();

    assertThat(chatStorage.listChats(attacker)).isEmpty();
    // The victim's store was not moved out from under them.
    assertThat(victimDir).exists();
    assertThat(chatStorage.listChats(victim)).hasSize(1);
  }

  @Test
  void aUsernameShapedLikeAHashCannotReadAnotherUsersChatById() {
    // #7620 finding 2, driven through getChat() rather than listChats(): getChat resolves the user
    // directory through the same getUserDir() choke point, so it is its own entry point into the
    // migration and gets its own test.
    final String victim = "victim@corp.com";
    final String attacker = ChatStorage.hashUsername(victim);

    final JSONObject victimChat = ChatStorage.createNewChat("db", "Private");
    chatStorage.saveChat(victim, victimChat);

    assertThat(chatStorage.getChat(attacker, victimChat.getString("id"))).isNull();
    assertThat(chatStorage.getChat(victim, victimChat.getString("id"))).isNotNull();
  }

  @Test
  void aUsernameShapedLikeAHashCannotDeleteAnotherUsersChat() {
    // #7620 finding 2, driven through deleteChat(). Pre-fix this both moved the victim's whole
    // store into the attacker's and then deleted a file out of it.
    final String victim = "victim@corp.com";
    final String attacker = ChatStorage.hashUsername(victim);

    final JSONObject victimChat = ChatStorage.createNewChat("db", "Private");
    chatStorage.saveChat(victim, victimChat);

    assertThat(chatStorage.deleteChat(attacker, victimChat.getString("id"))).isFalse();
    assertThat(chatStorage.getChat(victim, victimChat.getString("id"))).isNotNull();
  }

  @Test
  void aUsernameShapedLikeAHashWritesIntoItsOwnStoreNotTheVictimsOne() {
    // #7620 finding 2, driven through saveChat(). The attacker must still get a working, private
    // chat store of their own - refusing the migration may not break the hash-shaped username.
    final String victim = "victim@corp.com";
    final String attacker = ChatStorage.hashUsername(victim);

    final JSONObject victimChat = ChatStorage.createNewChat("db", "Victim private");
    chatStorage.saveChat(victim, victimChat);

    final JSONObject attackerChat = ChatStorage.createNewChat("db", "Attacker own");
    chatStorage.saveChat(attacker, attackerChat);

    assertThat(chatStorage.listChats(attacker)).hasSize(1);
    assertThat(chatStorage.listChats(attacker).getFirst().getString("title")).isEqualTo("Attacker own");
    assertThat(chatStorage.listChats(victim)).hasSize(1);
    assertThat(chatStorage.listChats(victim).getFirst().getString("title")).isEqualTo("Victim private");
    // The two stores are distinct directories: the attacker's own hash, not the name they chose -
    // which is the name of the victim's directory, so assert the two paths really do differ.
    final File attackerDir = Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername(attacker)).toFile();
    final File victimDir = Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername(victim)).toFile();
    assertThat(attackerDir).exists();
    assertThat(victimDir).exists();
    assertThat(attackerDir).isNotEqualTo(victimDir);
  }

  @Test
  void aUsernameSpellingAnotherUsersHashInUpperCaseCannotTakeTheirLiveChatStore() {
    // #7620, the case-insensitive-filesystem variant of finding 2, found while reviewing the fix for
    // it. hashUsername emits lower-case hex, so an UPPER-case spelling of a victim's digest is not
    // itself a hash-shaped name under a case-sensitive test and carries no underscore - it clears
    // both of the other two guards. On NTFS and on the default macOS APFS/HFS+ configuration
    // "chats/ABC..." and "chats/abc..." are the same directory, so exists() would find the victim's
    // live store and Files.move would take it.
    final String victim = "victim@corp.com";
    final String attacker = ChatStorage.hashUsername(victim).toUpperCase();
    assertThat(ChatStorage.sanitizeFilename(attacker)).isEqualTo(attacker);
    assertThat(attacker).doesNotContain("_");

    final JSONObject victimChat = ChatStorage.createNewChat("db", "Private");
    chatStorage.saveChat(victim, victimChat);
    final File victimDir = Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername(victim)).toFile();
    assertThat(victimDir).exists();

    // Holds on both kinds of filesystem: where the lookup resolves onto the victim's directory the
    // guards refuse it, and where it does not there was never anything to migrate.
    assertThat(chatStorage.listChats(attacker)).isEmpty();
    assertThat(chatStorage.getChat(attacker, victimChat.getString("id"))).isNull();
    assertThat(chatStorage.deleteChat(attacker, victimChat.getString("id"))).isFalse();
    assertThat(victimDir).exists();
    assertThat(chatStorage.listChats(victim)).hasSize(1);
  }

  @Test
  void aLegacyDirectorySpelledDifferentlyOnDiskIsNotMigrated() throws Exception {
    // #7620: two user names differing only in case hash to two different, correct directories, but
    // sanitize to two spellings of ONE legacy directory. On a case-insensitive filesystem whichever
    // is looked up first would otherwise migrate the other's chats away.
    final File legacyDir = Paths.get(TEST_ROOT, "chats", "alice").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "alice's pre-upgrade chat");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    // Pinned to a filesystem whose sameDirectory probe keeps spellings apart, so that without an account
    // list neither lookup is refused up front on a possible case-folded twin (#8340) and this test keeps
    // reaching the guard it names: isSpelledExactlyOnDisk.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, null, KEEPS_CASE_APART);

    // "Alice" is a different user from "alice" and must not receive alice's history, however the
    // filesystem happens to compare the two names.
    assertThat(storage.listChats("Alice")).isEmpty();
    assertThat(Paths.get(TEST_ROOT, "chats", "alice").toFile().list()).hasSize(1);

    // ...while "alice" herself, whose spelling does match the entry on disk, still migrates.
    assertThat(storage.listChats("alice")).hasSize(1);
  }

  @Test
  void anUpperCaseHexLegacyDirectoryIsRefusedOnEveryFilesystem() throws Exception {
    // Companion to aUsernameSpellingAnotherUsersHashInUpperCase...: that test can only reach the
    // HASHED_DIR_NAME check on a case-insensitive filesystem, and CI runs on a case-sensitive one,
    // where it returns at "!legacyDir.exists()" and passes because there was nothing to migrate.
    // This one creates a directory literally named with 64 UPPER-case hex characters, so exists()
    // hits it for real whatever the filesystem does with case, and the refusal can only come from
    // HASHED_DIR_NAME matching case-insensitively. It fails against a lower-case-only pattern.
    final String upperHexName = "A1B2C3D4E5F60718293A4B5C6D7E8F90A1B2C3D4E5F60718293A4B5C6D7E8F90";
    assertThat(upperHexName).hasSize(64);
    assertThat(ChatStorage.sanitizeFilename(upperHexName)).isEqualTo(upperHexName);

    final File legacyDir = Paths.get(TEST_ROOT, "chats", upperHexName).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Must not be claimed");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());
    assertThat(legacyDir).exists();

    assertThat(chatStorage.listChats(upperHexName)).isEmpty();
    // Refused, not consumed: the directory is still there under its original name.
    assertThat(legacyDir).exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername(upperHexName)).toFile()).doesNotExist();
  }

  @Test
  void isSpelledExactlyOnDiskComparesAgainstTheParentListingNotTheFilesystemsOwnComparison() throws Exception {
    // Direct unit test of the third guard. Reaching it through migrateLegacyDirectoryIfPresent needs
    // a case-insensitive filesystem; comparing against the parent's listing is deterministic on both
    // kinds, so the comparison itself can be exercised anywhere.
    final File chatsDir = Paths.get(TEST_ROOT, "chats").toFile();
    assertThat(new File(chatsDir, "alice").mkdirs()).isTrue();

    // The spelling that is really on disk.
    assertThat(ChatStorage.isSpelledExactlyOnDisk(new File(chatsDir, "alice"), "alice")).isTrue();
    // A different spelling of it - what a case-insensitive filesystem would have said exists().
    assertThat(ChatStorage.isSpelledExactlyOnDisk(new File(chatsDir, "Alice"), "Alice")).isFalse();
    assertThat(ChatStorage.isSpelledExactlyOnDisk(new File(chatsDir, "ALICE"), "ALICE")).isFalse();
    // A name nothing on disk matches at all.
    assertThat(ChatStorage.isSpelledExactlyOnDisk(new File(chatsDir, "bob"), "bob")).isFalse();
    // An unreadable/absent parent must not be treated as a match.
    final File missingParent = Paths.get(TEST_ROOT, "chats", "nope", "deeper").toFile();
    assertThat(ChatStorage.isSpelledExactlyOnDisk(missingParent, "deeper")).isFalse();
  }

  @Test
  void anAmbiguousLegacyDirectoryMigratesWhenExactlyOneRegisteredAccountMapsOntoIt() throws Exception {
    // Regression test for #8078: the #7620 fix refused every legacy name containing '_' on the
    // theoretical preimage alone, even when the server's real user registry proves only one account
    // could have written it. Here "john_doe" is the only registered user that sanitizes to
    // "john_doe" - "john.doe" and "john@doe" are not accounts on this server - so the migration must
    // now proceed instead of leaving the history stranded.
    final ChatStorage resolvingStorage = new ChatStorage(TEST_ROOT, () -> Set.of("john_doe", "root"));

    final File legacyDir = Paths.get(TEST_ROOT, "chats", "john_doe").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "john_doe's pre-upgrade chat");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    final List<JSONObject> chats = resolvingStorage.listChats("john_doe");

    assertThat(chats).hasSize(1);
    assertThat(legacyDir).doesNotExist();
    final File hashedDir = Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername("john_doe")).toFile();
    assertThat(hashedDir).exists();
  }

  @Test
  void anAmbiguousLegacyDirectoryStaysRefusedWhenTwoRegisteredAccountsMapOntoIt() throws Exception {
    // Companion to the test above: when the real registry shows the ambiguity is NOT just
    // theoretical - two actual accounts collide - the directory must still be left untouched, exactly
    // as the #7620 fix did with no registry available at all.
    final ChatStorage resolvingStorage = new ChatStorage(TEST_ROOT, () -> Set.of("user@corp.com", "user.corp.com"));
    final String sharedLegacyName = ChatStorage.sanitizeFilename("user@corp.com");
    assertThat(sharedLegacyName).isEqualTo(ChatStorage.sanitizeFilename("user.corp.com"));

    final File legacyDir = Paths.get(TEST_ROOT, "chats", sharedLegacyName).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Whoever migrates first owns this");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    assertThat(resolvingStorage.listChats("user@corp.com")).isEmpty();
    assertThat(resolvingStorage.listChats("user.corp.com")).isEmpty();
    assertThat(legacyDir).exists();
  }

  @Test
  void anAmbiguousDirectoryStaysRefusedEvenAfterOneOfTheTwoCollidingAccountsIsDeleted() throws Exception {
    // Code review on PR #8126: deciding purely from the CURRENT account list is not enough. If
    // "user@corp.com" and "user.corp.com" are both seen to collide on "user_corp_com" (refused,
    // correctly), and "user.corp.com" is deleted afterwards, a naive re-check would see only
    // "user@corp.com" mapping onto the name and migrate the directory - including
    // "user.corp.com"'s chats - straight into "user@corp.com"'s store. That is the exact cross-user
    // access #7113 set out to remove, reached through account deletion instead of a name collision.
    final AtomicReference<Set<String>> registeredAccounts =
        new AtomicReference<>(Set.of("user@corp.com", "user.corp.com"));
    final ChatStorage resolvingStorage = new ChatStorage(TEST_ROOT, registeredAccounts::get);

    final String sharedLegacyName = ChatStorage.sanitizeFilename("user@corp.com");
    final File legacyDir = Paths.get(TEST_ROOT, "chats", sharedLegacyName).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Both accounts existed when this collision was seen");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    // First lookup: both accounts still exist, the collision is real and observed - refused, and the
    // collision is now recorded on disk, not just refused in the moment.
    assertThat(resolvingStorage.listChats("user@corp.com")).isEmpty();
    assertThat(legacyDir).exists();

    // "user.corp.com" is deleted. Taken at face value, the account list now shows exactly one match.
    registeredAccounts.set(Set.of("user@corp.com"));

    // The directory must STILL be refused: the recorded collision outlives the deleted account, on
    // this same ChatStorage instance...
    assertThat(resolvingStorage.listChats("user@corp.com")).isEmpty();
    assertThat(legacyDir).as("must not have been migrated, taking user.corp.com's chats with it").exists();

    // ...and on a FRESH instance too, e.g. after a server restart, since the record is a file on disk
    // rather than in-memory state.
    final ChatStorage afterRestart = new ChatStorage(TEST_ROOT, registeredAccounts::get);
    assertThat(afterRestart.listChats("user@corp.com")).isEmpty();
    assertThat(legacyDir).exists();
  }

  @Test
  void twoAccountsDifferingOnlyByCaseAreTreatedAsACollisionEvenThoughSanitizeFilenameDoesNotFoldCase() throws Exception {
    // CodeRabbit review on PR #8126: ServerSecurity keys accounts by EXACT name, so "John_Doe" and
    // "john_doe" can both be registered - sanitizeFilename does not lowercase either, so a naive
    // case-sensitive comparison in soleKnownAccountName would not see them as colliding. But
    // legacyDir.exists() (the check that gets here at all) is case-INSENSITIVE on the filesystems this
    // class already special-cases (HASHED_DIR_NAME, isSpelledExactlyOnDisk), so "chats/John_Doe" and
    // "chats/john_doe" name the very same on-disk directory: only one of the two accounts could really
    // have owned it, and it cannot be told which from the file tree - the migration must be refused.
    // Pinned to a case-folding filesystem: that is where the two spellings are one directory (#8340).
    final ChatStorage resolvingStorage = new ChatStorage(TEST_ROOT, () -> Set.of("John_Doe", "john_doe"), FOLDS_CASE);

    final File legacyDir = Paths.get(TEST_ROOT, "chats", "john_doe").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Which of the two really wrote this?");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    assertThat(resolvingStorage.listChats("john_doe")).isEmpty();
    assertThat(legacyDir).as("must be left untouched, not awarded to whichever spelling looked up first").exists();
  }

  @Test
  void anAmbiguousLegacyDirectoryStaysRefusedWhenTheKnownUserSupplierIsAbsent() throws Exception {
    // The default, single-argument constructor (used by every pre-#8078 caller and by every other
    // test in this class) must keep today's fully conservative behaviour: no known-user set to
    // consult means the theoretical ambiguity is never resolved.
    final File legacyDir = Paths.get(TEST_ROOT, "chats", "john_doe").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "john_doe's pre-upgrade chat");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    assertThat(chatStorage.listChats("john_doe")).isEmpty();
    assertThat(legacyDir).exists();
  }

  @Test
  void anUnderscoreFreeLegacyDirectoryStillMigratesBecauseOnlyOneUsernameCouldHaveProducedIt() throws Exception {
    // The complement of the #7620 fix: sanitizeFilename only ever rewrites a character TO '_', so a
    // legacy name with no underscore is the image of exactly one string - itself. Those migrations
    // are unambiguous and must keep working, or the fix would orphan every ordinary user's history.
    final String username = "legacyuser";
    assertThat(ChatStorage.sanitizeFilename(username)).doesNotContain("_");

    final File legacyDir = Paths.get(TEST_ROOT, "chats", ChatStorage.sanitizeFilename(username)).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "Pre-upgrade chat");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    // True of sanitizeFilename, and of the directory only where the filesystem keeps case apart: on a
    // case-folding one "LegacyUser" names the same directory, which singleArgument...FoldsCase covers.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, null, KEEPS_CASE_APART);
    assertThat(storage.listChats(username)).hasSize(1);
    assertThat(legacyDir).doesNotExist();
  }

  // ---------------------------------------------------------------------------------------------
  // Issue #8154: the ambiguity guard used to run only when the sanitized legacy name contained an
  // underscore. sanitizeFilename() does not fold case, but the filesystems this class already
  // special-cases do, so "Alice" and "alice" are two accounts, two hashed directories - and ONE
  // legacy directory. The six tests below drive that collision through every public entry point.
  //
  // Since #8340 the collision is only a collision where the filesystem folds case, so these tests pin
  // one with FOLDS_CASE; that keeps them deterministic on the case-SENSITIVE filesystem CI runs on,
  // where creating chats/Alice and looking up "Alice" still reaches the decision. Before #8154,
  // "Alice" passed the underscore gate untested, passed isSpelledExactlyOnDisk (the entry really is
  // spelled "Alice"), and migrated the shared directory - "alice"'s chat included.
  // ---------------------------------------------------------------------------------------------

  /**
   * Seeds chats/Alice with one chat belonging to each of the two case-folded accounts and returns
   * the legacy directory, so each entry-point test below starts from the reported repro's tree.
   */
  private File seedCaseFoldedLegacyDirectory(final JSONObject alicesChat, final JSONObject othersChat) throws Exception {
    final File legacyDir = Paths.get(TEST_ROOT, "chats", "Alice").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(alicesChat.getString("id")) + ".json"),
        alicesChat.toString());
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(othersChat.getString("id")) + ".json"),
        othersChat.toString());
    return legacyDir;
  }

  private static ChatStorage caseFoldedAccountsStorage() {
    return new ChatStorage(TEST_ROOT, () -> Set.of("Alice", "alice"), FOLDS_CASE);
  }

  @Test
  void listChatsRefusesALegacyDirectoryTwoCaseFoldedAccountsMapOntoEvenWithoutAnUnderscore() throws Exception {
    // The reported repro. "Alice" and "alice" both sanitize to themselves - no underscore, one
    // preimage each - yet both name the same legacy directory. Awarding it to "Alice" hands her
    // "alice"'s chat and leaves "alice" with an empty store and no warning.
    final ChatStorage storage = caseFoldedAccountsStorage();
    assertThat(ChatStorage.sanitizeFilename("Alice")).doesNotContain("_");

    final JSONObject alices = ChatStorage.createNewChat("db", "Alice private notes");
    final JSONObject others = ChatStorage.createNewChat("db", "alice OTHER USER notes");
    final File legacyDir = seedCaseFoldedLegacyDirectory(alices, others);

    assertThat(storage.listChats("Alice")).as("must not be handed the other account's chats").isEmpty();
    assertThat(storage.listChats("alice")).isEmpty();
    assertThat(legacyDir).as("left intact for the operator to split by hand").exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername("Alice")).toFile()).doesNotExist();
  }

  @Test
  void getChatRefusesALegacyDirectoryTwoCaseFoldedAccountsMapOnto() throws Exception {
    // getChat() reaches the migration through getChatFile(), not listChats(), so it is its own
    // disclosure path: reading the other account's chat BY ID needs no listing at all.
    final ChatStorage storage = caseFoldedAccountsStorage();
    final JSONObject alices = ChatStorage.createNewChat("db", "Alice private notes");
    final JSONObject others = ChatStorage.createNewChat("db", "alice OTHER USER notes");
    final File legacyDir = seedCaseFoldedLegacyDirectory(alices, others);

    assertThat(storage.getChat("Alice", others.getString("id"))).as("cross-user read").isNull();
    assertThat(storage.getChat("Alice", alices.getString("id"))).isNull();
    assertThat(legacyDir).exists();
  }

  @Test
  void saveChatRefusesALegacyDirectoryTwoCaseFoldedAccountsMapOnto() throws Exception {
    // The write path. A refused migration must still let the account write - into its own, empty
    // hashed store - without dragging the shared legacy directory in behind it.
    final ChatStorage storage = caseFoldedAccountsStorage();
    final JSONObject alices = ChatStorage.createNewChat("db", "Alice private notes");
    final JSONObject others = ChatStorage.createNewChat("db", "alice OTHER USER notes");
    final File legacyDir = seedCaseFoldedLegacyDirectory(alices, others);

    final JSONObject fresh = ChatStorage.createNewChat("db", "Written after the upgrade");
    storage.saveChat("Alice", fresh);

    assertThat(storage.listChats("Alice")).extracting(c -> c.getString("title"))
        .as("only the post-upgrade chat, never the legacy pair").containsExactly("Written after the upgrade");
    assertThat(legacyDir).exists();
    assertThat(legacyDir.list()).hasSize(2);
  }

  @Test
  void deleteChatRefusesALegacyDirectoryTwoCaseFoldedAccountsMapOnto() throws Exception {
    // The destructive path, and the worst of the four: a wrongly migrated directory lets one
    // account delete the other's history outright.
    final ChatStorage storage = caseFoldedAccountsStorage();
    final JSONObject alices = ChatStorage.createNewChat("db", "Alice private notes");
    final JSONObject others = ChatStorage.createNewChat("db", "alice OTHER USER notes");
    final File legacyDir = seedCaseFoldedLegacyDirectory(alices, others);

    assertThat(storage.deleteChat("Alice", others.getString("id"))).as("cross-user delete").isFalse();
    assertThat(legacyDir).exists();
    assertThat(legacyDir.list()).as("both legacy chats untouched").hasSize(2);
  }

  @Test
  void aCaseFoldedCollisionIsRecordedOnDiskSoItSurvivesTheOtherAccountBeingDeleted() throws Exception {
    // The marker half of the gap. The old code only reached ambiguityMarkerFile() for a name with an
    // underscore, so a case-folded collision was never recorded: deleting "alice" afterwards left
    // exactly one account mapping onto chats/Alice, and the next lookup migrated it - "alice"'s chat
    // included. Same escalation-by-deletion as #8126's, reached through case folding instead.
    final AtomicReference<Set<String>> registeredAccounts = new AtomicReference<>(Set.of("Alice", "alice"));
    final ChatStorage storage = new ChatStorage(TEST_ROOT, registeredAccounts::get, FOLDS_CASE);

    final JSONObject alices = ChatStorage.createNewChat("db", "Alice private notes");
    final JSONObject others = ChatStorage.createNewChat("db", "alice OTHER USER notes");
    final File legacyDir = seedCaseFoldedLegacyDirectory(alices, others);

    assertThat(storage.listChats("Alice")).isEmpty();
    final File marker = Paths.get(TEST_ROOT, "chats", ".Alice.ambiguous-migration").toFile();
    assertThat(marker).as("the collision must be recorded, not only refused in the moment").exists();

    // "alice" is deleted. The current account list alone now says the name is unambiguous.
    registeredAccounts.set(Set.of("Alice"));

    assertThat(storage.listChats("Alice")).isEmpty();
    assertThat(legacyDir).exists();

    // ...and across a restart, since the record is a file rather than in-memory state.
    final ChatStorage afterRestart = new ChatStorage(TEST_ROOT, registeredAccounts::get, FOLDS_CASE);
    assertThat(afterRestart.listChats("Alice")).isEmpty();
    assertThat(legacyDir).as("must not have been migrated, taking alice's chat with it").exists();
  }

  @Test
  void anUnderscoreFreeLegacyDirectoryStillMigratesWhenTheAccountListShowsNoCaseFoldedTwin() throws Exception {
    // The guard rail on the fix: consulting the account list for every name must not start refusing
    // the ordinary migration. "bob" is registered alongside accounts that do not collide with it in
    // any casing, so its history must still move under its hash.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("bob", "carol", "root"));

    final File legacyDir = Paths.get(TEST_ROOT, "chats", "bob").toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    final JSONObject chat = ChatStorage.createNewChat("db", "bob's pre-upgrade chat");
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());

    assertThat(storage.listChats("bob")).hasSize(1);
    assertThat(legacyDir).doesNotExist();
    assertThat(Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername("bob")).toFile()).exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ".bob.ambiguous-migration").toFile()).doesNotExist();
  }

  // ---------------------------------------------------------------------------------------------
  // Issue #8340 (#8183 + #8184): the case-folded half of the ambiguity guard is asked of the
  // filesystem instead of assumed. On a filesystem that keeps "Alice" and "alice" apart they are two
  // directories with one owner each and both migrate; where it folds them, the pair is still refused -
  // and with no account list to consult, any name an unseen case-only twin could share is refused too.
  // ---------------------------------------------------------------------------------------------

  private static File seedLegacyDirectory(final String name, final JSONObject chat) throws Exception {
    final File legacyDir = Paths.get(TEST_ROOT, "chats", name).toFile();
    assertThat(legacyDir.mkdirs()).isTrue();
    FileUtils.writeFile(new File(legacyDir, ChatStorage.sanitizeFilename(chat.getString("id")) + ".json"), chat.toString());
    return legacyDir;
  }

  /**
   * Whether the filesystem under {@link #TEST_ROOT} compares names case-sensitively, asked the only
   * reliable way: create one spelling and look for another.
   */
  private static boolean testRootIsCaseSensitive() {
    final File probe = Paths.get(TEST_ROOT, "CaseProbe").toFile();
    assertThat(probe.mkdirs()).isTrue();
    try {
      return !Paths.get(TEST_ROOT, "caseprobe").toFile().exists();
    } finally {
      FileUtils.deleteRecursively(probe);
    }
  }

  @Test
  void caseOnlyTwinsOnAFilesystemThatKeepsThemApartDoNotBlockEachOthersMigration() throws Exception {
    // #8184, the reported regression. On ext4/XFS chats/Alice holds only Alice's chats however many
    // case-spellings of her name are registered, and refusing it cost her history for a collision that
    // does not exist on that volume.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("Alice", "alice"), KEEPS_CASE_APART);
    final JSONObject alices = ChatStorage.createNewChat("db", "Alice's own chat");
    final File legacyDir = seedLegacyDirectory("Alice", alices);

    assertThat(storage.listChats("Alice")).extracting(c -> c.getString("title")).containsExactly("Alice's own chat");
    assertThat(legacyDir).as("migrated under Alice's hash").doesNotExist();
    assertThat(Paths.get(TEST_ROOT, "chats", ".Alice.ambiguous-migration").toFile()).as("no collision to record").doesNotExist();
  }

  @Test
  void anExactSanitizeCollisionIsRefusedWithoutAskingTheFilesystem() throws Exception {
    // #8184's first care point: "user@corp.com" and "user.corp.com" sanitize to ONE name, so there is
    // only ever one directory and nothing for the filesystem to decide. A probe answering "apart" must
    // not be able to re-admit it - and must not even be consulted.
    final AtomicInteger probes = new AtomicInteger();
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("user@corp.com", "user.corp.com"), (dir, other) -> {
      probes.incrementAndGet();
      return false;
    });
    final JSONObject chat = ChatStorage.createNewChat("db", "Whose?");
    final File legacyDir = seedLegacyDirectory(ChatStorage.sanitizeFilename("user@corp.com"), chat);

    assertThat(storage.listChats("user@corp.com")).isEmpty();
    assertThat(storage.listChats("user.corp.com")).isEmpty();
    assertThat(legacyDir).exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ".user_corp_com.ambiguous-migration").toFile()).exists();
    assertThat(probes.get()).as("an exact collision needs no filesystem answer").isZero();
  }

  @Test
  void aCaseOnlyCollisionIsRefusedAndRecordedWhereTheFilesystemFoldsCase() throws Exception {
    // The other side of the probe: where both spellings are one directory both accounts wrote into it,
    // so the #8154 refusal and its marker still apply in full.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("Bob", "bob"), FOLDS_CASE);
    final File legacyDir = seedLegacyDirectory("Bob", ChatStorage.createNewChat("db", "mixed"));

    assertThat(storage.listChats("Bob")).isEmpty();
    assertThat(legacyDir).exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ".Bob.ambiguous-migration").toFile()).exists();
  }

  @Test
  void theMarkerOfACaseFoldedCollisionStillRefusesAfterTheTreeIsCopiedToACaseSensitiveVolume() throws Exception {
    // #8184's second care point. The marker records that the directory's CONTENTS are mixed, which a
    // copy onto ext4 does not undo. Here the tree as it arrives on the case-sensitive volume: one
    // directory "alice" and its marker, spelled like it. The probe now says the spellings are apart, and
    // the account list alone would call alice the sole owner - the marker is what keeps refusing.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("Alice", "alice"), KEEPS_CASE_APART);
    final File legacyDir = seedLegacyDirectory("alice", ChatStorage.createNewChat("db", "Alice's and alice's, mixed"));
    FileUtils.writeFile(Paths.get(TEST_ROOT, "chats", ".alice.ambiguous-migration").toFile(), "");

    assertThat(storage.listChats("alice")).isEmpty();
    assertThat(legacyDir).exists();
  }

  @Test
  void spellingOnDiskPrefersTheExactEntryAndOtherwiseReportsTheCaseFoldedOne() throws Exception {
    // The marker is named after this, so it has to name the directory as it is really spelled: that is
    // the spelling a case-sensitive copy of the tree will be looked up by.
    final File chatsDir = Paths.get(TEST_ROOT, "chats").toFile();
    assertThat(new File(chatsDir, "alice").mkdirs()).isTrue();

    assertThat(ChatStorage.spellingOnDisk(new File(chatsDir, "alice"), "alice")).isEqualTo("alice");
    assertThat(ChatStorage.spellingOnDisk(new File(chatsDir, "Alice"), "Alice")).isEqualTo("alice");
    assertThat(ChatStorage.spellingOnDisk(new File(chatsDir, "bob"), "bob")).as("nothing on disk").isEqualTo("bob");
    final File missingParent = Paths.get(TEST_ROOT, "chats", "nope", "deeper").toFile();
    assertThat(ChatStorage.spellingOnDisk(missingParent, "deeper")).isEqualTo("deeper");
  }

  @Test
  void theSingleArgumentConstructorRefusesAnUnderscoreFreeNameWhereTheFilesystemFoldsCase() throws Exception {
    // #8183. With no account list, a case-folded twin cannot be SEEN - but whether one could share the
    // directory is a question for the filesystem, and where it folds case the answer is yes, so the
    // name is no longer claimed on the underscore test alone.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, null, FOLDS_CASE);
    final File legacyDir = seedLegacyDirectory("legacyuser", ChatStorage.createNewChat("db", "maybe LegacyUser's too"));

    assertThat(storage.listChats("legacyuser")).isEmpty();
    assertThat(legacyDir).as("left intact").exists();
    assertThat(Paths.get(TEST_ROOT, "chats", ChatStorage.hashUsername("legacyuser")).toFile()).doesNotExist();
  }

  @Test
  void everyUnknownAccountListStateRefusesAnUnderscoreFreeNameWhereTheFilesystemFoldsCase() throws Exception {
    // UNKNOWN has three more ways in than a null supplier: it throws, it returns null, or it does not
    // list the requesting user. Each must fall to the same filesystem-aware rule.
    final List<ChatStorage> unknownStates = List.of(
        new ChatStorage(TEST_ROOT, () -> {
          throw new IllegalStateException("security not ready");
        }, FOLDS_CASE),
        new ChatStorage(TEST_ROOT, () -> null, FOLDS_CASE),
        new ChatStorage(TEST_ROOT, () -> Set.of("root"), FOLDS_CASE));
    final File legacyDir = seedLegacyDirectory("carol", ChatStorage.createNewChat("db", "maybe Carol's too"));

    for (final ChatStorage storage : unknownStates) {
      assertThat(storage.listChats("carol")).isEmpty();
      assertThat(legacyDir).exists();
    }
  }

  @Test
  void withoutAccountsANameWithNoLetterIsStillClaimedEvenWhereTheFilesystemFoldsCase() throws Exception {
    // The guard rail on #8183: a name with no letter has no other case spelling, so no twin can share it
    // on any filesystem, and refusing it would orphan history for nothing.
    final ChatStorage storage = new ChatStorage(TEST_ROOT, null, FOLDS_CASE);
    final File legacyDir = seedLegacyDirectory("12345", ChatStorage.createNewChat("db", "digits only"));

    assertThat(storage.listChats("12345")).hasSize(1);
    assertThat(legacyDir).doesNotExist();
  }

  @Test
  void flipAsciiCaseInvertsEveryLetterAndReportsANameWithoutOne() {
    assertThat(ChatStorage.flipAsciiCase("John_Doe-42")).isEqualTo("jOHN_dOE-42");
    assertThat(ChatStorage.flipAsciiCase("a")).isEqualTo("A");
    assertThat(ChatStorage.flipAsciiCase("12_3-4")).isNull();
  }

  @Test
  void resolvesToSameDirectoryIsFalseForAMissingOrDistinctDirectoryAndTrueForItself() {
    final File chatsDir = Paths.get(TEST_ROOT, "chats").toFile();
    final File one = new File(chatsDir, "one");
    final File two = new File(chatsDir, "two");
    assertThat(one.mkdirs()).isTrue();
    assertThat(two.mkdirs()).isTrue();

    assertThat(ChatStorage.resolvesToSameDirectory(one, one)).isTrue();
    assertThat(ChatStorage.resolvesToSameDirectory(one, two)).isFalse();
    assertThat(ChatStorage.resolvesToSameDirectory(one, new File(chatsDir, "absent"))).isFalse();
  }

  @Test
  void onARealCaseSensitiveFilesystemTwoCaseOnlyTwinsEachMigrateTheirOwnDirectory() throws Exception {
    // End to end through the production probe, on the filesystem #8184 is about. Runs where TEST_ROOT
    // is case-sensitive (CI); skipped elsewhere, where the two directories cannot both exist.
    assumeTrue(testRootIsCaseSensitive(), "needs a case-sensitive filesystem");
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("Alice", "alice"));
    final File upper = seedLegacyDirectory("Alice", ChatStorage.createNewChat("db", "Alice's"));
    final File lower = seedLegacyDirectory("alice", ChatStorage.createNewChat("db", "alice's"));

    assertThat(storage.listChats("Alice")).extracting(c -> c.getString("title")).containsExactly("Alice's");
    assertThat(storage.listChats("alice")).extracting(c -> c.getString("title")).containsExactly("alice's");
    assertThat(upper).doesNotExist();
    assertThat(lower).doesNotExist();

    // ...and the single-argument constructor still claims an underscore-free name there.
    final File legacyDir = seedLegacyDirectory("dave", ChatStorage.createNewChat("db", "dave's"));
    assertThat(new ChatStorage(TEST_ROOT).listChats("dave")).hasSize(1);
    assertThat(legacyDir).doesNotExist();
  }

  @Test
  void onARealCaseInsensitiveFilesystemCaseOnlyTwinsAndAnUnlistedNameAreRefused() throws Exception {
    // End to end through the production probe where the filesystem folds case (NTFS, default macOS
    // APFS/HFS+); skipped on CI's case-sensitive one, which the FOLDS_CASE tests stand in for.
    assumeFalse(testRootIsCaseSensitive(), "needs a case-insensitive filesystem");
    final ChatStorage storage = new ChatStorage(TEST_ROOT, () -> Set.of("Alice", "alice"));
    final File legacyDir = seedLegacyDirectory("alice", ChatStorage.createNewChat("db", "mixed"));

    assertThat(storage.listChats("Alice")).isEmpty();
    assertThat(storage.listChats("alice")).isEmpty();
    assertThat(legacyDir).exists();
    // Recorded under the directory's own spelling although "Alice" was looked up first.
    assertThat(Paths.get(TEST_ROOT, "chats").toFile().list()).contains(".alice.ambiguous-migration")
        .doesNotContain(".Alice.ambiguous-migration");

    // #8183 on the real filesystem: no accounts, underscore-free name, refused.
    final File other = seedLegacyDirectory("erin", ChatStorage.createNewChat("db", "maybe Erin's too"));
    assertThat(new ChatStorage(TEST_ROOT).listChats("erin")).isEmpty();
    assertThat(other).exists();
  }
}
