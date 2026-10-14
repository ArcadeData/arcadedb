/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.database;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PageManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9951: two changes to the SAME setting on the SAME node, both refused. The first change (A) stores V1 over P,
 * the second (B) stores V2 capturing V1 as the value it replaced. A is refused while B is in flight, so its undo leaves
 * V2; then B is refused and its undo put back V1 - the value of a change that was itself refused, held by no other node
 * and not by {@code configuration.json}. The undo now knows whether the change it would restore was itself refused, and
 * then settles on what {@code configuration.json} holds instead.
 */
// A hang detector for the latches that order the two commands, not a latency bound
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class Issue9951SameSettingRefusedUndoTest {
  private static final String DB_PATH  = "target/databases/Issue9951SameSettingRefusedUndoTest";
  private static final String KEY      = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();
  private static final String FILE_KEY = KEY.substring(GlobalConfiguration.PREFIX.length());

  /**
   * A database whose configuration save is refused outright, as an HA save a peer cannot decode is. With
   * {@link #interleave} the first refused save waits until a second one started, and the second is refused only once
   * the first command finished its undo: A refused first, then B, as in the issue.
   */
  private static final class RefusingDatabase extends LocalDatabase {
    private volatile boolean        refuse;
    private volatile boolean        interleave;
    private final    AtomicInteger  refusedSaves = new AtomicInteger();
    private final    CountDownLatch firstInSave  = new CountDownLatch(1);
    private final    CountDownLatch secondInSave = new CountDownLatch(1);
    private volatile Thread         firstCommand;

    private RefusingDatabase(final String path) {
      super(path, ComponentFile.MODE.READ_WRITE, new ContextConfiguration(), null, new ConcurrentHashMap<>());
    }

    @Override
    public void saveConfiguration() throws IOException {
      if (!refuse) {
        super.saveConfiguration();
        return;
      }
      if (interleave) {
        try {
          if (refusedSaves.incrementAndGet() == 1) {
            firstInSave.countDown();
            secondInSave.await();
          } else {
            secondInSave.countDown();
            // Refused only after the first command is refused and has run its undo
            firstCommand.join();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      throw new IllegalStateException("a peer cannot decode the configuration entry yet");
    }
  }

  private RefusingDatabase database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    database = new RefusingDatabase(DB_PATH);
    database.create();
    database.command("sql", "ALTER DATABASE `" + KEY + "` 3");
    database.refuse = true;
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /** The issue's sequence through ALTER DATABASE: A stores 13, B stores 17, A is refused, then B is refused. */
  @Test
  void twoRefusedAlterDatabaseOnTheSameSettingLeaveTheSavedValue() throws Exception {
    database.interleave = true;

    final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    final Thread first = new Thread(() -> {
      try {
        database.command("sql", "ALTER DATABASE `" + KEY + "` 13");
      } catch (final Throwable t) {
        firstFailure.set(t);
      }
    });
    database.firstCommand = first;
    first.start();
    database.firstInSave.await();

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 17")).isInstanceOf(IllegalStateException.class);
    first.join();
    assertThat(firstFailure.get()).isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("both changes were refused: the node is back on the value configuration.json holds").isEqualTo(3);
    assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
  }

  /** A refused while B is in flight: A's undo leaves B's value; B refused: back to the saved value, not A's. */
  @Test
  void theUndoOfAChangeThatReplacedARefusedOneFallsBackToTheSavedValue() throws IOException {
    final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 17));
    assertThat(b.previousValue()).isEqualTo(13);

    database.undoSettingChange(a);
    assertThat(valueInForce()).as("B is still in flight: its value stands").isEqualTo(17);

    database.undoSettingChange(b);
    assertThat(valueInForce()).as("13 belongs to a refused change: not put back").isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /** B refused first: A is still in flight, so its value comes back; then A refused: back to the value A replaced. */
  @Test
  void refusedInTheOrderTheyWereMadeEachUndoPutsBackThePreviousValue() throws IOException {
    final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 17));

    database.undoSettingChange(b);
    assertThat(valueInForce()).as("A is still in flight: its value comes back").isEqualTo(13);

    database.undoSettingChange(a);
    assertThat(valueInForce()).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /**
   * The second ambiguity in the issue: B stores the SAME value A stored, its save under way. Comparing values could not
   * tell that from "untouched", so A's undo reverted B's value, which no file held yet; B's own save would then have
   * shipped the reverted value.
   */
  @Test
  void theUndoLeavesTheSameValueStoredSinceByAnotherChange() throws IOException {
    final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    final LocalDatabase.SettingChange b = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));

    database.undoSettingChange(a);
    assertThat(valueInForce()).as("B stored 13 too and is still in flight").isEqualTo(13);

    database.undoSettingChange(b);
    assertThat(valueInForce()).as("both refused").isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /** A change superseded by a replicated apply that was then refused is not put back by a later undo. */
  @Test
  void anApplySinceTheChangeLeavesItsValueInForce() throws IOException {
    final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    // Another node committed the same value: memory and file hold 13 now
    database.applyConfigurationChanges(
        new JSONObject().put("configuration", new JSONObject().put(FILE_KEY, 13)).put("removed", new JSONArray()).toString());

    database.undoSettingChange(a);

    assertThat(valueInForce()).isEqualTo(13);
    assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(13);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /**
   * B fails inside the change itself and puts A's value back: A, still in flight, is the latest change again, so A's
   * refusal still undoes it.
   */
  @Test
  void aChangeThatFailsInsideLeavesTheEarlierChangeUndoable() throws IOException {
    final LocalDatabase.SettingChange a = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    assertThatThrownBy(() -> database.changeSetting(KEY, () -> {
      database.getConfiguration().setValue(KEY, 17);
      // What putInForce() does when the side effect of the new value fails
      database.restoreSetting(KEY, 13);
      throw new IllegalArgumentException("side effect failed");
    })).isInstanceOf(IllegalArgumentException.class);
    assertThat(valueInForce()).isEqualTo(13);

    database.undoSettingChange(a);

    assertThat(valueInForce()).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  private int valueInForce() {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
  }

  private JSONObject savedSettings() throws IOException {
    return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
        .getJSONObject("configuration");
  }
}
