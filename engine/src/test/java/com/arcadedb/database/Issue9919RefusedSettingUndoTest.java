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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9919: under Raft HA the save of {@code ALTER DATABASE} can be refused outright (a peer cannot decode the
 * configuration entry yet, this node left the configuration, ...), and the setting is then put back to the value the
 * command replaced. When a change to the SAME setting committed by another node was applied on this node inside that
 * window, the undo overwrote it with the stale previous value: this node ran on a value no other node held, and that
 * its own {@code configuration.json} did not hold.
 * <p>
 * The other node's apply is reproduced with the call the HA layer makes on every node,
 * {@link LocalDatabase#applyConfigurationChanges(String)}, from another thread as the Raft state machine runs it.
 */
// A hang detector for the wait on the other node's apply, not a latency bound
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class Issue9919RefusedSettingUndoTest {
  private static final String DB_PATH  = "target/databases/Issue9919RefusedSettingUndoTest";
  private static final String KEY      = GlobalConfiguration.TYPE_DEFAULT_BUCKETS.getKey();
  private static final String FILE_KEY = KEY.substring(GlobalConfiguration.PREFIX.length());

  /** What another node committed for the same setting: what its apply installs on this node. */
  private static final String OTHER_NODE_CHANGE = new JSONObject().put("configuration", new JSONObject().put(FILE_KEY, 21))
      .put("removed", new JSONArray()).toString();

  private RefusingDatabase database;

  /**
   * Fires once, on the first read of {@link #KEY}'s value after it is armed: the read the command makes to capture the
   * value it replaces. Right after that read it starts the other node's apply on another thread and gives it the chance
   * to land before the command goes on - which it does unless the capture holds the lock the apply takes.
   */
  private static final class HookedConfiguration extends ContextConfiguration {
    private volatile RefusingDatabase database;
    private volatile boolean          armed;

    @Override
    public <T> T getValue(final String name, final T defaultValue) {
      final T value = super.getValue(name, defaultValue);
      if (armed && KEY.equals(name)) {
        armed = false;
        database.otherNodeApply = new Thread(() -> {
          try {
            database.applyConfigurationChanges(OTHER_NODE_CHANGE);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        });
        database.otherNodeApply.start();
        // Until the apply either landed or blocked: it blocks when the capture holds the apply lock, and then lands later,
        // inside the refused save
        final Thread apply = database.otherNodeApply;
        // The interrupt is how @Timeout ends a wait that never ends
        while (apply.isAlive() && apply.getState() != Thread.State.BLOCKED && !Thread.currentThread().isInterrupted())
          Thread.onSpinWait();
      }
      return value;
    }
  }

  /** A database whose configuration save is refused outright, as an HA save a peer cannot decode is. */
  private static final class RefusingDatabase extends LocalDatabase {
    private volatile boolean refuse;
    private volatile boolean otherNodeAppliesDuringTheSave;
    private volatile Thread  otherNodeApply;

    private RefusingDatabase(final String path, final HookedConfiguration configuration) {
      super(path, ComponentFile.MODE.READ_WRITE, configuration, null, new ConcurrentHashMap<>());
      configuration.database = this;
    }

    @Override
    public void saveConfiguration() throws IOException {
      if (!refuse) {
        super.saveConfiguration();
        return;
      }
      try {
        if (otherNodeApply != null)
          otherNodeApply.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (otherNodeAppliesDuringTheSave)
        applyConfigurationChanges(OTHER_NODE_CHANGE);
      throw new IllegalStateException("a peer cannot decode the configuration entry yet");
    }
  }

  private HookedConfiguration configuration;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    PageManager.INSTANCE.acquire();
    configuration = new HookedConfiguration();
    database = new RefusingDatabase(DB_PATH, configuration);
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

  /**
   * The window the undo could not see: the other node's change is applied right after the command read the value it
   * replaces, and before it stores its own. The undo then put back the value the other node's change had already replaced.
   */
  @Test
  void anOtherNodeChangeAppliedBeforeTheCommandStoresItsValueSurvivesTheUndo() throws IOException {
    configuration.armed = true;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

    assertThat(configuration.armed).as("the hook fired").isFalse();
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS))
        .as("the node holds what the other node committed, as every node does").isEqualTo(21);
    assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(21);
    assertThat(database.pendingConfigurationChanges()).as("memory and file agree").isNull();
  }

  /** The other node's change is applied after the command stored its value, while the refused save is under way. */
  @Test
  void anOtherNodeChangeAppliedDuringTheRefusedSaveSurvivesTheUndo() throws IOException {
    database.otherNodeAppliesDuringTheSave = true;

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(21);
    assertThat(savedSettings().getInt(FILE_KEY)).isEqualTo(21);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /** Nothing else happened meanwhile: the refusal puts the previous value back, as before. */
  @Test
  void aRefusedChangeWithNothingElseMeanwhileIsUndone() throws IOException {
    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + KEY + "` 13")).isInstanceOf(IllegalStateException.class);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(3);
    assertThat(database.pendingConfigurationChanges()).isNull();
  }

  /**
   * The undo puts the previous value back only while the setting still holds the value the refused change set: one
   * stored since by another writer stands, whether or not it reached the file yet.
   */
  @Test
  void theUndoLeavesAValueStoredSinceByAnotherWriter() {
    final LocalDatabase.SettingChange change = database.changeSetting(KEY, () -> database.getConfiguration().setValue(KEY, 13));
    assertThat(change.previousValue()).isEqualTo(3);
    assertThat(change.valueSet()).isEqualTo(13);

    // Another change to the same setting on this node, its save still under way
    database.getConfiguration().setValue(KEY, 17);

    database.undoSettingChange(change);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS)).isEqualTo(17);
  }

  /** A setting not set before the refused change goes back to "not set", so it falls back to its default. */
  @Test
  void theUndoOfASettingNotSetBeforeRemovesIt() {
    final LocalDatabase.SettingChange change = database.changeSetting(GlobalConfiguration.TX_RETRIES.getKey(),
        () -> database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 7));
    assertThat(change.previousValue()).isNull();

    database.undoSettingChange(change);

    assertThat(database.getConfiguration().getContextKeys()).doesNotContain(GlobalConfiguration.TX_RETRIES.getKey());
  }

  private JSONObject savedSettings() throws IOException {
    return new JSONObject(FileUtils.readFileAsString(new File(DB_PATH, LocalDatabase.CONFIGURATION_FILE_NAME)))
        .getJSONObject("configuration");
  }
}
