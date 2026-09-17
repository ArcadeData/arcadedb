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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for item 2 of issue #7296.
 * <p>
 * {@code RaftHAServer.resolvePersistStorage} read {@code arcadedb.ha.raftPersistStorage} straight off
 * {@code System.getProperty} with {@code Boolean.parseBoolean}, which maps everything that is not the literal
 * {@code "true"} to {@code false}. So {@code -Darcadedb.ha.raftPersistStorage=yes} - an operator AFFIRMING that
 * the Raft log must survive a restart - silently made the storage ephemeral instead, and that setting's own
 * description says what that costs: wiping the Raft log on every restart turns a follower that was merely lagging
 * into a permanently diverged node on a full-cluster cold restart.
 * <p>
 * The property now goes through the same strict parse every other reader of raw configuration text uses. A value
 * that cannot be read is reported and the DEFAULT kept, which for this setting is the durable side.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7296RaftPersistStorageStrictTest {

  private static final String KEY = GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey();

  @AfterEach
  void clearProperty() {
    System.clearProperty(KEY);
  }

  @Test
  void anUnreadablePropertyKeepsTheDurableDefaultInsteadOfSilentlyDisablingPersistence() {
    System.setProperty(KEY, "yes");

    assertThat(RaftHAServer.resolvePersistStorage(new ContextConfiguration()))
        .as("'yes' is not boolean text: refuse it and keep the durable default, never read it as 'wipe the log'")
        .isTrue();
  }

  @Test
  void aTypoKeepsTheDurableDefaultToo() {
    System.setProperty(KEY, "flase");

    assertThat(RaftHAServer.resolvePersistStorage(new ContextConfiguration())).isTrue();
  }

  /** An explicit, readable {@code false} is still honoured: a throwaway/test cluster may really want ephemeral. */
  @Test
  void anExplicitFalseIsStillHonoured() {
    System.setProperty(KEY, "false");

    assertThat(RaftHAServer.resolvePersistStorage(new ContextConfiguration())).isFalse();
  }

  @Test
  void anExplicitTrueIsStillHonoured() {
    System.setProperty(KEY, "TRUE");

    assertThat(RaftHAServer.resolvePersistStorage(new ContextConfiguration())).isTrue();
  }

  /** With no system property, the ContextConfiguration still decides, exactly as before. */
  @Test
  void theConfigurationStillDecidesWhenNoPropertyIsSet() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);

    assertThat(RaftHAServer.resolvePersistStorage(configuration)).isFalse();
  }
}
