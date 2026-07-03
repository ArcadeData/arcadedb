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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4835: the Raft storage directory was wiped and re-FORMATted on every
 * {@code RaftHAServer.start()} because {@code HA_RAFT_PERSIST_STORAGE} defaults to {@code false}.
 * In Kubernetes the storage lives on a PersistentVolume that survives pod restarts, so wiping it
 * forced a full resync and, on a single-seed cluster, risked re-forming a fresh empty cluster
 * (data loss / split-brain).
 * <p>
 * The fix makes Kubernetes deployments persist storage by default while still honoring an explicit
 * {@code raftPersistStorage=false}. This test exercises {@link RaftHAServer#resolvePersistStorage}
 * directly across the full decision matrix, without standing up a Ratis cluster.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4835RaftStoragePersistenceTest {

  private final String persistKey = GlobalConfiguration.HA_RAFT_PERSIST_STORAGE.getKey();

  @AfterEach
  void clearSystemProperty() {
    System.clearProperty(persistKey);
  }

  @Test
  void nonK8sDefaultsToPersistent() {
    // The default is now durable everywhere (not just K8s): wiping the Raft log on restart can turn a
    // lagging follower into a permanently diverged node on a cold restart, so storage is preserved
    // unless the operator explicitly opts into ephemeral. Outside Kubernetes, with nothing set, persist.
    final ContextConfiguration config = new ContextConfiguration();
    assertThat(RaftHAServer.resolvePersistStorage(config)).isTrue();
  }

  @Test
  void nonK8sHonorsExplicitEphemeral() {
    // A throwaway/test cluster can still opt into ephemeral storage explicitly.
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isFalse();
  }

  @Test
  void nonK8sHonorsExplicitPersist() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isTrue();
  }

  @Test
  void k8sDefaultsToPersistent() {
    // The core fix: under Kubernetes, with no explicit raftPersistStorage, storage must be preserved.
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, true);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isTrue();
  }

  @Test
  void k8sHonorsExplicitEphemeralViaContext() {
    // An operator who really wants ephemeral storage in K8s can still opt out explicitly.
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, true);
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isFalse();
  }

  @Test
  void k8sHonorsExplicitEphemeralViaSystemProperty() {
    System.setProperty(persistKey, "false");
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, true);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isFalse();
  }

  @Test
  void k8sExplicitPersistStaysPersistent() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_K8S, true);
    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, true);
    assertThat(RaftHAServer.resolvePersistStorage(config)).isTrue();
  }

  @Test
  void isExplicitlyConfiguredDetectsContextAndSystemProperty() {
    final ContextConfiguration config = new ContextConfiguration();
    assertThat(RaftHAServer.isExplicitlyConfigured(config, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isFalse();

    config.setValue(GlobalConfiguration.HA_RAFT_PERSIST_STORAGE, false);
    assertThat(RaftHAServer.isExplicitlyConfigured(config, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isTrue();

    final ContextConfiguration empty = new ContextConfiguration();
    System.setProperty(persistKey, "false");
    assertThat(RaftHAServer.isExplicitlyConfigured(empty, GlobalConfiguration.HA_RAFT_PERSIST_STORAGE)).isTrue();
  }
}
