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
package com.arcadedb.server.ha;

/**
 * Common interface for replicated database wrappers across all HA implementations (legacy custom HA
 * and Raft-based HA). Allows server-layer code to interact with the replicated database abstraction
 * without knowing which concrete HA implementation is active.
 */
public interface HAReplicatedDatabase {

  /**
   * Propagates the (just-created) database to all replicas. Must be called on the leader after
   * creating a new database so that replicas are made aware of it before any transactions arrive.
   */
  void createInReplicas();

  /**
   * Returns the write quorum configured for this database.
   */
  HAServer.QUORUM getQuorum();

  /**
   * Returns {@code true} if the local node is currently the HA leader.
   */
  boolean isLeader();

  /**
   * Returns the HTTP address (host:port) of the current leader, or {@code null} if the leader is
   * unknown or its HTTP address is not configured. Used to forward commands from replicas to the leader.
   */
  String getLeaderHttpAddress();
}
