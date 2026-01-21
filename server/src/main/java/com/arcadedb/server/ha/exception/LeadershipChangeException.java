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
package com.arcadedb.server.ha.exception;

import com.arcadedb.server.ha.ReplicationException;

/**
 * Exception indicating a leadership change in the cluster.
 *
 * <p>Thrown when:
 * <ul>
 *   <li>Leader election is in progress
 *   <li>Current server is no longer the leader
 *   <li>Replica needs to reconnect to new leader
 * </ul>
 *
 * <p>Recovery strategy: Find and connect to new leader (no backoff needed)
 */
public class LeadershipChangeException extends ReplicationException {

  private final String formerLeader;
  private final String newLeader;

  public LeadershipChangeException(String message, String formerLeader, String newLeader) {
    super(message);
    this.formerLeader = formerLeader;
    this.newLeader = newLeader;
  }

  public String getFormerLeader() {
    return formerLeader;
  }

  public String getNewLeader() {
    return newLeader;
  }
}
