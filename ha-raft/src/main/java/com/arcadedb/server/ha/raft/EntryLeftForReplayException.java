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

/**
 * A committed entry this node did not apply because its database was closed under the apply thread while the node is
 * shutting down (issue #9550). The entry is left in the Raft log for the replay on restart: the applied index does not
 * move over it, and {@code ArcadeStateMachine.takeSnapshot()} does not checkpoint past it.
 * <p>
 * A {@link ReplicationException}, so the apply path reports it as a failed apply without reaching the per-database
 * quarantine: nothing diverged, the node is going away.
 */
public class EntryLeftForReplayException extends ReplicationException {
  public EntryLeftForReplayException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
