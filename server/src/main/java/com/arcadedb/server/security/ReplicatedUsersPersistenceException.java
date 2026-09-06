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
package com.arcadedb.server.security;

import com.arcadedb.server.ServerException;

/**
 * Thrown by {@link ServerSecurity#applyReplicatedUsers} when a replicated user list was applied in memory but
 * could not be written to {@code server-users.jsonl}.
 * <p>
 * A distinct type because the two ways that method can fail need opposite handling on the Raft apply path
 * (issue #7137). This one happens AFTER the in-memory swap: the node is already enforcing the new list, nothing
 * it replicates can diverge, and halting would turn a full or read-only config volume into an indefinite crash
 * loop - so the apply reports it and the node stays up. Every other failure - a payload that cannot be parsed,
 * or a user entry that cannot be constructed - happens BEFORE any mutation and means this node cannot read a
 * committed entry its peers applied, which is the case #4798 argues must never be skipped quietly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicatedUsersPersistenceException extends ServerException {

  public ReplicatedUsersPersistenceException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
