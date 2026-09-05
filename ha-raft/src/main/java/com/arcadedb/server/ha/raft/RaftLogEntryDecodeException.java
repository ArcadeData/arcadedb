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
 * Thrown by {@link RaftLogEntryCodec#decode} when a committed entry of a KNOWN type cannot be decoded:
 * truncated, corrupt, or written in a shape this version does not understand.
 * <p>
 * It exists so the failure is not indistinguishable from a bug in the apply itself (issue #7138). A decode
 * failure carries the two things a caller needs to decide how bad it is - the entry {@link #getType() type}
 * and, when the envelope got far enough to read it, the {@link #getDatabaseName() database} the entry
 * targets. With a database name, {@code ArcadeStateMachine} quarantines that one database and lets the leader
 * resend it as a snapshot; the node stays up for its other databases instead of halting on one bad entry.
 * Without one, the node-wide halt is still the honest answer.
 * <p>
 * An entry whose leading type byte is UNKNOWN is not this: it is handled separately and still halts, because
 * skipping a committed mutation nobody can read is a silent divergence (issue #4798).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftLogEntryDecodeException extends IllegalStateException {

  private final RaftLogEntryType type;
  private final String           databaseName;

  public RaftLogEntryDecodeException(final String message, final RaftLogEntryType type, final String databaseName,
      final Throwable cause) {
    super(message, cause);
    this.type = type;
    this.databaseName = databaseName;
  }

  /** The entry type being decoded, or {@code null} when the failure happened before it was known. */
  public RaftLogEntryType getType() {
    return type;
  }

  /**
   * The database the entry targets, or {@code null}/empty when the entry is node-scoped or the name had not
   * been read yet when the decode failed.
   */
  public String getDatabaseName() {
    return databaseName;
  }
}
