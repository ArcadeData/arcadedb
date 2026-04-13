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

/**
 * Wire type codes for Raft log entries. Each entry starts with one of these bytes.
 * <p>
 * Returning {@code null} from {@link #fromCode(byte)} instead of throwing for unknown codes
 * allows forward-compatible handling during rolling upgrades where a newer node may write
 * entry types that an older node does not yet know about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum RaftLogEntryType {
  /**
   * Replicate database creation to all nodes.
   */
  CREATE_DATABASE((byte) 1),
  /**
   * Replicate database drop to all nodes.
   */
  DROP_DATABASE((byte) 2),
  /**
   * Replicate a committed transaction (WAL page diffs + optional schema changes).
   */
  TRANSACTION((byte) 3);

  private final byte code;

  RaftLogEntryType(final byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  /**
   * Returns the RaftLogEntryType for the given wire code, or {@code null} if the code is unrecognized.
   */
  public static RaftLogEntryType fromCode(final byte code) {
    return switch (code) {
      case 1 -> CREATE_DATABASE;
      case 2 -> DROP_DATABASE;
      case 3 -> TRANSACTION;
      default -> null;
    };
  }
}
