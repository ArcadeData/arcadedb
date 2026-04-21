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

public enum RaftLogEntryType {
  TX_ENTRY((byte) 1),
  SCHEMA_ENTRY((byte) 2),
  INSTALL_DATABASE_ENTRY((byte) 3),
  DROP_DATABASE_ENTRY((byte) 4),
  SECURITY_USERS_ENTRY((byte) 5);

  private final byte id;

  RaftLogEntryType(final byte id) {
    this.id = id;
  }

  public byte getId() {
    return id;
  }

  public static RaftLogEntryType fromId(final byte id) {
    for (final RaftLogEntryType type : values())
      if (type.id == id)
        return type;
    return null;
  }
}
