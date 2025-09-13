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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.remote;

import com.arcadedb.database.TransactionExplicitLock;

import java.util.*;

/**
 * Explicit lock on a transaction to lock buckets and types in pessimistic way. This avoids the retry mechanism of default implicit locking.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteTransactionExplicitLock implements TransactionExplicitLock {
  private final RemoteDatabase database;
  private final Set<String>    lockedTypeNames   = new HashSet<>();
  private final Set<String>    lockedBucketNames = new HashSet<>();

  public RemoteTransactionExplicitLock(final RemoteDatabase database) {
    this.database = database;
  }

  @Override
  public RemoteTransactionExplicitLock bucket(final String bucketName) {
    lockedBucketNames.add(bucketName);
    return this;
  }

  @Override
  public RemoteTransactionExplicitLock type(final String typeName) {
    lockedTypeNames.add(typeName);
    return this;
  }

  @Override
  public void lock() {
    final StringBuilder command = new StringBuilder();
    command.append("LOCK");
    if (!lockedTypeNames.isEmpty()) {
      command.append(" TYPE ");
      command.append(String.join(", ", lockedTypeNames));
    }
    if (!lockedBucketNames.isEmpty()) {
      command.append(" BUCKET ");
      command.append(String.join(", ", lockedBucketNames));
    }

    lockedBucketNames.clear();
    lockedTypeNames.clear();

    database.command("sql", command.toString());
  }
}
