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

import com.arcadedb.exception.ConcurrentModificationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The retryable conflict the leader refuses a replicated transaction with when it was prepared before the originating
 * node had applied a schema change the leader has already applied (issue #8686). Such a transaction is valid against
 * the page versions, since it comes after the change in the log, but its WAL was built against a schema without, say,
 * the index the change created, so applying it would leave the record out of that index on every node.
 * <p>
 * A {@link ConcurrentModificationException} to every caller, so the existing retry loops handle it. The Raft log index of
 * the schema change is carried as well, so the originating replica can wait for it to be applied locally before letting the
 * caller retry: without the wait a replica whose apply trails the leader would prepare the retry under the same old schema and
 * be refused again.
 * <p>
 * Ratis carries the cause of a state machine refusal to the client by class name and message only, and rebuilds it through
 * the {@code (String)} constructor, so the fields travel in a fixed machine-readable header at the front of the message and
 * only that header is parsed back - the same contract as {@link ReplicatedPageConflictException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicatedSchemaConflictException extends ConcurrentModificationException {
  /** The machine-readable header the fields travel in: {@code [8686 db='x' prepared=p schema=s] }. */
  private static final Pattern HEADER = Pattern.compile("^\\[8686 db='([^']*)' prepared=(-?\\d+) schema=(\\d+)] ");

  private final String databaseName;
  private final long   preparedAtIndex;
  private final long   schemaIndex;

  public ReplicatedSchemaConflictException(final String databaseName, final long preparedAtIndex, final long schemaIndex) {
    super("[8686 db='" + databaseName + "' prepared=" + preparedAtIndex + " schema=" + schemaIndex
        + "] The transaction on database '" + databaseName + "' was prepared at Raft index " + preparedAtIndex
        + ", before the schema change applied at index " + schemaIndex
        + ", so it was built against a schema that no longer holds. Please retry the operation");
    this.databaseName = databaseName;
    this.preparedAtIndex = preparedAtIndex;
    this.schemaIndex = schemaIndex;
  }

  /** Rebuilds the exception from its own message, as the Ratis client does when it receives the refusal. */
  public ReplicatedSchemaConflictException(final String message) {
    super(message);
    final Matcher matcher = message != null ? HEADER.matcher(message) : null;
    if (matcher != null && matcher.find()) {
      databaseName = matcher.group(1);
      preparedAtIndex = Long.parseLong(matcher.group(2));
      schemaIndex = Long.parseLong(matcher.group(3));
    } else {
      databaseName = null;
      preparedAtIndex = -1L;
      schemaIndex = -1L;
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  /** The index the refused transaction was prepared at, or {@code -1} when the message carried no header. */
  public long getPreparedAtIndex() {
    return preparedAtIndex;
  }

  /** The Raft log index of the schema change the originator has to apply, or {@code -1} when the message carried no header. */
  public long getSchemaIndex() {
    return schemaIndex;
  }
}
