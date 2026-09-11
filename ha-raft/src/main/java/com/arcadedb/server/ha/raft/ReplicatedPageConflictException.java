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
 * The retryable conflict the leader refuses a replicated transaction with when it was validated against a page
 * version the Raft log has already moved past (issue #6965). A {@link ConcurrentModificationException} to every
 * caller, so the existing retry loops handle it; the page and the version the cluster is at are also carried, so the
 * originating replica can wait for that version to be applied locally before letting the caller retry - otherwise a
 * replica whose apply trails the leader by a couple of entries keeps re-reading a stale page and is refused again.
 * <p>
 * Ratis carries the cause of a state machine refusal to the client by class name and message, and rebuilds it through
 * the {@code (String)} constructor: the fields therefore live in the message and are parsed back out of it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicatedPageConflictException extends ConcurrentModificationException {
  private static final Pattern MESSAGE = Pattern.compile(
      "Concurrent modification on page (\\d+)/(\\d+) of database '([^']*)': the transaction was validated against version (\\d+) but the cluster is at version (\\d+)");

  private final int    fileId;
  private final int    pageNumber;
  private final int    clusterVersion;
  private final String databaseName;

  public ReplicatedPageConflictException(final String databaseName, final int fileId, final int pageNumber, final int baseVersion,
      final int clusterVersion) {
    super("Concurrent modification on page " + fileId + "/" + pageNumber + " of database '" + databaseName
        + "': the transaction was validated against version " + baseVersion + " but the cluster is at version " + clusterVersion
        + ". Please retry the operation");
    this.databaseName = databaseName;
    this.fileId = fileId;
    this.pageNumber = pageNumber;
    this.clusterVersion = clusterVersion;
  }

  /** Rebuilds the exception from its own message, as the Ratis client does when it receives the refusal. */
  public ReplicatedPageConflictException(final String message) {
    super(message);
    final Matcher matcher = message != null ? MESSAGE.matcher(message) : null;
    if (matcher != null && matcher.find()) {
      fileId = Integer.parseInt(matcher.group(1));
      pageNumber = Integer.parseInt(matcher.group(2));
      databaseName = matcher.group(3);
      clusterVersion = Integer.parseInt(matcher.group(5));
    } else {
      fileId = -1;
      pageNumber = -1;
      databaseName = null;
      clusterVersion = -1;
    }
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public int getFileId() {
    return fileId;
  }

  public int getPageNumber() {
    return pageNumber;
  }

  /** The version the cluster is at for the page, or {@code -1} when the message could not be parsed. */
  public int getClusterVersion() {
    return clusterVersion;
  }
}
