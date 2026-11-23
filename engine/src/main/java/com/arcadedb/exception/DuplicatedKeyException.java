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
package com.arcadedb.exception;

import com.arcadedb.database.RID;

public class DuplicatedKeyException extends ArcadeDBException {
  private final String indexName;
  private final String keys;
  private final RID    currentIndexedRID;

  public DuplicatedKeyException(final String indexName, final String keys, final RID currentIndexedRID) {
    super("Duplicated key " + keys + " found on index '" + indexName + "' already assigned to record " + currentIndexedRID);
    this.indexName = indexName;
    this.keys = keys;
    this.currentIndexedRID = currentIndexedRID;
  }

  /**
   * Rebuilds the exception from the {@code indexName|keys|rid} string a server sends as {@code exceptionArgs}, the
   * inverse of how the HTTP error mapper writes it. The index name is the first segment and the RID the last, so a key
   * VALUE that contains the separator itself (customer data, nothing stops it) stays whole in the keys. The RID segment
   * {@code null} is how the server writes a missing current RID, and rebuilds as a null RID.
   * <p>
   * The format cannot carry a separator in BOTH the index name and the keys, so the index name is assumed not to contain
   * one. If it ever does, the split point moves inside the index name: the result is still typed and carries the right
   * RID, but the index name and keys are cut in the wrong place.
   *
   * @return the rebuilt exception, or null when the string is null, has fewer than three segments or its last segment
   * is not a RID: the caller then falls back to its generic mapping instead of failing to report the server's failure
   * at all (issue #9473)
   */
  public static DuplicatedKeyException fromExceptionArgs(final String exceptionArgs) {
    if (exceptionArgs == null)
      return null;

    final int firstSeparator = exceptionArgs.indexOf('|');
    final int lastSeparator = exceptionArgs.lastIndexOf('|');
    if (firstSeparator < 0 || firstSeparator == lastSeparator)
      return null;

    final String ridToken = exceptionArgs.substring(lastSeparator + 1);
    final RID rid;
    if ("null".equals(ridToken))
      rid = null;
    else
      try {
        rid = new RID(ridToken);
      } catch (final RuntimeException e) {
        // Not a RID, so malformed args. RuntimeException rather than IllegalArgumentException on purpose: "#7" throws
        // IndexOutOfBoundsException from RID(String), "#a:b" NumberFormatException, "garbage" IllegalArgumentException
        return null;
      }

    return new DuplicatedKeyException(exceptionArgs.substring(0, firstSeparator),
        exceptionArgs.substring(firstSeparator + 1, lastSeparator), rid);
  }

  public String getIndexName() {
    return indexName;
  }

  public String getKeys() {
    return keys;
  }

  public RID getCurrentIndexedRID() {
    return currentIndexedRID;
  }
}
