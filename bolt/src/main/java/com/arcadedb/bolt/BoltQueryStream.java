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
package com.arcadedb.bolt;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * One open BOLT result stream, i.e. everything a single RUN produced that a later PULL/DISCARD needs.
 * <p>
 * BOLT 4.0 introduced the {@code qid} field precisely so a client can hold several result streams open at once
 * inside one explicit transaction: RUN is valid from TX_STREAMING, and PULL/DISCARD name the stream they act on.
 * Before issue #6804 this state lived in a single set of fields on {@code BoltNetworkExecutor}, which made more
 * than one open stream unrepresentable - so a second RUN in a transaction whose first stream still had rows was
 * rejected as a protocol error and the session was lost.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BoltQueryStream {
  /**
   * Identifier returned in the RUN SUCCESS metadata and accepted back on PULL/DISCARD. Numbered per explicit
   * transaction, starting at 0, matching what a Neo4j server returns.
   */
  final long qid;

  /** Null for a synthetic (system-query) stream, which serves its rows from {@link #syntheticResults}. */
  ResultSet resultSet;

  /** Column names of this query, as sent in the RUN SUCCESS "fields" metadata. */
  List<String> fields;

  /**
   * First row, consumed by {@code extractFieldNames} to derive {@link #fields} and therefore already off the
   * result set: PULL must emit it before pulling anything else.
   */
  Result firstResult;

  /** Rows of a system query (SHOW DATABASES, CALL dbms.components(), ...) answered without the engine. */
  List<List<Object>> syntheticResults;

  boolean writeOperation;

  long queryStartTime;  // System.nanoTime() when execution started, for the t_first/t_last metadata
  long firstRecordTime; // System.nanoTime() when the first row became available, 0 if there was none

  /** EXPLAIN/PROFILE plan surfaced in the PULL/DISCARD SUCCESS metadata, and the key it goes under. */
  Map<String, Object> planMetadata;
  String              planMetadataKey;

  BoltQueryStream(final long qid) {
    this.qid = qid;
  }

  /**
   * True while this stream still owes the client rows. A synthetic stream is done when its buffered rows run
   * out; an engine-backed one when neither the peeked first row nor the result set has anything left.
   */
  boolean hasMore() {
    if (syntheticResults != null)
      return !syntheticResults.isEmpty();
    return firstResult != null || (resultSet != null && resultSet.hasNext());
  }

  /**
   * Releases the engine resources behind this stream. Never throws: it runs on the teardown paths (PULL
   * completion, DISCARD, RESET, ROLLBACK, LOGOFF, connection close) where a failure to close one stream must
   * not stop the others from being released.
   */
  void close(final Object requester, final String phase) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (final Exception e) {
        LogManager.instance().log(requester, Level.WARNING, "Failed to close ResultSet during " + phase, e);
      }
      resultSet = null;
    }
    fields = null;
    firstResult = null;
    syntheticResults = null;
    planMetadata = null;
    planMetadataKey = null;
  }
}
