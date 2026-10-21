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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.TimeSeriesType;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@code insert} command of the MongoDB wire protocol over a TIMESERIES type (issue #10476). Its samples are not records, so
 * the record path - which gives every collection the unique index on {@code _id} - cannot serve them, and failed with
 * {@code NamespaceNotFound} on a collection that exists. A sample is appended through the engine of the type, by the same
 * {@code INSERT INTO <type> CONTENT ...} SQL runs: the column rules, the policy on undeclared keys, the constraints and the unit of
 * a timestamp (a number is epoch milliseconds, a date is the instant it names) are SQL's, so the sample a collection takes is the
 * one {@code INSERT} would have taken, and the one {@code find} and {@code deleteMany} then read.
 * <p>
 * A sample has no identity, so there is no {@code _id} to store: the ObjectId a driver generates for a document that has none is
 * dropped, and any other {@code _id} is refused rather than lost.
 * <p>
 * Like SQL's, the append is not part of a transaction: every sample is durable when its statement returns. So a batch is not
 * atomic, and one the engine refuses is reported by its index in {@code writeErrors}, as MongoDB does for a document it
 * rejects: an ordered batch stops there, an unordered one goes on. A command that fails midway (the server's own fault) leaves the
 * samples before it stored, and a client that retries the whole {@code insert} writes them again: a sample has no {@code _id} to
 * tell it was written, so retry only from the index the reply names.
 * <p>
 * The statement text is the same for every sample of a type, so the engine's statement cache parses it once. What a batch costs
 * is a statement and a shard commit per sample, so the throughput of an {@code insertMany} is the one of that many single
 * {@code INSERT}s: a client that ingests large volumes of samples is better served by the line protocol or the HTTP time-series
 * endpoint, which append a batch to the engine in one call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoTimeSeriesInserts {
  /** What a {@link CommandExecutionException} wraps when the server failed, and not the document: the disk, or the code itself. */
  private static final List<Class<? extends Throwable>> SERVER_FAULTS = List.of(IOException.class, NullPointerException.class,
      IllegalStateException.class, ClassCastException.class, IndexOutOfBoundsException.class);

  private MongoTimeSeriesInserts() {
  }

  /**
   * Appends the samples of {@code documents}, one statement each. SQL has no batch form that reports a refused sample by its index
   * and carries its column rules (a bulk append through the ingestion gateway would read a number in the unit of the column
   * instead of in milliseconds), and the engine commits the shard transaction of each sample on its own either way, so a batch
   * costs what its samples cost.
   *
   * @param writeErrors receives one entry for every document that was refused, with its {@code index}
   *
   * @return how many samples the engine stored
   *
   * @throws RuntimeException when the engine failed for a reason that is not the document's (an I/O error): the whole command fails
   */
  static int insert(final Database database, final TimeSeriesType type, final List<Document> documents, final boolean ordered,
      final List<Document> writeErrors) {
    final String sql = "INSERT INTO " + Identifier.quote(type.getName()) + " CONTENT :sample";

    int stored = 0;
    for (int i = 0; i < documents.size(); i++) {
      try {
        try (final ResultSet rs = database.command("sql", sql, Map.of("sample", sampleOf(type, documents.get(i))))) {
          while (rs.hasNext()) {
            rs.next();
            ++stored;
          }
        }
      } catch (final RuntimeException e) {
        writeErrors.add(writeError(i, refusal(e)));
        if (ordered)
          break;
      }
    }
    return stored;
  }

  /**
   * The document without its {@code _id}: a sample is identified by its time and its series, not by a key.
   */
  private static Map<String, Object> sampleOf(final TimeSeriesType type, final Document document) {
    final Map<String, Object> sample = LinkedHashMap.newLinkedHashMap(document.size());
    for (final Map.Entry<String, Object> entry : document.entrySet()) {
      if ("_id".equals(entry.getKey())) {
        // the driver puts an ObjectId in a document that has no _id; one the client chose would be lost, so it is refused
        if (!(entry.getValue() instanceof ObjectId))
          throw new MongoServerError(ErrorCode.BadValue, "Cannot insert the _id " + entry.getValue() + " into the TIMESERIES type '"
              + type.getName() + "': a sample has no _id, it is identified by its '" + type.getTimestampColumn()
              + "' and its TAG values. Remove the _id from the document");
        continue;
      }
      sample.put(entry.getKey(), entry.getValue());
    }
    return sample;
  }

  /**
   * The failure as MongoDB reports a document it rejects, as a write error with its index. Only what says the document is wrong is
   * one: our own refusal of its {@code _id}, a value the type refuses ({@link ErrorCategory#VALIDATION}), and the engine's refusal
   * of a sample (an undeclared key, a value the column cannot hold), which carries no type of its own and so is a {@link
   * CommandExecutionException} that wraps no fault of the server (see {@link #SERVER_FAULTS}). That last one is a default, not a
   * proof: the engine gives the refusal of a sample no type of its own, so a cause the list does not know is taken for the
   * document's. The list names what a bug of the code looks like, and a test pins both sides of it.
   * <p>
   * Everything else is not the document's, and fails the command with the code MongoDB gives it, the samples before it stored:
   * a conflict to retry, a timeout, a stop on request, a lack of permission, a schema that changed meanwhile, an I/O error, a
   * fault of the code. An ordered batch does not stop on those as on a bad sample, and a transient failure is not blamed on the
   * client's data.
   *
   * @throws RuntimeException {@code e} itself, when it is not a refusal of the document
   */
  static MongoServerError refusal(final RuntimeException e) {
    if (e instanceof MongoServerError coded)
      return coded;
    return switch (ErrorCategory.of(e)) {
      case VALIDATION -> new MongoServerError(ErrorCode.BadValue, e.getMessage());
      case SERVER -> {
        if (e instanceof CommandExecutionException && !isFaultOfTheServer(e))
          yield new MongoServerError(ErrorCode.BadValue, e.getMessage());
        throw e;
      }
      default -> throw e;
    };
  }

  private static boolean isFaultOfTheServer(final RuntimeException e) {
    for (final Class<? extends Throwable> fault : SERVER_FAULTS)
      if (CauseChain.contains(e, fault))
        return true;
    return false;
  }

  private static Document writeError(final int index, final MongoServerError error) {
    final Document writeError = new Document();
    writeError.put("index", index);
    writeError.put("code", error.getCode());
    writeError.putIfNotNull("codeName", error.getCodeName());
    writeError.put("errmsg", error.getMessage());
    return writeError;
  }
}
