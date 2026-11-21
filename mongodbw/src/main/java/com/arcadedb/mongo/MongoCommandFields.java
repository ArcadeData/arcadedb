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

import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The fields of the commands the MongoDB plugin runs itself, and what it does with each (issue #11778). Every handler used to read
 * the fields it implements and answer {@code ok:1} whatever else the command carried, so a {@code collation}, a {@code hint} with a
 * {@code max}, a {@code returnKey} or a misspelled field was acknowledged and ignored, and the client was handed the result of
 * another command than the one it sent. {@code createIndexes} already refuses what it cannot honour (issue #11446): this is the
 * same rule for {@code find}, {@code count}, {@code aggregate}, {@code update}, {@code delete} and {@code create}.
 * <p>
 * A field of a command is one of:
 * <ul>
 * <li><b>accepted</b>: the plugin applies it, or it carries nothing a single node could apply differently (the size of a batch
 * when every document is answered in the first one, a hint to spill to disk);</li>
 * <li><b>generic</b>: one of the arguments {@code mongod} takes on every command, which drivers add on their own (the session, the
 * database, the concerns, the versioned API);</li>
 * <li><b>unsupported</b>: {@code mongod} has it and the plugin does not. Refused with {@code 2 BadValue} naming it, unless its
 * value asks for the default, which is asking for nothing (see {@link #asksForNothing});</li>
 * <li>anything else is unknown, and refused as {@code mongod} refuses it: {@code 40415}, "BSON field 'find.x' is an unknown
 * field".</li>
 * </ul>
 * The tables are keyed by the context {@code mongod} names in its own refusals: the command, or {@code update.updates} and
 * {@code delete.deletes} for the statements of a write.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
final class MongoCommandFields {
  private static final int UNKNOWN_FIELD = 40415;

  /**
   * The arguments {@code mongod} accepts on every command (its {@code generic_argument.idl}). Drivers, shells and proxies add them
   * without being asked, so refusing one would refuse the connection rather than an option.
   */
  private static final Set<String> GENERIC = Set.of("$db", "$readPreference", "$clusterTime", "$audit", "$client", "$configServerState",
      "$oplogQueryData", "$queryOptions", "$replData", "$configTime", "$topologyTime", "$tenant", "apiVersion", "apiStrict",
      "apiDeprecationErrors", "maxTimeMS", "maxTimeMSOpOnly", "usesDefaultMaxTimeMS", "readConcern", "writeConcern", "lsid",
      "clientOperationKey", "txnNumber", "txnRetryCounter", "autocommit", "startTransaction", "startOrContinueTransaction", "stmtId",
      "comment", "allowImplicitCollectionCreation", "databaseVersion", "shardVersion", "tracking_info", "coordinator",
      "mayBypassWriteBlocking", "expectPrefix", "requestGossipRoutingCache", "serialization_context", "versionContext");

  /** The boolean fields whose default is {@code false}: sent as {@code false} they ask for nothing. */
  private static final Set<String> FALSE_IS_DEFAULT = Set.of("tailable", "awaitData", "returnKey", "showRecordId", "explain", "capped",
      "upsertSupplied", "temp", "isTimeseriesNamespace", "needsMerge", "fromMongos", "fromRouter", "isMapReduceCommand", "readOnce",
      "allowSpeculativeMajorityRead", "recordPreImages", "bypassEmptyTsReplacement", "rawData", "mirrored",
      "isClusterQueryWithoutShardKeyCmd", "recordIdsReplicated", "$_requestResumeToken", "$_requestReshardingResumeToken");

  /** The document and array fields that mean nothing when they are empty. */
  private static final Set<String> EMPTY_IS_DEFAULT = Set.of("hint", "min", "max", "let", "arrayFilters", "validator", "storageEngine",
      "indexOptionDefaults");

  private record Table(Set<String> accepted, Set<String> unsupported, boolean command) {
  }

  private static final Map<String, Table> TABLES = Map.of(
      // every document is answered in the first batch with no cursor left open, so the fields that size or keep a cursor ask for
      // nothing more; allowPartialResults is about unreachable shards and oplogReplay is ignored by mongod itself
      "find", new Table(
          Set.of("find", "filter", "sort", "projection", "skip", "limit", "batchSize", "singleBatch", "allowDiskUse", "noCursorTimeout",
              "allowPartialResults", "oplogReplay"),
          Set.of("collation", "hint", "min", "max", "returnKey", "showRecordId", "tailable", "awaitData", "let", "collectionUUID",
              "encryptionInformation", "readOnce", "allowSpeculativeMajorityRead", "term", "querySettings", "rawData", "mirrored",
              "sampleId", "includeQueryStatsMetrics", "$_requestResumeToken", "$_resumeAfter", "$_startAt"), true),
      // "fields" is a leftover mongod itself declares ignored
      "count", new Table(
          Set.of("count", "query", "limit", "skip", "fields"),
          Set.of("collation", "hint", "collectionUUID", "encryptionInformation", "rawData", "mirrored", "sampleId",
              "includeQueryStatsMetrics"), true),
      "aggregate", new Table(
          Set.of("aggregate", "pipeline", "cursor", "allowDiskUse", "bypassDocumentValidation"),
          Set.of("explain", "collation", "hint", "let", "collectionUUID", "encryptionInformation", "needsMerge", "fromMongos",
              "fromRouter", "exchange", "runtimeConstants", "isMapReduceCommand", "querySettings", "passthroughToShard", "rawData",
              "sampleId", "includeQueryStatsMetrics", "isClusterQueryWithoutShardKeyCmd", "$_requestReshardingResumeToken",
              "$_requestResumeToken", "$_resumeAfter", "$_generateV2ResumeTokens", "$_passthroughToShard"), true),
      "update", new Table(
          Set.of("update", "updates", "ordered", "bypassDocumentValidation"),
          Set.of("let", "runtimeConstants", "collectionUUID", "encryptionInformation", "isTimeseriesNamespace",
              "bypassEmptyTsReplacement", "stmtIds", "sampleId", "$_originalQuery", "$_originalCollation"), true),
      "update.updates", new Table(
          Set.of("q", "u", "multi", "upsert"),
          Set.of("collation", "arrayFilters", "hint", "c", "sort", "upsertSupplied", "sampleId",
              "$_allowShardKeyUpdatesWithoutFullShardKeyInQuery"), false),
      "delete", new Table(
          Set.of("delete", "deletes", "ordered"),
          Set.of("let", "runtimeConstants", "collectionUUID", "encryptionInformation", "isTimeseriesNamespace",
              "bypassEmptyTsReplacement", "stmtIds", "sampleId", "$_originalQuery", "$_originalCollation"), true),
      "delete.deletes", new Table(
          Set.of("q", "limit"),
          Set.of("collation", "hint", "sampleId"), false),
      "create", new Table(
          Set.of("create"),
          Set.of("capped", "autoIndexId", "idIndex", "size", "max", "storageEngine", "validator", "validationLevel", "validationAction",
              "indexOptionDefaults", "viewOn", "pipeline", "collation", "changeStreamPreAndPostImages", "timeseries", "clusteredIndex",
              "recordPreImages", "expireAfterSeconds", "encryptedFields", "temp", "flags", "recordIdsReplicated"), true));

  private MongoCommandFields() {
  }

  /**
   * Refuses the first field of {@code document} the plugin would otherwise ignore: an unsupported one that asks for something, or
   * one {@code mongod} does not have either.
   *
   * @param context the command ({@code find}), or the statements of a write ({@code update.updates}, {@code delete.deletes})
   */
  static void check(final String context, final Document document) {
    final Table table = TABLES.get(context);
    for (final Map.Entry<String, Object> entry : document.entrySet()) {
      final String field = entry.getKey();
      if (table.accepted.contains(field) || (table.command && GENERIC.contains(field)))
        continue;
      if (!table.unsupported.contains(field))
        throw new MongoServerError(UNKNOWN_FIELD, "Location" + UNKNOWN_FIELD, "BSON field '" + context + "." + field + "' is an unknown field.");
      if (!asksForNothing(field, entry.getValue()))
        throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(),
            "Field '" + field + "' of '" + context + "' is not supported by ArcadeDB: the command would run as if it were not there");
    }
  }

  /**
   * Whether an unsupported field carries its default, which the plugin honours by doing nothing: a null, the {@code false} of a flag
   * that is off by default (the Java driver sends {@code capped: false} with every {@code create}), an empty {@code hint} or
   * {@code let}, the {@code simple} collation, which is the binary comparison the plugin applies, the {@code strict} and {@code error}
   * a collection validates with when it is given no other.
   */
  private static boolean asksForNothing(final String field, final Object value) {
    return switch (value) {
      case null -> true;
      case Boolean flag -> "autoIndexId".equals(field) ? flag : !flag && FALSE_IS_DEFAULT.contains(field);
      // mongod reads a number as a flag too
      case Number flag -> "autoIndexId".equals(field) ? isSet(flag) : !isSet(flag) && FALSE_IS_DEFAULT.contains(field);
      case Document document when document.isEmpty() -> EMPTY_IS_DEFAULT.contains(field);
      case Document document -> "collation".equals(field) && document.size() == 1 && "simple".equals(document.get("locale"));
      case List<?> list -> list.isEmpty() && EMPTY_IS_DEFAULT.contains(field);
      // the defaults of a collection without a validator, which is the only kind created: the validator itself is refused
      case String text -> "validationLevel".equals(field) && "strict".equals(text) || "validationAction".equals(field) && "error".equals(text);
      default -> false;
    };
  }

  /**
   * A number read as a flag: anything but zero sets it. A Decimal128 is compared as a decimal, since narrowed to a double one as
   * small as 1E-6176 is zero, and a flag that is set would read as its default.
   */
  private static boolean isSet(final Number flag) {
    return flag instanceof Decimal128 decimal ? decimal.compareTo(Decimal128.POSITIVE_ZERO) != 0 : Utils.isTrue(flag);
  }

  /** The contexts that have a table: read by the census that sends every field of each (issue #11778). */
  static Set<String> contexts() {
    return TABLES.keySet();
  }

  static Set<String> accepted(final String context) {
    return TABLES.get(context).accepted;
  }

  static Set<String> unsupported(final String context) {
    return TABLES.get(context).unsupported;
  }

  /** The arguments accepted on every command, and on no statement of a write. */
  static Set<String> generic() {
    return GENERIC;
  }
}
