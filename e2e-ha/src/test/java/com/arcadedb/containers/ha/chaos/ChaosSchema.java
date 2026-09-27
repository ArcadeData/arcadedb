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

package com.arcadedb.containers.ha.chaos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema and statements of the chaos workload. {@code id} is the ledger key; {@code w} and {@code s} repeat the writer
 * and sequence for humans reading a dump. A pair operation adds, in the same transaction, a {@code NEXT} edge from the
 * new vertex to an earlier acknowledged one, so a partially applied transaction shows up as a vertex without its edge.
 * If the target were missing on the leader the edge would not be created; the checker then reports I1 for the target
 * and I5 for the new vertex, both real bugs because the target was acknowledged.
 */
public final class ChaosSchema {
  public static final String DATABASE  = "chaos";
  public static final int    PAGE_SIZE = 20_000;

  public static final List<String> DDL = List.of(
      "CREATE VERTEX TYPE ChaosOp IF NOT EXISTS",
      "CREATE PROPERTY ChaosOp.id IF NOT EXISTS LONG",
      "CREATE INDEX IF NOT EXISTS ON ChaosOp (id) UNIQUE",
      "CREATE EDGE TYPE NEXT IF NOT EXISTS");

  public static final String INSERT_SINGLE = "INSERT INTO ChaosOp SET id = :id, w = :w, s = :s, pair = false";

  public static final String INSERT_PAIR = """
      BEGIN;
      LET a = CREATE VERTEX ChaosOp SET id = :id, w = :w, s = :s, pair = true;
      LET b = SELECT FROM ChaosOp WHERE id = :target;
      CREATE EDGE NEXT FROM $a TO $b;
      COMMIT;""";

  public static final String COUNT_OPS   = "SELECT count(*) AS c FROM ChaosOp";
  public static final String COUNT_EDGES = "SELECT count(*) AS c FROM NEXT";

  private ChaosSchema() {
  }

  /** A page of records read from the buckets in RID order after {@code afterRid} (null for the first page). */
  public static String recordPage(final String afterRid, final int size) {
    if (afterRid != null && !afterRid.matches("#\\d+:\\d+"))
      throw new IllegalArgumentException("Not a RID: " + afterRid);
    return "SELECT @rid AS rid, id FROM ChaosOp" + (afterRid == null ? "" : " WHERE @rid > " + afterRid) + " ORDER BY @rid LIMIT "
        + size;
  }

  public static String page(final int size) {
    return "SELECT id, out('NEXT').size() AS e FROM ChaosOp WHERE id > :last ORDER BY id LIMIT " + size;
  }

  public static Map<String, Object> singleParams(final long key) {
    final Map<String, Object> params = new HashMap<>();
    params.put("id", key);
    params.put("w", Ledger.writerOf(key));
    params.put("s", Ledger.seqOf(key));
    return params;
  }

  public static Map<String, Object> pairParams(final long key, final long target) {
    final Map<String, Object> params = singleParams(key);
    params.put("target", target);
    return params;
  }
}
