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
package com.arcadedb.database;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseStats {
  public final AtomicLong writeTx       = new AtomicLong();
  public final AtomicLong readTx        = new AtomicLong();
  public final AtomicLong txRollbacks   = new AtomicLong();
  public final AtomicLong createRecord  = new AtomicLong();
  public final AtomicLong readRecord    = new AtomicLong();
  public final AtomicLong updateRecord  = new AtomicLong();
  public final AtomicLong deleteRecord  = new AtomicLong();
  public final AtomicLong existsRecord  = new AtomicLong();
  public final AtomicLong queries       = new AtomicLong();
  public final AtomicLong commands      = new AtomicLong();
  public final AtomicLong scanType      = new AtomicLong();
  public final AtomicLong scanBucket    = new AtomicLong();
  public final AtomicLong iterateType   = new AtomicLong();
  public final AtomicLong iterateBucket = new AtomicLong();
  public final AtomicLong countType     = new AtomicLong();
  public final AtomicLong countBucket   = new AtomicLong();

  public Map<String, Object> toMap() {
    final Map<String, Object> map = new HashMap<>(Map.ofEntries(
        Map.entry("writeTx", writeTx.get()),
        Map.entry("readTx", readTx.get()),
        Map.entry("txRollbacks", txRollbacks.get()),
        Map.entry("createRecord", createRecord.get()),
        Map.entry("readRecord", readRecord.get()),
        Map.entry("updateRecord", updateRecord.get()),
        Map.entry("deleteRecord", deleteRecord.get()),
        Map.entry("existsRecord", existsRecord.get()),
        Map.entry("queries", queries.get()),
        Map.entry("commands", commands.get()),
        Map.entry("scanType", scanType.get()),
        Map.entry("scanBucket", scanBucket.get()),
        Map.entry("iterateType", iterateType.get()),
        Map.entry("iterateBucket", iterateBucket.get()),
        Map.entry("countType", countType.get()),
        Map.entry("countBucket", countBucket.get())));
    return map;
  }
}
