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
  protected final AtomicLong txCommits     = new AtomicLong();
  protected final AtomicLong txRollbacks   = new AtomicLong();
  protected final AtomicLong createRecord  = new AtomicLong();
  protected final AtomicLong readRecord    = new AtomicLong();
  protected final AtomicLong updateRecord  = new AtomicLong();
  protected final AtomicLong deleteRecord  = new AtomicLong();
  protected final AtomicLong queries       = new AtomicLong();
  protected final AtomicLong commands      = new AtomicLong();
  protected final AtomicLong scanType      = new AtomicLong();
  protected final AtomicLong scanBucket    = new AtomicLong();
  protected final AtomicLong iterateType   = new AtomicLong();
  protected final AtomicLong iterateBucket = new AtomicLong();
  protected final AtomicLong countType     = new AtomicLong();
  protected final AtomicLong countBucket   = new AtomicLong();

  public Map<String, Object> toMap() {
    final Map<String, Object> map = new HashMap<>();
    map.put("txCommits", txCommits.get());
    map.put("txRollbacks", txRollbacks.get());
    map.put("createRecord", createRecord.get());
    map.put("readRecord", readRecord.get());
    map.put("updateRecord", updateRecord.get());
    map.put("deleteRecord", deleteRecord.get());
    map.put("queries", queries.get());
    map.put("commands", commands.get());
    map.put("scanType", scanType.get());
    map.put("scanBucket", scanBucket.get());
    map.put("iterateType", iterateType.get());
    map.put("iterateBucket", iterateBucket.get());
    map.put("countType", countType.get());
    map.put("countBucket", countBucket.get());
    return map;
  }
}
