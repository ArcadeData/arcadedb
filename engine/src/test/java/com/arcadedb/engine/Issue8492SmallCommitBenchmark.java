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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;

/**
 * Issue #8492: the TPC-C-style new-order of the report through the record API - a lookup by a unique key, an insert,
 * an update of the record looked up, commit - timed per commit. A small transaction whose commit dominates it: what
 * the commit does per modified page (compression, the read-cache publication) is what this measures. The timings are
 * logged at INFO: run it with a log configuration that shows INFO for {@code com.arcadedb} to read them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class Issue8492SmallCommitBenchmark extends TestHelper {
  private static final int PARTS  = 200_000;
  private static final int WARMUP = 5_000;
  private static final int OPS    = 30_000;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 0);
    database.command("sql", "CREATE DOCUMENT TYPE Part");
    database.command("sql", "CREATE PROPERTY Part.p_partkey LONG");
    database.command("sql", "CREATE INDEX ON Part (p_partkey) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE OrderNew");
    database.command("sql", "CREATE PROPERTY OrderNew.okey LONG");
    database.command("sql", "CREATE INDEX ON OrderNew (okey) UNIQUE");
    database.begin();
    for (int k = 0; k < PARTS; k++) {
      database.newDocument("Part").set("p_partkey", (long) k).set("p_retailprice", 900.0 + k % 1000).set("stock", 100L).save();
      if ((k + 1) % 10_000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
  }

  @Test
  void newOrderCommit() {
    final Random rnd = new Random(11);
    final long[] commitNs = new long[OPS];
    final long[] txNs = new long[OPS];
    for (int i = 0; i < WARMUP + OPS; i++) {
      final long pkey = rnd.nextInt(PARTS);
      final long t0 = System.nanoTime();
      database.begin();
      final IndexCursor c = database.lookupByKey("Part", "p_partkey", pkey);
      final Document part = c.next().asDocument();
      part.getDouble("p_retailprice");
      database.newDocument("OrderNew").set("okey", (long) i).set("pkey", pkey).set("qty", 1).set("paid", 0).save();
      final MutableDocument m = part.modify();
      m.set("stock", m.getLong("stock") - 1).save();
      final long t1 = System.nanoTime();
      database.commit();
      final long t2 = System.nanoTime();
      if (i >= WARMUP) {
        commitNs[i - WARMUP] = t2 - t1;
        txNs[i - WARMUP] = t2 - t0;
      }
    }
    Arrays.sort(commitNs);
    Arrays.sort(txNs);
    LogManager.instance().log(this, Level.INFO, "Issue #8492 new-order: commit p50 %.4f ms p99 %.4f ms, transaction p50 %.4f ms", null,
        commitNs[OPS / 2] / 1e6, commitNs[(int) (OPS * 0.99)] / 1e6, txNs[OPS / 2] / 1e6);
  }
}
