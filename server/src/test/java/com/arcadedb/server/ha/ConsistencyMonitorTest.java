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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ConsistencyReport data structure.
 */
public class ConsistencyMonitorTest {

  @Test
  public void testConsistencyReportTracksNoDrift() {
    final ConsistencyReport report = new ConsistencyReport("testDB", 1000);

    assertEquals("testDB", report.getDatabaseName());
    assertEquals(1000, report.getSampleSize());
    assertEquals(0, report.getDriftCount());
    assertTrue(report.getDrifts().isEmpty());
  }

  @Test
  public void testConsistencyReportTracksDrift() {
    final ConsistencyReport report = new ConsistencyReport("testDB", 1000);

    final RID rid = new RID(null, 1, 100);
    final Map<String, byte[]> checksums = new HashMap<>();
    checksums.put("server1", new byte[]{1, 2, 3});
    checksums.put("server2", new byte[]{4, 5, 6});

    report.recordDrift(rid, checksums);

    assertEquals(1, report.getDriftCount());
    assertEquals(1, report.getDrifts().size());

    final ConsistencyReport.RecordDrift drift = report.getDrifts().get(0);
    assertEquals(rid, drift.rid());
    assertEquals(2, drift.checksumsByReplica().size());
  }

  @Test
  public void testMultipleDrifts() {
    final ConsistencyReport report = new ConsistencyReport("testDB", 1000);

    // Record first drift
    final RID rid1 = new RID(null, 1, 100);
    final Map<String, byte[]> checksums1 = new HashMap<>();
    checksums1.put("server1", new byte[]{1, 2, 3});
    checksums1.put("server2", new byte[]{4, 5, 6});
    report.recordDrift(rid1, checksums1);

    // Record second drift
    final RID rid2 = new RID(null, 2, 200);
    final Map<String, byte[]> checksums2 = new HashMap<>();
    checksums2.put("server1", new byte[]{7, 8, 9});
    checksums2.put("server2", new byte[]{10, 11, 12});
    report.recordDrift(rid2, checksums2);

    assertEquals(2, report.getDriftCount());
    assertEquals(2, report.getDrifts().size());
  }
}
