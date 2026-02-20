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
package com.arcadedb.server.ha;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Report of consistency check results for a database.
 * Tracks records that have different checksums across replicas (data drift).
 */
public class ConsistencyReport {
  private final String databaseName;
  private final long sampleSize;
  private final List<RecordDrift> drifts;

  /**
   * Creates a new consistency report.
   *
   * @param databaseName the name of the database checked
   * @param sampleSize   the number of records sampled
   */
  public ConsistencyReport(final String databaseName, final long sampleSize) {
    this.databaseName = databaseName;
    this.sampleSize = sampleSize;
    this.drifts = new ArrayList<>();
  }

  /**
   * Records a detected drift (inconsistency) for a specific record.
   *
   * @param rid                the RID of the inconsistent record
   * @param checksumsByReplica map of replica name to checksum for this record
   */
  public void recordDrift(final RID rid, final Map<String, byte[]> checksumsByReplica) {
    drifts.add(new RecordDrift(rid, checksumsByReplica));
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Gets the sample size (number of records checked).
   *
   * @return the sample size
   */
  public long getSampleSize() {
    return sampleSize;
  }

  /**
   * Gets the number of drifts detected.
   *
   * @return the drift count
   */
  public int getDriftCount() {
    return drifts.size();
  }

  /**
   * Gets the list of detected drifts (immutable view).
   *
   * @return unmodifiable list of drifts
   */
  public List<RecordDrift> getDrifts() {
    return Collections.unmodifiableList(drifts);
  }

  /**
   * Represents a single record that has inconsistent checksums across replicas.
   *
   * @param rid                the RID of the inconsistent record
   * @param checksumsByReplica map of replica name to checksum bytes
   */
  public record RecordDrift(RID rid, Map<String, byte[]> checksumsByReplica) {
  }
}
