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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.runner.Workload;

import java.util.List;

/**
 * The workloads of the suite in a fixed order: ingestion, graph-oltp, vector, fulltext, facet, timeseries, correctness,
 * protocols, issues, ramp, hybrid-rag, mixed, soak, ha. Workloads register here as they are added; ha stays last.
 */
public final class WorkloadRegistry {
  private WorkloadRegistry() {
  }

  public static List<Workload> all() {
    return List.of(new IngestionWorkload(), new GraphOltpWorkload(), new VectorWorkload(), new FullTextWorkload(),
        new FacetWorkload(), new TimeSeriesWorkload(), new CorrectnessWorkload(), new ProtocolsWorkload(),
        new IssuesWorkload(), new RampWorkload(), new HybridRagWorkload(), new MixedWorkload(),
        new SoakWorkload(), new HaWorkload());
  }
}
