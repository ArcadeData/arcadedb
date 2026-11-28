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
package com.arcadedb.bench.runner;

import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.serializer.json.JSONObject;

/**
 * What a workload receives from the runner. The server may be null in unit tests, and is null for a workload that
 * {@link Workload#ownsServers() owns its servers}: that one starts them from {@code spec}, which is null for the others.
 */
public record WorkloadContext(ServerUnderTest server, Scale scale, long seed, JSONObject params, ServerSpec spec) {
  /** The run's server options, for a workload that starts its own servers. */
  public record ServerSpec(String image, String serverCpus, int memLimitGb, String heap) {
  }

  public WorkloadContext(final ServerUnderTest server, final Scale scale, final long seed, final JSONObject params) {
    this(server, scale, seed, params, null);
  }
}
