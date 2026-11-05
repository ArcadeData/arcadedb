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
package com.arcadedb.census;

import com.arcadedb.index.vector.GroupAdmissionState;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;

/**
 * Never run: compiled only so {@link AdmissionSurfaceCensusTest#theCensusTellsARegistrationFromItsName} can read the shapes a
 * surface takes. Top-level classes, because a surface is a top-level class.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AdmissionProbes {
  private AdmissionProbes() {
  }
}

/** The helper a surface inherits, as the HTTP handlers inherit {@code registerRunningQuery}. */
abstract class RegistersThroughAHelperProbe {
  protected RunningQuery registerRunningQuery(final RunningQueryRegistry registry) {
    return registry.register("db", "user", "probe", null, null, RunningQuery.Client.NONE);
  }
}

/** Admits, and opens its entry through the inherited helper. */
final class AdmitsAndRegistersProbe extends RegistersThroughAHelperProbe {
  void run(final RunningQueryRegistry registry) {
    try (final QueryAdmissionGate.Ticket ticket = QueryAdmissionGate.getInstance().admit()) {
      registerRunningQuery(registry);
    }
  }
}

/** Admits, and calls a helper that is merely NAMED like the registration: the source-text census took it for one. */
final class AdmitsAndRegistersNothingProbe {
  void run() {
    try (final QueryAdmissionGate.Ticket ticket = QueryAdmissionGate.getInstance().admit()) {
      registerRunningQuery();
    }
  }

  private void registerRunningQuery() {
  }
}

/** An {@code admit} of another class: no surface. */
final class AdmitsOnAnotherGateProbe {
  boolean run(final GroupAdmissionState state) {
    return state.admit("group");
  }
}
