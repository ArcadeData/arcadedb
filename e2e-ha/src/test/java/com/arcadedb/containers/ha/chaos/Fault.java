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

import java.util.Random;

/**
 * One kind of failure. {@link #inject} picks targets from the seeded random and impairs them; {@link #heal} restores
 * exactly those targets. Only one fault is active at a time.
 */
public interface Fault {
  String name();

  boolean canApply(ClusterState state);

  /** @return the role and targets, for the step log, e.g. {@code "LEADER [2]"} */
  String inject(ClusterState state, NodeControl control, Random random) throws Exception;

  void heal(ClusterState state, NodeControl control) throws Exception;

  /** True when a majority stays connected, so acknowledged writes must keep flowing once a leader is elected. */
  boolean expectsWritesAvailable();
}
