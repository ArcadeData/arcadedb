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

/**
 * What the runner may ask of the server a workload runs against, between cases: whether it is still alive, a fresh one when it
 * is not, and a thread dump of it. One OOM must not fail every later case of the workload because nothing restarted the server.
 */
public interface ServerGuard {
  /**
   * Null when the server answers, else why it does not: {@code "OOMKilled"}, {@code "exit code 137"},
   * {@code "not answering /api/v1/ready"}.
   */
  String deathReason();

  /** Closes the dead server, starts a fresh one with the same image and settings and returns the context to go on with. */
  WorkloadContext restart();

  /** Writes thread dump number {@code n} of the server; false when none could be taken. */
  boolean threadDump(int n);
}
