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

/**
 * One line of {@code steps.log}. {@code ackedDuringHold} is -1 for faults that do not expect writes to stay available.
 */
public record StepRecord(int step, String fault, String targets, int leaderBefore, int leaderAfter, long timeToLeaderMs,
                         long convergenceMs, long ackedDuringHold, long acked, long unknown, long failed) {
  public String toLine() {
    return "step=" + step + " fault=" + fault + " targets=" + targets + " leader=" + leaderBefore + "->" + leaderAfter
        + " timeToLeaderMs=" + timeToLeaderMs + " convergenceMs=" + convergenceMs + " ackedDuringHold=" + ackedDuringHold
        + " acked=" + acked + " unknown=" + unknown + " failed=" + failed;
  }
}
