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
package com.arcadedb.bench.gate;

/**
 * The outcome for the newest point of one trigger on one line. {@code baseline} is null when there is none (then the kind is
 * NOT_JUDGED and {@code change} and {@code band} are 0). {@code change} is relative, positive = worse.
 */
public record Verdict(LineKey key, String trigger, Kind kind, Point newest, Point baseline, double change, double band) {

  public enum Kind {
    WORSE, BETTER, SAME, NOT_JUDGED
  }
}
