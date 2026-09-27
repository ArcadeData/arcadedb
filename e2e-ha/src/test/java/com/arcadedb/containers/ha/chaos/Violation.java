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

import java.util.List;

/**
 * One violated invariant, with up to {@link InvariantChecker#MAX_KEYS} offending ledger keys and, when the checker
 * knows more than the key, one human-readable detail line per key (outcome, and which nodes hold it).
 */
public record Violation(ResultKind kind, String invariant, String message, long[] keys, List<String> details) {
  public Violation(final ResultKind kind, final String invariant, final String message, final long[] keys) {
    this(kind, invariant, message, keys, List.of());
  }

  public String describe() {
    return invariant + " (" + kind + "): " + message;
  }
}
