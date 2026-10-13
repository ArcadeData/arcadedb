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
package com.arcadedb.exception;

/**
 * Thrown when a change was handed to replication but its outcome could not be learned: it may still commit cluster-wide,
 * or never. Neither safe to retry blindly (a retry runs the change twice whenever the first one does commit) nor to
 * undo locally (an undo leaves this node on the previous value whenever it does commit). The caller has to check
 * whether its change landed.
 * <p>
 * The engine-side name for an outcome the HA layer cannot confirm, so engine code can tell it apart from a refusal
 * without depending on the HA module (issue #9763).
 */
public class TransactionOutcomeUnknownException extends TransactionException {
  public TransactionOutcomeUnknownException(final String message) {
    super(message);
  }
}
