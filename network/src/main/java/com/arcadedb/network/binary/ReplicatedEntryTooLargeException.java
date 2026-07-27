/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.network.binary;

import com.arcadedb.exception.TransactionException;

/**
 * Thrown when a transaction (or a DDL/compaction schema change) would produce a single Raft log
 * entry larger than the cluster can replicate.
 * <p>
 * Deliberately NOT a {@code NeedRetryException}: the size of an entry is deterministic, so retrying
 * the identical payload can never succeed. Worse, dispatching it anyway makes the Ratis leader step
 * down ({@code StateMachineException.leaderShouldStepDown()}), and a retry loop then topples every
 * newly elected leader in turn - an unbounded election-churn cascade in which the write never lands
 * (issue #4743). Failing the caller once, loudly, is the only safe outcome: the payload must be
 * split, or the limit raised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicatedEntryTooLargeException extends TransactionException {
  public ReplicatedEntryTooLargeException(final String s) {
    super(s);
  }
}
