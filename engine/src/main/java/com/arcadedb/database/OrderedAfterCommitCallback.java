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
package com.arcadedb.database;

/**
 * An after-commit callback that must run in the order its transaction committed, relative to the callbacks of the transactions
 * that overlap it (issue #10249).
 * <p>
 * An ordinary callback runs on the committing thread once the transaction has released its file locks, so two transactions that
 * conflict on a file - the second one only able to commit after the first has - can run their callbacks in either order: the
 * second thread may get there before the first. For a callback that mirrors the commit into state of its own, that state then
 * sees the second transaction's change first. A record slot freed by the first and reused by the second is the example: the
 * mirror sees the new record created before the old one is deleted, and deletes the new one.
 * <p>
 * A callback of this kind is told when its transaction reaches the point in the commit where it holds every file lock it needs
 * and can no longer be refused ({@link #onCommitPoint()}). Two transactions that conflict reach that point one after the other,
 * so whatever the callback does there to take a place in line is in the order they committed. It is then told either that its
 * transaction went on to commit, by {@link #run()}, or that it did not, by {@link #onAbandoned()}: exactly one of the two. A
 * callback registered by a transaction that never reaches the commit point (nothing to write), or registered after it, is only
 * run: it has taken no place, and has to cope with that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface OrderedAfterCommitCallback extends Runnable {
  /**
   * Runs while the transaction holds its file locks, right before its pages are published. It must be cheap, must not block and
   * must not wait for another transaction, or the transaction holds its locks for as long. An exception it throws is logged and
   * the callback is then run as an ordinary one.
   */
  void onCommitPoint();

  /**
   * The transaction ended without running this callback, after {@link #onCommitPoint()} ran: its commit failed past that point,
   * or a callback that ran before this one threw an error. Whatever {@link #onCommitPoint()} reserved has to be released, or it
   * holds back the callbacks queued behind it. An exception it throws is logged.
   * <p>
   * It can follow a {@link #onCommitPoint()} that threw, or reach a callback that was registered after the commit point and was
   * never told of it: an implementation has nothing to release then, and must cope with that.
   */
  void onAbandoned();
}
