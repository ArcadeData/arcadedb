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
 * A record that outgrew its page cannot be assembled because its chunk chain is structurally broken: a continuation
 * pointer to a page the file does not have, a slot that no longer holds a chunk, a marker that is not a continuation,
 * or a chain that loops. The record is corrupted and no amount of retrying will change that (#6258).
 * <p>
 * That last sentence is the whole reason this exception exists. A broken chain used to be reported as a
 * {@link ConcurrentModificationException} - a {@link NeedRetryException}, i.e. "somebody else got there first, come
 * back" - which is a different problem with a different answer: it cost every reader of the corrupted record a full
 * retry budget of chain walks, and then sent whoever read the log looking for contention that was not there. Every
 * caller that had to tell the two apart did it by walking the chain a second time itself. They can now ask the
 * exception.
 * <p>
 * NOT a {@link NeedRetryException}, deliberately: the retry machinery must not re-run a transaction for this. The
 * repair is {@code CHECK DATABASE ... FIX}, which deletes the record the chain can no longer reach.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BrokenChunkChainException extends DatabaseOperationException {
  public BrokenChunkChainException(final String s) {
    super(s);
  }

  public BrokenChunkChainException(final String s, final Throwable e) {
    super(s, e);
  }
}
