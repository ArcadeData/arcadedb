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
 * A point-in-time page snapshot ({@code PageManager.openSnapshot}, issue #6075) can no longer serve the state it was
 * opened on, so every read through it fails instead of returning a torn image.
 * <p>
 * The two reasons are the copy-on-write shadow breaching {@code arcadedb.pageSnapshotMaxSize} on a write-heavy
 * database, and an I/O error while capturing a page pre-image. Both are recoverable from the consumer's side: a
 * backup, an HA verify or an HA snapshot ship catching this falls back to the suspend-and-freeze path, which
 * throttles writers but always completes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PageSnapshotException extends ArcadeDBException {
  public PageSnapshotException(final String s) {
    super(s);
  }

  public PageSnapshotException(final String s, final Throwable e) {
    super(s, e);
  }
}
