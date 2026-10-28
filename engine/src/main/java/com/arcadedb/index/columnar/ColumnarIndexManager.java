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
package com.arcadedb.index.columnar;

import com.arcadedb.database.LocalDatabase;

/**
 * Owner of the columnar projections of one database. One instance per {@link LocalDatabase}, created when the database opens and
 * before its schema loads, so every {@link ColumnarIndex} the load or a later CREATE INDEX makes is registered here.
 * <p>
 * Registration and reconcile requests are accepted but not acted on yet: the projection lifecycle (open, apply, flush, close) is
 * not wired to this owner.
 */
public final class ColumnarIndexManager {
  private final LocalDatabase database;

  public ColumnarIndexManager(final LocalDatabase database) {
    this.database = database;
  }

  /** The database this manager belongs to. */
  public LocalDatabase getDatabase() {
    return database;
  }

  /** Called when an index is created or loaded. */
  public void register(final ColumnarIndex index) {
    // no projection lifecycle yet
  }

  /** Called when an index is dropped. */
  public void unregister(final ColumnarIndex index) {
    // no projection lifecycle yet
  }

  /** Whether the projection of {@code index} holds a valid state to answer facets from. */
  public boolean isUsable(final ColumnarIndex index) {
    // no projection lifecycle yet: nothing can be invalid
    return true;
  }

  /** Asks for the projections to be reconciled with the COLUMNAR indexes of the schema (columns added or dropped). */
  public void requestReconcile() {
    // no projection lifecycle yet
  }
}
