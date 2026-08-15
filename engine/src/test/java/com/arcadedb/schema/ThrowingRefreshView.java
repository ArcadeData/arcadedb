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
package com.arcadedb.schema;

import com.arcadedb.database.Database;

import java.util.List;

/**
 * A materialized view whose refresh always fails with an {@link Error}, so the failure walks straight past any
 * {@code catch (Exception)} on the refresh path. The refresher takes ownership of the view's refresh state machine
 * before it reads the backing type name, so throwing from there exercises a failure that happens while ownership is
 * held - the shape that used to leave the view latched as refreshing for the life of the database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ThrowingRefreshView extends MaterializedViewImpl {
  /** An {@link Error} rather than an {@link Exception}, which is the whole point of the fixture. */
  static final class RefreshFailure extends Error {
    RefreshFailure() {
      super("refresh failed with an Error");
    }
  }

  ThrowingRefreshView(final Database database, final String name, final MaterializedViewRefreshMode refreshMode,
      final long refreshInterval) {
    super(database, name, "SELECT value FROM Source", name + "_backing", List.of("Source"), refreshMode, true,
        refreshInterval);
  }

  @Override
  public String getBackingTypeName() {
    throw new RefreshFailure();
  }
}
