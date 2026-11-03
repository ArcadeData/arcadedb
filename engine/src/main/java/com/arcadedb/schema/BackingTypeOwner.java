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

/**
 * A schema object whose rows live in a type of their own, the BACKING type, and are derived from other types: a materialized view
 * or a continuous aggregate (issue #11020). The backing type is written by the owner's refresh and by nothing else, which
 * {@link LocalSchema#checkBackingTypeWrite} enforces on every record write and on {@code TRUNCATE}: a row written, changed or
 * deleted by anyone else is a row the owner's state does not account for. A materialized view would answer it until its next
 * refresh while reporting VALID, and a continuous aggregate would never recompute a bucket deleted behind its watermark.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
interface BackingTypeOwner {
  /** The database the owner belongs to: a refresh scope authorizes writes in that database only. */
  Database getDatabase();

  String getName();

  String getBackingTypeName();

  /** What the owner is, for a message: {@code "materialized view"} or {@code "continuous aggregate"}. */
  String kind();

  /** The statement that recomputes the rows, for a message. */
  String refreshStatement();

  /** The statement that removes the owner and its backing type, for a message. */
  String dropStatement();
}
