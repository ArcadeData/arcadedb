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
package com.arcadedb.server.monitor;

import com.arcadedb.serializer.json.JSONObject;

public class ProfiledQueryEntry {
  final String     database;
  final String     language;
  final String     queryText;
  final long       timestampMs;
  final long       executionTimeNanos;
  final JSONObject executionPlan;

  public ProfiledQueryEntry(final String database, final String language, final String queryText, final long timestampMs,
      final long executionTimeNanos, final JSONObject executionPlan) {
    this.database = database;
    this.language = language;
    this.queryText = queryText;
    this.timestampMs = timestampMs;
    this.executionTimeNanos = executionTimeNanos;
    this.executionPlan = executionPlan;
  }
}
