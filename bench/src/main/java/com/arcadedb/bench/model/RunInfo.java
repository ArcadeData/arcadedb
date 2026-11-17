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
package com.arcadedb.bench.model;

import com.arcadedb.serializer.json.JSONObject;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Who ran what, where and when. The {@code arcadedb} object carries commit, branch, tag, version, variant, image,
 * imageDigest and revision; it is stored as-is.
 */
public record RunInfo(String runId, Instant startedAt, Instant finishedAt, String trigger, String source,
                      JSONObject arcadedb, JSONObject machine, JSONObject config) {
  private static final DateTimeFormatter RUN_ID_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss'Z'")
      .withZone(ZoneOffset.UTC);

  /**
   * @return an id safe as a file name: colons of the ISO timestamp become dashes
   */
  public static String newRunId(final Instant startedAt, final String commit, final String variant) {
    return RUN_ID_TIME.format(startedAt) + "-" + commit + "-" + variant;
  }
}
