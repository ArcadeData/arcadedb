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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One benchmark run: the {@link RunInfo} plus every {@link CaseResult}, serialized as the versioned results JSON.
 */
public record RunResult(RunInfo info, List<CaseResult> results) {
  public static final int SCHEMA_VERSION = 1;

  public JSONObject toJSON() {
    final JSONArray array = new JSONArray();
    for (final CaseResult result : results)
      array.put(result.toJSON());
    return new JSONObject().put("schemaVersion", SCHEMA_VERSION).put("runId", info.runId())
        .put("startedAt", info.startedAt().toString()).put("finishedAt", info.finishedAt().toString())
        .put("trigger", info.trigger()).put("source", info.source()).put("arcadedb", info.arcadedb())
        .put("machine", info.machine()).put("config", info.config()).put("results", array);
  }

  public static RunResult fromJSON(final JSONObject json) {
    final int version = json.getInt("schemaVersion", -1);
    if (version != SCHEMA_VERSION)
      throw new IllegalArgumentException("Unsupported results schemaVersion " + version + ", expected " + SCHEMA_VERSION);
    final RunInfo info = new RunInfo(json.getString("runId"), Instant.parse(json.getString("startedAt")),
        Instant.parse(json.getString("finishedAt")), json.getString("trigger"), json.getString("source"),
        json.getJSONObject("arcadedb", new JSONObject()), json.getJSONObject("machine", new JSONObject()),
        json.getJSONObject("config", new JSONObject()));
    final JSONArray array = json.getJSONArray("results", new JSONArray());
    final List<CaseResult> results = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      results.add(CaseResult.fromJSON(array.getJSONObject(i)));
    return new RunResult(info, results);
  }

  /**
   * Writes {@code <runId>.json} into the directory, creating it when missing.
   */
  public void write(final Path dir) {
    try {
      Files.createDirectories(dir);
      Files.writeString(dir.resolve(info.runId() + ".json"), toJSON().toString(2), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static RunResult read(final Path file) {
    try {
      return fromJSON(new JSONObject(Files.readString(file, StandardCharsets.UTF_8)));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
