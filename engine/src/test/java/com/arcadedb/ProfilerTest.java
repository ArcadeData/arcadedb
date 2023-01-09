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
package com.arcadedb;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;

public class ProfilerTest {

  @Test
  public void testDumpProfileMetrics() {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    Profiler.INSTANCE.dumpMetrics(new PrintStream(out));
    Assertions.assertTrue(out.size() > 0);
  }

  @Test
  public void testMetricsToJSON() {
    JSONObject json = Profiler.INSTANCE.toJSON();
    Assertions.assertTrue(json.has("diskFreeSpace"));
    Assertions.assertTrue(json.has("diskTotalSpace"));
    Assertions.assertTrue(json.has("updateRecord"));
    Assertions.assertTrue(json.has("totalDatabases"));
  }
}
