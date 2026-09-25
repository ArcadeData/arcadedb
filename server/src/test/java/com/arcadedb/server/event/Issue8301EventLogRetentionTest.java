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
package com.arcadedb.server.event;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8301: server event logs were rotated by name order, and before the fix a log started under th-TH (or ar-EG)
 * was named with the Buddhist year (or Arabic-Indic digits), which sorts after every newer log: the newest logs were
 * deleted while the legacy ones were kept forever. Retention now goes by the start counter in the name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8301EventLogRetentionTest {

  @Test
  void legacyLocaleNamedLogsAreTheOldest(@TempDir final Path dir) throws IOException {
    final List<String> files = new ArrayList<>();
    // Counters 0..4 written under th-TH, 5 under ar-EG, 6..10 after the fix
    for (int i = 0; i < 5; i++)
      files.add(create(dir, "server-event-log-2569010%d-000000.%d.jsonl".formatted(i, i)));
    files.add(create(dir, "server-event-log-٢٠٢٦٠١٠٦-٠٠٠٠٠٠.5.jsonl"));
    for (int i = 6; i <= 10; i++)
      files.add(create(dir, "server-event-log-202609%02d-000000.%d.jsonl".formatted(i, i)));

    FileServerEventLog.retainNewest(dir.toFile(), files);

    assertThat(files).hasSize(10);
    assertThat(FileServerEventLog.fileCounter(files.getFirst())).isEqualTo(10);
    assertThat(FileServerEventLog.fileCounter(files.getLast())).isEqualTo(1);
    assertThat(dir.resolve("server-event-log-25690100-000000.0.jsonl")).doesNotExist();
    assertThat(dir.resolve("server-event-log-20260910-000000.10.jsonl")).exists();
  }

  private static String create(final Path dir, final String name) throws IOException {
    final File file = Files.writeString(dir.resolve(name), "").toFile();
    return file.getName();
  }
}
