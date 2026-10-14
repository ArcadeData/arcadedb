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
package com.arcadedb.log;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.TransactionManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #9984: {@code LogManager.isDebugEnabled()} returned a field nothing outside a test ever set, so the 41 production
 * guards written against it were unreachable whatever the logging configuration said. They now ask
 * {@link LogManager#isLoggable}, and the flag is gone so that no new guard can be written against it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9984DebugGuardsFollowLoggingConfigTest extends TestHelper {

  @Test
  void theUnwiredFlagIsGone() {
    final List<String> methods = Arrays.stream(LogManager.class.getMethods()).map(Method::getName).toList();
    assertThat(methods).doesNotContain("isDebugEnabled", "setDebugEnabled").contains("isLoggable");
  }

  /** One of the converted guards, on the commit path: it opens when its logger is set to FINE, and only then. */
  @Test
  void aConvertedGuardOpensWithItsLoggerLevel() {
    database.getSchema().createDocumentType("Issue9984");

    final Logger jul = Logger.getLogger(TransactionManager.class.getName());
    final Level previous = jul.getLevel();
    final List<String> published = new CopyOnWriteArrayList<>();
    final Handler capture = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel() == Level.FINE)
          published.add(record.getMessage());
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    capture.setLevel(Level.ALL);
    jul.addHandler(capture);
    try {
      jul.setLevel(Level.INFO);
      database.transaction(() -> database.newDocument("Issue9984").set("i", 0).save());
      assertThat(published).noneMatch(m -> m.startsWith("Locked files"));

      jul.setLevel(Level.FINE);
      database.transaction(() -> database.newDocument("Issue9984").set("i", 1).save());
      assertThat(published).anyMatch(m -> m.startsWith("Locked files"));
    } finally {
      jul.removeHandler(capture);
      jul.setLevel(previous);
    }
  }
}
