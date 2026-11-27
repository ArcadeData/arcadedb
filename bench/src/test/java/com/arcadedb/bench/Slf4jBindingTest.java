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
package com.arcadedb.bench;

import com.arcadedb.bench.server.ServerUnderTest;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #13662: the bench logged through the SLF4J NOP logger, so every warning of the bench and of
 * Testcontainers was lost. testcontainers brought slf4j-api 1.7.36, which won the nearest-first mediation over the
 * engine's 2.0.x, and a 1.7 API looks for {@code StaticLoggerBinder} and never sees the {@code ServiceLoader} providers
 * of a 2.x binding. The bench now pins slf4j-api to the project version and ships the java.util.logging binding, the
 * same one the console and the server distribution use, so the bench logs and the client's own JUL logs share one
 * stream.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Slf4jBindingTest {
  private static final String SERVICE_FILE = "META-INF/services/org.slf4j.spi.SLF4JServiceProvider";
  private static final String JUL_PROVIDER = "org.slf4j.jul.JULServiceProvider";

  @Test
  void benchWarningReachesJavaUtilLogging() {
    final Logger julLogger = Logger.getLogger(ServerUnderTest.class.getName());
    final List<LogRecord> records = new CopyOnWriteArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        records.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    julLogger.addHandler(handler);
    try {
      LoggerFactory.getLogger(ServerUnderTest.class).warn("bench warning for #13662");

      assertThat(records).as("the SLF4J warning never reached java.util.logging: SLF4J is bound to %s",
          LoggerFactory.getILoggerFactory().getClass().getName()).hasSize(1);
      assertThat(records.getFirst().getLevel()).isEqualTo(Level.WARNING);
      assertThat(records.getFirst().getMessage()).isEqualTo("bench warning for #13662");
    } finally {
      julLogger.removeHandler(handler);
    }
  }

  @Test
  void theJulProviderIsTheOnlySlf4jProvider() throws IOException {
    final List<String> providers = new ArrayList<>();
    final Enumeration<URL> files = Slf4jBindingTest.class.getClassLoader().getResources(SERVICE_FILE);
    while (files.hasMoreElements()) {
      final URL file = files.nextElement();
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(file.openStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          final int comment = line.indexOf('#');
          final String provider = (comment >= 0 ? line.substring(0, comment) : line).trim();
          if (!provider.isEmpty())
            providers.add(provider);
        }
      }
    }

    assertThat(providers).as("a second SLF4J provider makes the binding depend on the classpath order").containsExactly(JUL_PROVIDER);
  }
}
