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
package com.arcadedb.server;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.SoftThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Console reader implementation that uses the Java System.in.
 */
public class DefaultConsoleReader {
  private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, DatabaseFactory.getDefaultCharset()));

  private static class EraserThread extends SoftThread {
    public EraserThread() {
      super("ServerConsoleReader");
    }

    @Override
    @SuppressWarnings({ "checkstyle:AvoidEscapedUnicodeCharacters", "checkstyle:IllegalTokenText" })
    protected void execute() throws Exception {
      System.out.print("\u0008*");
      try {
        Thread.sleep(1);
      } catch (InterruptedException ignore) {
        // om nom nom
      }
    }
  }

  public String readLine() {
    try {
      return reader.readLine();
    } catch (IOException ignore) {
      return null;
    }
  }

  public String readPassword() {
    if (System.console() == null)
      // IDE
      return readLine();

    System.out.print(" ");

    final EraserThread et = new EraserThread();
    et.start();

    try {
      return reader.readLine();
    } catch (IOException ignore) {
      return null;
    } finally {
      et.sendShutdown();
    }
  }
}
