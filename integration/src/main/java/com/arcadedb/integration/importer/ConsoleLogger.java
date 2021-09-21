/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.integration.importer;

/**
 * Console logger used by importers and exporters.
 *
 * @author Luca Garulli
 */
public class ConsoleLogger {
  private final int verboseLevel;

  public ConsoleLogger(final int verboseLevel) {
    this.verboseLevel = verboseLevel;
  }

  public void logLine(final int level, final String text, final Object... args) {
    if (level > verboseLevel)
      return;

    if (args.length == 0)
      System.out.println(text);
    else
      System.out.println(String.format(text, args));
  }

  public void log(final int level, final String text, final Object... args) {
    if (level > verboseLevel)
      return;

    if (args.length == 0)
      System.out.print(text);
    else
      System.out.print(String.format(text, args));
  }

  public void errorLine(final String text, final Object... args) {
    if (args.length == 0)
      System.out.println(text);
    else
      System.out.println(String.format(text, args));
  }
}
