/*
 * Copyright 2023 Arcade Data Ltd
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

package com.arcadedb.server;

import java.util.logging.*;

/**
 * Delay the reset after the server completed the shutdown.
 * Credits: Peter Lawrey (https://stackoverflow.com/questions/13825403/java-how-to-get-logger-to-work-in-shutdown-hook)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerLogManager extends LogManager {
  static ServerLogManager instance;
  static boolean          enableReset = true;

  public ServerLogManager() {
    instance = this;
  }

  public static void enableReset(final boolean value) {
    enableReset = value;
  }

  @Override
  public void reset() {
    if (enableReset)
      super.reset();
  }

  private void reset0() {
    super.reset();
  }

  public static void resetFinally() {
    if (instance != null)
      instance.reset0();
  }
}
