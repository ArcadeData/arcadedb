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

package com.arcadedb.server.log;

import java.util.logging.Level;

/**
 * Exposes logging to server components.
 */
public interface ServerLogger {

  default void log(final Object requester, final Level level, final String message) {
  }

  default void log(final Object requester, final Level level, final String message, final Object arg1) {
  }

  default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2) {
  }

  default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3) {
  }

  default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3,
      final Object arg4) {
  }

  default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
      final Object arg5) {
  }

  default void log(final Object requester, final Level level, final String message, final Object... args) {
  }
}
