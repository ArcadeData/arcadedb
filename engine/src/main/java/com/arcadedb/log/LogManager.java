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
 */
package com.arcadedb.log;

import java.util.logging.*;

/**
 * Centralized Log Manager.
 *
 * @author Luca Garulli
 */
public class LogManager {
  private static final LogManager instance = new LogManager();
  private              boolean    debug    = false;
  private              Logger     logger   = new DefaultLogger();

  static class LogContext extends ThreadLocal<String> {
  }

  public static final LogContext CONTEXT_INSTANCE = new LogContext();

  protected LogManager() {
  }

  public static LogManager instance() {
    return instance;
  }

  public String getContext() {
    return CONTEXT_INSTANCE.get();
  }

  public void setContext(final String context) {
    CONTEXT_INSTANCE.set(context);
  }

  public void setLogger(final Logger logger) {
    this.logger = logger;
  }

  public void log(final Object requester, final Level level, String message) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get());
  }

  public void log(final Object requester, final Level level, String message,  final Object... args) {
    logger.log(requester, level, message, null, CONTEXT_INSTANCE.get(), args);
  }

  public void log(final Object requester, final Level level, String message, final Throwable throwable, final Object... args) {
    logger.log(requester, level, message, throwable, CONTEXT_INSTANCE.get(), args);
  }


  public boolean isDebugEnabled() {
    return debug;
  }

  public void flush() {
    logger.flush();
  }
}
