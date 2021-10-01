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
  private              boolean    info     = true;
  private              boolean    warn     = true;
  private              boolean    error    = true;
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

  public void log(final Object iRequester, final Level iLevel, String iMessage) {
    logger.log(iRequester, iLevel, iMessage, null, CONTEXT_INSTANCE.get(), null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object... args) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), args);
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public void flush() {
    logger.flush();
  }
}
