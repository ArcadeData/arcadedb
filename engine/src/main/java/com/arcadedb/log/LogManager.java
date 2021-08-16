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

package com.arcadedb.log;

import java.util.logging.Level;

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

  public static LogContext CONTEXT_INSTANCE = new LogContext();

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

  public Logger getLogger() {
    return logger;
  }

  public void setLogger(final Logger logger) {
    this.logger = logger;
  }

  public void warn(final Object iRequester, String iMessage) {
    logger.log(iRequester, Level.WARNING, iMessage, null, CONTEXT_INSTANCE.get(), null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void error(final Object iRequester, String iMessage) {
    logger.log(iRequester, Level.SEVERE, iMessage, null, CONTEXT_INSTANCE.get(), null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null);
  }

  public void error(final Object iRequester, String iMessage, Exception e) {
    logger
        .log(iRequester, Level.SEVERE, iMessage, e, CONTEXT_INSTANCE.get(), null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null);
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

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, null, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, null, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, null, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, null, null,
        null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, null,
            null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            null, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13, final Object arg14) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, null, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, null, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, null);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object arg1, final Object arg2,
      final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10,
      final Object arg11, final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
    logger
        .log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17);
  }

  public void log(final Object iRequester, final Level iLevel, String iMessage, final Throwable iException, final Object... args) {
    logger.log(iRequester, iLevel, iMessage, iException, CONTEXT_INSTANCE.get(), args);
  }

  public boolean isLevelEnabled(final Level level) {
    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      return debug;
    else if (level.equals(Level.INFO))
      return info;
    else if (level.equals(Level.WARNING))
      return warn;
    else if (level.equals(Level.SEVERE))
      return error;
    return false;
  }

  public boolean isWarn() {
    return warn;
  }

  public boolean isDebugEnabled() {
    return debug;
  }

  public void setDebugEnabled(boolean debug) {
    this.debug = debug;
  }

  public boolean isInfoEnabled() {
    return info;
  }

  public void setInfoEnabled(boolean info) {
    this.info = info;
  }

  public boolean isWarnEnabled() {
    return warn;
  }

  public void setWarnEnabled(boolean warn) {
    this.warn = warn;
  }

  public boolean isErrorEnabled() {
    return error;
  }

  public void setErrorEnabled(boolean error) {
    this.error = error;
  }

  protected void setLevelInternal(final Level level) {
    if (level == null)
      return;

    if (level.equals(Level.FINER) || level.equals(Level.FINE) || level.equals(Level.FINEST))
      debug = info = warn = error = true;
    else if (level.equals(Level.INFO)) {
      info = warn = error = true;
      debug = false;
    } else if (level.equals(Level.WARNING)) {
      warn = error = true;
      debug = info = false;
    } else if (level.equals(Level.SEVERE)) {
      error = true;
      debug = info = warn = false;
    }
  }

  public void flush() {
    logger.flush();
  }
}
