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

package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public abstract class SoftThread extends Thread {
  private volatile boolean shutdownFlag;

  private boolean dumpExceptions = true;

  public SoftThread(final ThreadGroup iThreadGroup) {
    super(iThreadGroup, SoftThread.class.getSimpleName());
    setDaemon(true);
  }

  public SoftThread(final String name) {
    super(name);
    setDaemon(true);
  }

  public SoftThread(final ThreadGroup group, final String name) {
    super(group, name);
    setDaemon(true);
  }

  protected abstract void execute() throws Exception;

  public void startup() {
  }

  public void shutdown() {
  }

  public void sendShutdown() {
    shutdownFlag = true;
    interrupt();
  }

  public void softShutdown() {
    shutdownFlag = true;
  }

  public boolean isShutdownFlag() {
    return shutdownFlag;
  }

  @Override
  public void run() {
    startup();

    while (!shutdownFlag && !isInterrupted()) {
      try {
        beforeExecution();
        execute();
        afterExecution();
      } catch (Exception e) {
        if (dumpExceptions)
          LogManager.instance().log(this, Level.SEVERE, "Error during thread execution", e);
      } catch (Error e) {
        if (dumpExceptions)
          LogManager.instance().log(this, Level.SEVERE, "Error during thread execution", e);
        throw e;
      }
    }

    shutdown();
  }

  /**
   * Pauses current thread until iTime timeout or a wake up by another thread.
   *
   * @return true if timeout has reached, otherwise false. False is the case of wake-up by another thread.
   */
  public static boolean pauseCurrentThread(long iTime) {
    try {
      if (iTime <= 0)
        iTime = Long.MAX_VALUE;

      Thread.sleep(iTime);
      return true;
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public boolean isDumpExceptions() {
    return dumpExceptions;
  }

  public void setDumpExceptions(final boolean dumpExceptions) {
    this.dumpExceptions = dumpExceptions;
  }

  protected void beforeExecution() {
    return;
  }

  protected void afterExecution() {
    return;
  }
}
