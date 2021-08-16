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
package com.arcadedb.stresstest;

import com.arcadedb.Constants;
import com.arcadedb.stresstest.workload.OWorkload;
import com.arcadedb.utility.SoftThread;

/**
 * Takes care of updating the console with a completion percentage while the stress test is working; it takes the data to show from
 * the OStressTestResults class.
 *
 * @author
 */
public class ConsoleProgressWriter extends SoftThread {

  final private OWorkload workload;
  private       String    lastResult = null;

  public ConsoleProgressWriter(final OWorkload workload) {
    super(Constants.PRODUCT + " Console writer");
    this.workload = workload;
  }

  public void printMessage(final String message) {
    System.out.println(message);
  }

  @Override
  protected void execute() {
    final String result = workload.getPartialResult();
    if (lastResult == null || !lastResult.equals(result))
      System.out.print("\r- Workload in progress " + result);
    lastResult = result;
    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      softShutdown();
    }
  }

  @Override
  public void sendShutdown() {
    try {
      execute(); // flushes the final result, if we missed it
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.sendShutdown();
  }
}
