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

import com.arcadedb.remote.RemoteDatabase;

/**
 * StressTester settings.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class StressTesterSettings {
  public String                             dbName;
  public StressTester.OMode                 mode;
  public String                             rootPassword;
  public String                             resultOutputFile;
  public String                             embeddedPath;
  public int                                operationsPerTransaction;
  public int                                delay;
  public int                                concurrencyLevel;
  public String                             remoteIp;
  public boolean                            haMetrics;
  public String                             workloadCfg;
  public boolean                            keepDatabaseAfterTest;
  public int                                remotePort    = 2424;
  public boolean                            checkDatabase = false;
  public RemoteDatabase.CONNECTION_STRATEGY loadBalancing = RemoteDatabase.CONNECTION_STRATEGY.ROUND_ROBIN;
}
