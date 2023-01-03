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
package com.arcadedb.query.sql.executor;

import java.util.*;

/**
 * Created by luigidellaquila on 19/12/16.
 */
public class InfoExecutionStep implements ExecutionStep {

  String name;
  String type;
  String javaType;
  String targetNode;
  String description;
  long   cost;
  final List<ExecutionStep> subSteps = new ArrayList<>();

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public List<ExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public Result toResult() {
    return null;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public void setTargetNode(final String targetNode) {
    this.targetNode = targetNode;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  @Override
  public long getCost() {
    return cost;
  }

  public void setCost(final long cost) {
    this.cost = cost;
  }

  public String getJavaType() {
    return javaType;
  }

  public void setJavaType(final String javaType) {
    this.javaType = javaType;
  }
}
