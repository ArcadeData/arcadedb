package com.arcadedb.query.select;/*
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

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectWhereOperatorBlock {
  private final Select select;

  public SelectWhereOperatorBlock(final Select select) {
    this.select = select;
  }

  public SelectWhereRightBlock eq() {
    return select.setOperator(SelectOperator.eq);
  }

  public SelectWhereRightBlock neq() {
    return select.setOperator(SelectOperator.neq);
  }

  public SelectWhereRightBlock lt() {
    return select.setOperator(SelectOperator.lt);
  }

  public SelectWhereRightBlock le() {
    return select.setOperator(SelectOperator.le);
  }

  public SelectWhereRightBlock gt() {
    return select.setOperator(SelectOperator.gt);
  }

  public SelectWhereRightBlock ge() {
    return select.setOperator(SelectOperator.ge);
  }

  public SelectWhereRightBlock like() {
    return select.setOperator(SelectOperator.like);
  }

  public SelectWhereRightBlock ilike() {
    return select.setOperator(SelectOperator.ilike);
  }

  public SelectWhereRightBlock in() {
    return select.setOperator(SelectOperator.in_op);
  }

  public SelectWhereBetweenBlock between() {
    select.setOperator(SelectOperator.between);
    return new SelectWhereBetweenBlock(select);
  }

  public SelectWhereAfterBlock isNull() {
    select.setOperator(SelectOperator.is_null);
    select.propertyValue = Boolean.TRUE;
    return new SelectWhereAfterBlock(select);
  }

  public SelectWhereAfterBlock isNotNull() {
    select.setOperator(SelectOperator.is_not_null);
    select.propertyValue = Boolean.TRUE;
    return new SelectWhereAfterBlock(select);
  }
}
