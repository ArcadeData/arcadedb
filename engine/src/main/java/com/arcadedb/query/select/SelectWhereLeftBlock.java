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
 */

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SelectWhereLeftBlock extends SelectWhereBaseBlock {
  public SelectWhereLeftBlock(final Select select) {
    super(select);
  }

  public SelectWhereOperatorBlock property(final String name) {
    setProperty(name);
    return new SelectWhereOperatorBlock(select);
  }

  public SelectWhereOperatorBlock parameter(final String parameterName) {
    setParameter(parameterName);
    return new SelectWhereOperatorBlock(select);
  }
}
