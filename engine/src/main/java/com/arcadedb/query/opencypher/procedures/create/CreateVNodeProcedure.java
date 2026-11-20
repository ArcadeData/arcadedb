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
package com.arcadedb.query.opencypher.procedures.create;

import com.arcadedb.function.create.CreateVNode;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.List;
import java.util.stream.Stream;

/**
 * {@code CALL create.vNode(labels, properties = {}) YIELD node}: the procedure form of the {@link CreateVNode} function, as
 * APOC declares both. It yields the virtual node under {@code node}; the CALL of the function alone yielded its properties.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class CreateVNodeProcedure implements CypherProcedure {
  public static final String NAME = "create.vNode";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    // APOC declares props with a default of {} on the procedure, as it does on the function
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Create a virtual node (not persisted) with the given labels and properties";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("node", CreateVNode.create(NAME, args[0], args.length > 1 ? args[1] : null, context));
    return Stream.of(result);
  }
}
