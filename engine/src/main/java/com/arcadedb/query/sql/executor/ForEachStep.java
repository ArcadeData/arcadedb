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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.*;

import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class ForEachStep extends AbstractExecutionStep {
    private final Identifier loopVariable;
    private final Expression      source;
    public final  List<Statement> body;

    Iterator iterator;
    private ExecutionStepInternal finalResult = null;
    private boolean inited = false;

    public ForEachStep(Identifier loopVariable, Expression oExpression, List<Statement> statements, CommandContext ctx,
                       boolean enableProfiling) {
        super(ctx, enableProfiling);
        this.loopVariable = loopVariable;
        this.source = oExpression;
        this.body = statements;
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        prev.get().syncPull(ctx, nRecords);
        if (finalResult != null) {
            return finalResult.syncPull(ctx, nRecords);
        }
        init(ctx);
        while (iterator.hasNext()) {
            ctx.setVariable(loopVariable.getStringValue(), iterator.next());
            ScriptExecutionPlan plan = initPlan(ctx);
            ExecutionStepInternal result = plan.executeFull();
            if (result != null) {
                this.finalResult = result;
                return result.syncPull(ctx, nRecords);
            }
        }
        finalResult = new EmptyStep(ctx, false);
        return finalResult.syncPull(ctx, nRecords);

    }

    protected void init(CommandContext ctx) {
        if (!this.inited) {
            Object val = source.execute(new ResultInternal(), ctx);
            this.iterator = MultiValue.getMultiValueIterator(val);
            this.inited = true;
        }
    }

    public ScriptExecutionPlan initPlan(CommandContext ctx) {
        BasicCommandContext subCtx1 = new BasicCommandContext();
        subCtx1.setParent(ctx);
        ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
        for (Statement stm : body) {
            plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
        }
        return plan;
    }

    public boolean containsReturn() {
        for (Statement stm : this.body) {
            if (stm instanceof ReturnStatement) {
                return true;
            }
            if (stm instanceof ForEachBlock && ((ForEachBlock) stm).containsReturn()) {
                return true;
            }
            if (stm instanceof IfStatement && ((IfStatement) stm).containsReturn()) {
                return true;
            }
        }
        return false;
    }
}
