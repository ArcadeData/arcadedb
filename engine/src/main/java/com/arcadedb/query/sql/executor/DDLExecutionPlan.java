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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.DDLStatement;

import java.util.Collections;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class DDLExecutionPlan implements InternalExecutionPlan {

    private final DDLStatement statement;
    private final CommandContext ctx;

    boolean executed = false;

    public DDLExecutionPlan(CommandContext ctx, DDLStatement stm) {
        this.ctx = ctx;
        this.statement = stm;
    }

    @Override
    public void close() {

    }

    @Override
    public ResultSet fetchNext(int n) {
        return new InternalResultSet();
    }

    public void reset(CommandContext ctx) {
        executed = false;
    }

    @Override
    public long getCost() {
        return 0;
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    public ResultSet executeInternal(BasicCommandContext ctx) throws CommandExecutionException {
        if (executed) {
            throw new CommandExecutionException("Trying to execute a result-set twice. Please use reset()");
        }
        executed = true;
        ResultSet result = statement.executeDDL(this.ctx);
        if (result instanceof InternalResultSet) {
            ((InternalResultSet) result).plan = this;
        }
        return result;
    }

    @Override
    public List<ExecutionStep> getSteps() {
        return Collections.emptyList();
    }

    @Override
    public String prettyPrint(int depth, int indent) {
        String spaces = ExecutionStepInternal.getIndent(depth, indent);
        String result = spaces + "+ DDL\n" + "  " + statement.toString();
        return result;
    }

    @Override
    public Result toResult() {
        ResultInternal result = new ResultInternal();
        result.setProperty("type", "DDLExecutionPlan");
        result.setProperty(JAVA_TYPE, getClass().getName());
        result.setProperty("stmText", statement.toString());
        result.setProperty("cost", getCost());
        result.setProperty("prettyPrint", prettyPrint(0, 2));
        return result;
    }
}
