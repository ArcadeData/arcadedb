package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.Map;
import java.util.Optional;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

    private final ProjectionItem item;
    private boolean executed = false;

    public GuaranteeEmptyCountStep(
            ProjectionItem oProjectionItem, CommandContext ctx, boolean enableProfiling) {
        super(ctx, enableProfiling);
        this.item = oProjectionItem;
    }

    @Override
    public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
        if (!prev.isPresent()) {
            throw new IllegalStateException("filter step requires a previous step");
        }
        ResultSet upstream = prev.get().syncPull(ctx, nRecords);
        return new ResultSet() {
            @Override
            public boolean hasNext() {
                if (!executed) {
                    return true;
                }

                return upstream.hasNext();
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    throw new IllegalStateException();
                }

                try {
                    if (upstream.hasNext()) {
                        return upstream.next();
                    }
                    ResultInternal result = new ResultInternal();
                    result.setProperty(item.getProjectionAliasAsString(), 0L);
                    return result;
                } finally {
                    executed = true;
                }
            }

            @Override
            public void close() {
                prev.get().close();
            }

            @Override
            public Optional<ExecutionPlan> getExecutionPlan() {
                return Optional.empty();
            }

            @Override
            public Map<String, Long> getQueryStats() {
                return null;
            }
        };
    }

    @Override
    public ExecutionStep copy(CommandContext ctx) {
        return new GuaranteeEmptyCountStep(item.copy(), ctx, profilingEnabled);
    }

    public boolean canBeCached() {
        return true;
    }

    @Override
    public String prettyPrint(int depth, int indent) {
        StringBuilder result = new StringBuilder();
        result.append(ExecutionStepInternal.getIndent(depth, indent) + "+ GUARANTEE FOR ZERO COUNT ");
        return result.toString();
    }
}