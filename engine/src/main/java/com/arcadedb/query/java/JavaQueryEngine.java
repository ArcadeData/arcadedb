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
package com.arcadedb.query.java;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.graalvm.polyglot.Engine;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

public class JavaQueryEngine implements QueryEngine {
  public static final String                       ENGINE_NAME = "java";
  private final       long                         timeout;
  private final       ThreadPoolExecutor           userCodeExecutor;
  private             ArrayBlockingQueue<Runnable> userCodeExecutorQueue;

  private static final AnalyzedQuery ANALYZED_QUERY = new AnalyzedQuery() {
    @Override
    public boolean isIdempotent() {
      return false;
    }

    @Override
    public boolean isDDL() {
      return false;
    }
  };

  public static class JavaQueryEngineFactory implements QueryEngineFactory {
    private List<String> allowedPackages = null;

    public JavaQueryEngineFactory setAllowedPackages(final List<String> allowedPackages) {
      this.allowedPackages = allowedPackages;
      return this;
    }

    @Override
    public String getLanguage() {
      return ENGINE_NAME;
    }

    @Override
    public QueryEngine getInstance(final DatabaseInternal database) {
      return new JavaQueryEngine(database, allowedPackages);
    }

    public static Iterable<String> getSupportedLanguages() {
      return GraalPolyglotEngine.newBuilder(null, Engine.create()).build().getSupportedLanguages();
    }
  }

  protected JavaQueryEngine(final DatabaseInternal database, final List<String> allowedPackages) {
    this.userCodeExecutorQueue = new ArrayBlockingQueue<>(10000);
    this.userCodeExecutor = new ThreadPoolExecutor(8, 8, 30, TimeUnit.SECONDS, userCodeExecutorQueue, new ThreadPoolExecutor.CallerRunsPolicy());
    this.timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.POLYGLOT_COMMAND_TIMEOUT);
  }

  @Override
  public ResultSet command(final String query, final Object... parameters) {
    try {
      return executeUserCode(() -> {

        final String[] parts = query.split("::");
        if (parts.length != 2)
          throw new QueryParsingException(
              "Java function name '" + query + "' must contain the full package of the class, :: and the method. Example: org.acme.Math::sum");

        final Class<?> impl = Class.forName(parts[0]);

        final Object[] parameterArray = new Object[parameters.length];

        // LOOK FOR THE RIGHT METHOD TO INVOKE
        final Method rightMethod = searchMethod(parts, impl, parameterArray, parameters);

        if (rightMethod == null)
          throw new QueryParsingException(
              "Java function '" + query + "' not found on classpath (class '" + parts[0] + "' method '" + parts[1] + "' with parameters " + Arrays.toString(
                  parameters) + ")");

        final Object instance = Modifier.isStatic(rightMethod.getModifiers()) ? null : impl.getConstructor().newInstance();

        final Object result = rightMethod.invoke(instance, parameterArray);

        final InternalResultSet resultSet;
        if (result instanceof ResultSet)
          resultSet = (InternalResultSet) result;
        else if (result instanceof Iterable) {
          resultSet = new InternalResultSet();
          for (Object o : (Iterable) result)
            resultSet.add(extractResult(o));
        } else {
          resultSet = new InternalResultSet();
          resultSet.add(extractResult(result));
        }

        return resultSet;

      }, timeout);

    } catch (CommandExecutionException e) {
      throw e;
    } catch (ExecutionException e) {
      // USE THE UNDERLYING CAUSE BYPASSING THE NOT RELEVANT EXECUTION EXCEPTION
      throw new CommandExecutionException("Error on executing user code", e.getCause());
    } catch (Exception e) {
      throw new CommandExecutionException("Error on executing user code", e);
    }

  }

  private Method searchMethod(String[] parts, Class<?> impl, Object[] parameterArray, Object[] parameters) {
    Method rightMethod = null;
    for (Method method : impl.getMethods()) {
      if (method.getName().equals(parts[1])) {
        if (method.getParameterCount() == parameters.length) {

          // RESET PARAMETER ARRAY
          for (int i = 0; i < parameterArray.length; i++)
            parameterArray[i] = null;

          boolean allParamsMatch = true;
          final Parameter[] methodParameters = method.getParameters();
          for (int i = 0; i < methodParameters.length; i++) {
            final Object parameterValue = parameters[i];
            if (parameterValue == null)
              continue;

            final Parameter methodParameter = methodParameters[i];

            parameterArray[i] = parameterValue;

            final Class<?> methodParameterType = methodParameter.getType();
            final Class<?> parameterValueClass = parameterValue.getClass();

            if (!parameterValueClass.isAssignableFrom(methodParameterType)) {
              if (methodParameterType.isPrimitive()) {
                // CHECK FOR AUTOBOXING
                if (methodParameterType.equals(Integer.TYPE)) {
                  if (!parameterValueClass.equals(Integer.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Long.TYPE)) {
                  if (!parameterValueClass.equals(Long.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Float.TYPE)) {
                  if (!parameterValueClass.equals(Float.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Double.TYPE)) {
                  if (!parameterValueClass.equals(Double.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Byte.TYPE)) {
                  if (!parameterValueClass.equals(Byte.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Character.TYPE)) {
                  if (!parameterValueClass.equals(Character.class))
                    allParamsMatch = false;
                } else if (methodParameterType.equals(Short.TYPE)) {
                  if (!parameterValueClass.equals(Short.class))
                    allParamsMatch = false;
                }
              }
            } else
              allParamsMatch = false;

            if (!allParamsMatch)
              break;
          }

          if (allParamsMatch)
            rightMethod = method;

          break;
        }
      }
    }

    return rightMethod;
  }

  @Override
  public ResultSet command(final String query, final Map<String, Object> parameters) {
    if (parameters == null || parameters.size() == 0)
      return command(query);
    throw new UnsupportedOperationException("Execution of a command with parameters referenced by name is not supported for Java engine");
  }

  @Override
  public String getLanguage() {
    return ENGINE_NAME;
  }

  @Override
  public AnalyzedQuery analyze(final String query) {
    try {
      executeUserCode(() -> {
        return null;
      }, timeout);
    } catch (CommandExecutionException e) {
      throw e;
    } catch (ExecutionException e) {
      // USE THE UNDERLYING CAUSE BYPASSING THE NOT RELEVANT EXECUTION EXCEPTION
      throw new CommandExecutionException("Error on executing user code", e.getCause());
    } catch (Exception e) {
      throw new CommandExecutionException("Error on analyzing user code", e);
    }

    return ANALYZED_QUERY;
  }

  @Override
  public ResultSet query(final String query, final Map<String, Object> parameters) {
    throw new UnsupportedOperationException("Execution of a query (idempotent) is not supported for polyglot engine. Use command instead");
  }

  @Override
  public ResultSet query(final String query, final Object... parameters) {
    throw new UnsupportedOperationException("Execution of a query (idempotent) is not supported for polyglot engine. Use command instead");
  }

  private ResultSet executeUserCode(final Callable task, final long executionTimeoutMs) throws Exception {
// IF NOT INITIALIZED, EXECUTE AS SOON AS THE SERVICE STARTS
    final Future future = userCodeExecutor.submit(task);
    if (future == null)
      return null;

    try {
      Object result = executionTimeoutMs > 0 ? future.get(executionTimeoutMs, TimeUnit.MILLISECONDS) : future.get();
      if (result instanceof Exception)
        throw (Exception) result;

      return (ResultSet) result;

    } catch (TimeoutException e) {
      future.cancel(true); //this method will stop the running underlying task
      throw e;
    }
  }

  private ResultInternal extractResult(final Object o) {
    if (o instanceof Document)
      return new ResultInternal((Document) o);
    else if (o instanceof Identifiable)
      return new ResultInternal((Identifiable) o);
    else if (o instanceof Map)
      return new ResultInternal((Map) o);

    return new ResultInternal().setProperty("value", o);
  }
}
