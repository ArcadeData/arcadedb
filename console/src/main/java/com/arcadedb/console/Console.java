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
package com.arcadedb.console;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.AsyncResultsetCallback;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.AnsiCode;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.TableFormatter;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Console {
  private static final String               PROMPT                   = "%n%s> ";
  private static final String               REMOTE_PREFIX            = "remote:";
  private static final String               LOCAL_PREFIX             = "local:";
  private static final String               SQL_LANGUAGE             = "SQL";
  private final        Terminal             terminal;
  private final        TerminalParser       parser                   = new TerminalParser();
  private              ConsoleOutput        output;
  private              DatabaseFactory      databaseFactory;
  private              BasicDatabase        databaseProxy;
  private              int                  limit                    = 20;
  private              int                  maxMultiValueEntries     = 10;
  private              int                  maxWidth                 = TableFormatter.DEFAULT_MAX_WIDTH;
  private              Boolean              expandResultSet;
  private              String               databaseDirectory;
  private              int                  verboseLevel             = 3;
  private              String               language                 = SQL_LANGUAGE;
  private              boolean              asyncMode                = false;
  private              long                 transactionBatchSize     = 0L;
  protected            long                 currentOperationsInBatch = 0L;
  private              RemoteServer         remoteServer;
  private              boolean              batchMode                = false;
  private              boolean              failAtEnd                = false;
  private static       boolean              errored                  = false;

  public Console(final DatabaseInternal database) throws IOException {
    this();
    this.databaseProxy = database;
  }

  public Console(boolean batchMode, boolean failAtEnd) throws IOException {
    this();
    this.batchMode = batchMode;
    this.failAtEnd = failAtEnd;
  }

  public Console() throws IOException {
    final ContextConfiguration configuration = new ContextConfiguration();
    IntegrationUtils.setRootPath(configuration);
    databaseDirectory = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
    if (!databaseDirectory.endsWith(File.separator))
      databaseDirectory += File.separator;

    GlobalConfiguration.PROFILE.setValue("low-cpu");

    boolean system = System.console() != null;
    terminal = TerminalBuilder.builder().system(system).streams(System.in, System.out).jni(true).build();

    output(3, "%s Console v%s - %s (%s)", Constants.PRODUCT, Constants.getRawVersion(), Constants.COPYRIGHT, Constants.URL);
  }

  public void interactiveMode() throws IOException {
    final Completer completer = new StringsCompleter("align database", "begin", "rollback", "commit", "check database", "close",
        "connect", "create database", "create user", "drop database", "drop user", "export", "import", "help", "info types",
        "list databases", "load", "exit", "quit", "set", "match", "select", "insert into", "update", "delete", "pwd");

    final LineReader lineReader = LineReaderBuilder.builder().terminal(terminal).parser(parser).variable("history-file", ".history")
        .history(new DefaultHistory()).completer(completer).build();

    Runtime.getRuntime().addShutdownHook(new Thread(this::close));

    lineReader.getHistory().load();

    try {
      while (true) {

        String line = null;
        try {
          line = lineReader.readLine(getPrompt());
          if (line == null)
            continue;

          lineReader.getHistory().save();

        } catch (final UserInterruptException | EndOfFileException e) {
          return;
        }

        try {
          if (!parse(line, false))
            return;
        } catch (final Exception e) {
          // IGNORE (ALREADY PRINTED)
        }
      }
    } finally {
      close();
    }
  }

  public static void main(final String[] args) throws IOException {
    final String rootPath = GlobalConfiguration.SERVER_ROOT_PATH.getValueAsString();
    if (rootPath == null)
      GlobalConfiguration.SERVER_ROOT_PATH.setValue(".");

    try {
      execute(args);
    } finally {
      // FORCE EXIT IN CASE OF UNMANAGED ERROR
      if (errored) System.exit(1);
      else System.exit(0);
    }
  }

  public static void execute(final String[] args) throws IOException {
    final StringBuilder commands = new StringBuilder();
    boolean batchMode = false;
    boolean failAtEnd = false;
    // PARSE ARGUMENT, EXTRACT SETTING AND BATCH MODE AND COMPILE THE LINES TO EXECUTE
    for (int i = 0; i < args.length; i++) {
      final String value = args[i].trim();
      if (value.startsWith("-D")) {
        // SETTING
        final String[] parts = value.substring(2).split("=");
        System.setProperty(parts[0], parts[1]);
        setGlobalConfiguration(parts[0], parts[1], true);
      } else if (value.equalsIgnoreCase("-b")) {
        batchMode = true;
      } else if (value.equalsIgnoreCase("-fae")) {
        failAtEnd = true;
      } else {
        commands.append(value);
        if (!value.endsWith(";"))
          commands.append(";");
      }
    }

    final Console console = new Console(batchMode, failAtEnd);

    try {
      if (batchMode) {
        console.parse(commands.toString(), true);
        console.parse("exit", true);
      } else {
        // INTERACTIVE MODE
        if (console.parse(commands.toString(), true))
          console.interactiveMode();
      }
    } finally {
      // FORCE THE CLOSING
      console.close();
    }
  }

  public void close() {
    try {
      if (transactionBatchSize > 0 && currentOperationsInBatch > 0) {
        currentOperationsInBatch = 0;
        databaseProxy.commit();
      }

      if (terminal != null)
        flushOutput();

      if (databaseProxy != null) {
        databaseProxy.close();
        databaseProxy = null;
      }

      if (databaseFactory != null) {
        databaseFactory.close();
        databaseFactory = null;
      }
    } catch (Throwable t) {
      // IGNORE ANY EXCEPTION AT CLOSING
    }
  }

  public void setOutput(final ConsoleOutput output) {
    this.output = output;
  }

  public BasicDatabase getDatabase() {
    return databaseProxy;
  }

  private boolean execute(final String line) throws IOException {
    try {

      if (line == null)
        return true;

      final String lineTrimmed = line.trim();

      if (lineTrimmed.isEmpty() || lineTrimmed.startsWith("--"))
        return true;

      final String lineLowerCase = lineTrimmed.toLowerCase(Locale.ENGLISH);

      if (lineLowerCase.equals("quit") || lineLowerCase.equals("exit")) {
        executeClose();
        return false;
      } else if (lineLowerCase.equals("help") || line.equals("?"))
        executeHelp();
      else if (lineLowerCase.startsWith("begin"))
        executeBegin();
      else if (lineLowerCase.startsWith("close"))
        executeClose();
      else if (lineLowerCase.startsWith("commit"))
        executeCommit();
      else if (lineLowerCase.startsWith("rollback"))
        executeRollback();
      else if (lineLowerCase.startsWith("list databases"))
        executeListDatabases(lineTrimmed.substring("list databases".length()).trim());
      else if (lineLowerCase.startsWith("connect "))
        executeConnect(lineTrimmed.substring("connect".length()).trim());
      else if (lineLowerCase.startsWith("create database "))
        executeCreateDatabase(lineTrimmed.substring("create database".length()).trim());
      else if (lineLowerCase.startsWith("create user "))
        executeCreateUser(lineTrimmed.substring("create user".length()).trim());
      else if (lineLowerCase.startsWith("drop database "))
        executeDropDatabase(lineTrimmed.substring("drop database".length()).trim());
      else if (lineLowerCase.startsWith("drop user "))
        executeDropUser(lineTrimmed.substring("drop user".length()).trim());
      else if (lineLowerCase.startsWith("info"))
        executeInfo(lineTrimmed.substring("info".length()).trim());
      else if (lineLowerCase.startsWith("load"))
        executeLoad(lineTrimmed.substring("load".length()).trim());
      else if (lineLowerCase.startsWith("set "))
        executeSet(lineTrimmed.substring("set".length()).trim());
      else if (lineLowerCase.startsWith("pwd"))
        outputLine(3, "Current directory: " + new File(".").getAbsolutePath());
      else
        executeSQL(lineTrimmed);

      return true;
    } catch (final IOException | RuntimeException e) {
      outputError(e);
      throw e;
    }
  }

  private void executeSet(final String line) {
    final String[] parts = line.split("=");
    if (parts.length != 2)
      throw new ConsoleException("Invalid syntax for SET, use SET <name> = <value>");

    final String key = parts[0].trim();
    final String value = parts[1].trim();

    switch (key.toLowerCase()) {
    case "limit" -> {
      limit = Integer.parseInt(value);
      outputLine(3, "Set new limit to %d", limit);
    }
    case "asyncmode" -> {
      asyncMode = Boolean.parseBoolean(value);
      if (asyncMode) {
        // ENABLE ASYNCHRONOUS PARALLEL MODE
        GlobalConfiguration.ASYNC_WORKER_THREADS.reset();
        // AVOID BATCH IN ASYNC MODE BECAUSE IT IS NOT POSSIBLE TO RETRY THE OPERATION
        GlobalConfiguration.ASYNC_TX_BATCH_SIZE.setValue(1);
        if (!isRemoteDatabase())
          ((Database) databaseProxy).async().onError((e) -> {
            outputError(e);
          });
      }
      outputLine(3, "Set asyncMode to %s", asyncMode);
    }
    case "transactionbatchsize" -> {
      transactionBatchSize = Integer.parseInt(value);
      outputLine(3, "Set new transactionBatch to %d", transactionBatchSize);
    }
    case "language" -> {
      language = value;
      outputLine(3, "Set language to %s", language);
    }
    case "expandresultset" -> {
      expandResultSet = value.equalsIgnoreCase("true");
      outputLine(3, "Set expanded result set to %s", expandResultSet);
    }
    case "maxmultivalueentries" -> {
      maxMultiValueEntries = Integer.parseInt(value);
      outputLine(3, "Set maximum multi value entries to %d", maxMultiValueEntries);
    }
    case "verbose" -> {
      verboseLevel = Integer.parseInt(value);
      outputLine(3, "Set verbose level to %d", verboseLevel);
    }
    case "maxwidth" -> {
      maxWidth = Integer.parseInt(value);
      outputLine(3, "Set maximum width to %d", maxWidth);
    }
    default -> {
      if (!setGlobalConfiguration(key, value, false))
        outputLine(3, "Setting '%s' is not supported by the console", key);
    }
    }

    flushOutput();
  }

  private void executeTransactionStatus() {
    checkDatabaseIsOpen();

    if (databaseProxy instanceof DatabaseInternal db) {
      final TransactionContext tx = db.getTransaction();
      if (tx.isActive()) {
        final ResultInternal row = new ResultInternal(db);
        row.setPropertiesFromMap(tx.getStats());
        printRecord(row);

      } else
        outputLine(3, "Transaction is not Active");
    } else {
      outputLine(3, "No statistics available from remote database");
    }
  }

  private void executeBegin() {
    checkDatabaseIsOpen();
    databaseProxy.begin();
  }

  private void executeCommit() {
    checkDatabaseIsOpen();
    databaseProxy.commit();
  }

  private void executeRollback() {
    checkDatabaseIsOpen();
    databaseProxy.rollback();
  }

  private void executeClose() {
    if (databaseProxy != null) {
      if (databaseProxy.isTransactionActive())
        databaseProxy.commit();
      databaseProxy.close();
      databaseProxy = null;
    }
  }

  private void executeListDatabases(final String url) {

    outputLine(3, "Databases:");
    if (url.startsWith(REMOTE_PREFIX)) {
      connectToRemoteServer(url, false);
      for (final Object f : getRemoteServer().databases()) {
        outputLine(3, "- " + f.toString());
      }

    } else if (isRemoteDatabase()) {
      // REMOTE DATABASE
      for (final Object f : getRemoteServer().databases()) {
        outputLine(3, "- " + f.toString());
      }
    } else {
      // LOCAL DATABASE
      for (final String f : new File(databaseDirectory).list()) {
        outputLine(3, "- " + f);
      }
    }

    flushOutput();
  }

  private void executeConnect(final String url) {
    checkDatabaseIsConnected();
    checkIsEmpty("URL", url);

    final String databaseName;

    if (url.startsWith(REMOTE_PREFIX)) {
      connectToRemoteServer(url, true);
      databaseName = databaseProxy.getName();

    } else {
      final String[] urlParts = url.split(" ");

      final String localUrl = parseLocalUrl(urlParts[0]);

      checkDatabaseIsLocked(localUrl);

      ComponentFile.MODE mode = ComponentFile.MODE.READ_WRITE;
      if (urlParts.length > 1)
        mode = ComponentFile.MODE.valueOf(urlParts[1].toUpperCase(Locale.ENGLISH));

      databaseFactory = new DatabaseFactory(localUrl);
      databaseProxy = databaseFactory.setAutoTransaction(true).open(mode);
      databaseName = databaseProxy.getName();
    }

    outputLine(3, "Database '%s' connected", databaseName);
    flushOutput();
  }

  private void executeCreateDatabase(final String url) {
    checkDatabaseIsConnected();
    checkIsEmpty("URL", url);

    final String databaseName;

    if (url.startsWith(REMOTE_PREFIX)) {
      connectToRemoteServer(url, true);
      getRemoteServer().create(databaseProxy.getName());

    } else {
      final String localUrl = parseLocalUrl(url);

      if (new File(localUrl).exists())
        throw new ConsoleException("Database already exists");

      databaseFactory = new DatabaseFactory(localUrl);
      databaseProxy = databaseFactory.setAutoTransaction(true).create();
    }

    databaseName = databaseProxy.getName();

    outputLine(3, "Database '%s' created", databaseName);
    flushOutput();
  }

  private void executeCreateUser(final String params) {
    checkRemoteDatabaseIsConnected();

    final String paramsUpperCase = params.toUpperCase(Locale.ENGLISH);

    final int identifiedByPos = paramsUpperCase.indexOf("IDENTIFIED BY");
    if (identifiedByPos < 0)
      throw new ConsoleException("IDENTIFIED BY is missing");

    final int databasesByPos = paramsUpperCase.indexOf(" GRANT CONNECT TO ");

    final String userName = params.substring(0, identifiedByPos).trim();

    checkIsEmpty("User name", userName);
    checkHasSpaces("User name", userName);

    final String password;
    Map<String, String> databases = new HashMap<String, String>();

    if (databasesByPos > -1) {
      password = params.substring(identifiedByPos + "IDENTIFIED BY".length() + 1, databasesByPos).trim();
      final String databasesList = params.substring(databasesByPos + " GRANT CONNECT TO ".length()).trim();
      final String[] databasesArray = databasesList.split(",");
      final List<String> databasesName = List.of(databasesArray);
      for (final String db : databasesName) {
        final int colonPos = db.indexOf(":");
        if (colonPos > -1) {
          final String dbname = db.substring(0, colonPos - 1).trim();
          final String dbgroup = db.substring(colonPos + 1).trim();
          databases.put(dbname, dbgroup);
        } else {
          databases.put(db, "admin");
        }
      }
    } else {
      password = params.substring(identifiedByPos + "IDENTIFIED BY".length() + 1).trim();
    }

    checkIsEmpty("User password", password);
    checkHasSpaces("User password", password);

    getRemoteServer().createUser(userName, password, databases);

    outputLine(3, "User '%s' created (on the server)", userName);
    flushOutput();
  }

  private void executeDropDatabase(final String url) {

    checkDatabaseIsConnected();
    checkIsEmpty("URL", url);

    final String databaseName;

    if (url.startsWith(REMOTE_PREFIX)) {
      connectToRemoteServer(url, true);

    } else {
      final String localUrl = parseLocalUrl(url);

      checkDatabaseIsLocked(localUrl);

      databaseFactory = new DatabaseFactory(localUrl);
      databaseProxy = databaseFactory.setAutoTransaction(true).open();
    }

    databaseName = databaseProxy.getName();
    databaseProxy.drop();
    databaseProxy = null;

    outputLine(3, "Database '%s' dropped", databaseName);
    flushOutput();
  }

  private void executeDropUser(final String userName) {
    checkRemoteDatabaseIsConnected();
    checkIsEmpty("User name", userName);
    checkHasSpaces("User name", userName);

    getRemoteServer().dropUser(userName);

    outputLine(3, "User '%s' deleted (on the server)", userName);
    flushOutput();
  }

  private void printRecord(final Result currentRecord) {
    if (currentRecord == null)
      return;

    final Document rec = currentRecord.getElement().orElse(null);

    if (rec instanceof Vertex)
      outputLine(3, "VERTEX @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());
    else if (rec instanceof Edge)
      outputLine(3, "EDGE @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());
    else if (rec != null)
      outputLine(3, "DOCUMENT @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());

    final List<TableFormatter.TableRow> resultSet = new ArrayList<>();

    for (final String fieldName : currentRecord.getPropertyNames()) {
      Object value = currentRecord.getProperty(fieldName);
      if (value instanceof byte[] bytes)
        value = "byte[" + bytes.length + "]";
      else if (value instanceof Iterator<?> iterator) {
        final List<Object> coll = new ArrayList<>();
        while (iterator.hasNext())
          coll.add(iterator.next());
        value = coll;
      } else if (MultiValue.isMultiValue(value)) {
        value = TableFormatter.getPrettyFieldMultiValue(MultiValue.getMultiValueIterator(value), maxMultiValueEntries);
      }

      final ResultInternal row = new ResultInternal();
      resultSet.add(new RecordTableFormatter.TableRecordRow(row));

      row.setProperty("NAME", fieldName);
      row.setProperty("VALUE", value);
    }

    final TableFormatter formatter = new TableFormatter((text, args) -> output(3, text, args));
    formatter.setMaxWidthSize(maxWidth);
    formatter.writeRows(resultSet, -1);
  }

  private void executeSQL(final String line) {
    checkDatabaseIsOpen();

    final long beginTime = System.currentTimeMillis();

    ResultSet resultSet = null;

    if (transactionBatchSize > 0 && !databaseProxy.isTransactionActive())
      databaseProxy.begin();

    if (asyncMode && !isRemoteDatabase()) {
      ((DatabaseInternal) databaseProxy).async().command(language, line, new AsyncResultsetCallback() {
        @Override
        public void onComplete(final ResultSet resultset) {
          // NO ACTIONS
        }

        @Override
        public void onError(Exception exception) {
          outputError(exception);
        }
      });
    } else {
      try {
        resultSet = databaseProxy.command(language, line);
      } catch (Exception e) {
        errored = true;
        if (batchMode && !failAtEnd)
          throw e;
        else
          outputError(e);
        return;
      }
    }

    if (transactionBatchSize > 0) {
      ++currentOperationsInBatch;
      if (currentOperationsInBatch > transactionBatchSize) {
        currentOperationsInBatch = 1;
        databaseProxy.commit();
        databaseProxy.begin();
      }
    }

    if (resultSet == null)
      return;

    final long elapsed;

    Boolean expandOnThisQuery = expandResultSet;

    Result first = null;
    if (resultSet.hasNext()) {
      first = resultSet.next();

      if (expandOnThisQuery == null && !resultSet.hasNext())
        // AUTO MODE, EXPAND THE ONLY RECORD FOUND
        expandOnThisQuery = true;
    }

    if (expandOnThisQuery == null)
      expandOnThisQuery = false;

    if (expandOnThisQuery) {
      // EXPAND THE RECORD
      if (first != null) {
        printRecord(first);

        for (int i = 0; resultSet.hasNext(); ++i) {
          printRecord(resultSet.next());
          if (limit > -1 && i > limit)
            break;
        }
      }

      elapsed = System.currentTimeMillis() - beginTime;

    } else {
      // TABLE FORMAT
      final TableFormatter table = new TableFormatter((text, args) -> output(3, text, args));
      table.setMaxWidthSize(maxWidth);
      table.setPrefixedColumns("#", "@RID", "@TYPE");

      final List<RecordTableFormatter.TableRecordRow> list = new ArrayList<>();

      if (first != null) {
        list.add(new RecordTableFormatter.TableRecordRow(first));

        while (resultSet.hasNext()) {
          list.add(new RecordTableFormatter.TableRecordRow(resultSet.next()));

          if (limit > -1 && list.size() > limit)
            break;
        }
      }

      elapsed = System.currentTimeMillis() - beginTime;

      table.writeRows(list, limit);
    }

    outputLine(3, "Command executed in %dms", elapsed);
  }

  private void executeLoad(final String fileName) throws IOException {
    checkIsEmpty("File name", fileName);

    final File file = new File(fileName);
    if (!file.exists())
      throw new ConsoleException("File name '" + fileName + "' not found");

    output(2, "\nExecuting commands from file %s...", fileName);

    final long startedOn = System.currentTimeMillis();
    final long fileSize = file.length();

    long elapsed = 0L;
    long executedLines = 0L;
    long byteReadFromFile = 0L;
    long lastLapTime = System.currentTimeMillis();
    long lastLapExecutedLines = 0L;

    try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(file, DatabaseFactory.getDefaultCharset()))) {
      while (bufferedReader.ready()) {
        final String line = FileUtils.decodeFromFile(bufferedReader.readLine());

        parse(line, true);

        ++executedLines;
        byteReadFromFile += line.length() + 1;

        final long lapElapsed = System.currentTimeMillis() - lastLapTime;
        if (lapElapsed > 10_000) {
          elapsed = System.currentTimeMillis() - startedOn;
          final int commandsPerSec = (int) ((executedLines - lastLapExecutedLines) * 1000 / lapElapsed);
          final float statusPerc = byteReadFromFile * 100F / fileSize;
          final float etaInMinutes = (elapsed * (fileSize - byteReadFromFile) / (float) byteReadFromFile) / 60_000F;

          output(2, "\n- executed %d commands (%.2f%% of file processed - %d commands/sec - eta %.1f more minutes)", executedLines,
              statusPerc, commandsPerSec, etaInMinutes);
          flushOutput();

          lastLapTime = System.currentTimeMillis();
          lastLapExecutedLines = executedLines;
        }
      }
    }

    elapsed = System.currentTimeMillis() - startedOn;

    output(2, "\nFile processed in " + (elapsed / 1000) + " seconds");
    flushOutput();
  }

  public boolean parse(final String line) throws IOException {
    return parse(line, false);
  }

  public boolean parse(final String line, final boolean printCommand) throws IOException {

    final ParsedLine parsedLine = parser.parse(line, 0);

    if (parsedLine == null)
      return true;

    for (final String w : parsedLine.words()) {
      if (printCommand)
        output(3, getPrompt() + w);

      if (batchMode) {
        try {
          if (!execute(w))
            return false;
        } catch (final Exception e) {
          errored = true;
          if (!failAtEnd)
            throw e;
        }
      } else {
          if (!execute(w))
            return false;

      }
    }
    return true;
  }

  private void outputLine(final int level, final String text, final Object... args) {
    output(level, "\n" + text, args);
  }

  private void output(final int level, final String text, final Object... args) {
    if (verboseLevel < level)
      return;

    if (args.length > 0) {
      if (output != null)
        output.onOutput(text.formatted(args));
      else
        terminal.writer().printf(text, args);
    } else {
      if (output != null)
        output.onOutput(text);
      else
        terminal.writer().print(text);
    }
  }

  private void executeInfo(final String subject) {
    if (subject == null || subject.isEmpty())
      return;

    checkDatabaseIsOpen();

    if (subject.equalsIgnoreCase("types")) {
      outputLine(3, "AVAILABLE TYPES");

      final TableFormatter table = new TableFormatter((text, args) -> output(3, text, args));
      table.setMaxWidthSize(maxWidth);

      if (isRemoteDatabase()) {
        executeSQL("select from schema:types");
        return;
      }

      final List<TableFormatter.TableMapRow> rows = new ArrayList<>();
      for (final DocumentType type : databaseProxy.getSchema().getTypes()) {
        final TableFormatter.TableMapRow row = new TableFormatter.TableMapRow();
        row.setField("NAME", type.getName());

        final byte kind = type.getType();
        if (kind == Document.RECORD_TYPE)
          row.setField("TYPE", "Document");
        else if (kind == Vertex.RECORD_TYPE)
          row.setField("TYPE", "Vertex");
        else if (kind == Edge.RECORD_TYPE)
          row.setField("TYPE", "Edge");

        row.setField("SUPER TYPES", type.getSuperTypes());
        row.setField("BUCKETS", type.getBuckets(false));
        row.setField("PROPERTIES", type.getPropertyNames());
        row.setField("SYNC STRATEGY", type.getBucketSelectionStrategy());

        rows.add(row);
      }

      table.writeRows(rows, -1);
    } else if (subject.equalsIgnoreCase("transaction"))
      executeTransactionStatus();
    else if (subject.startsWith("type ")) {
      final String typeName = subject.substring("type ".length()).trim();

      final TableFormatter table = new TableFormatter((text, args) -> output(0, text, args));
      table.setMaxWidthSize(maxWidth);

      final ResultSet typeResult = databaseProxy.command("sql", "select from schema:types where name = \"" + typeName + "\"");
      if (!typeResult.hasNext())
        return;

      final Result result = typeResult.next();

      outputLine(0, result.getProperty("type").toString().toUpperCase(Locale.ENGLISH) + " TYPE '" + typeName + "'\n");
      outputLine(0, "Super types.......: " + result.getProperty("parentTypes"));
      outputLine(0, "Buckets...........: " + result.getProperty("buckets"));
      outputLine(0, "Bucket selection..: " + result.getProperty("bucketSelectionStrategy"));

      if (result.hasProperty("properties")) {
        outputLine(0, "\nPROPERTIES");

        final List<TableFormatter.TableMapRow> rows = new ArrayList<>();
        for (final Result property : (List<Result>) result.getProperty("properties")) {
          final TableFormatter.TableMapRow row = new TableFormatter.TableMapRow();
          row.setField("NAME", property.getProperty("name"));
          row.setField("TYPE", property.getProperty("type"));
          row.setField("OF", property.hasProperty("of") ? property.getProperty("of") : null);
          row.setField("MANDATORY", property.hasProperty("mandatory") ? property.getProperty("mandatory") : "false");
          row.setField("READONLY", property.hasProperty("readOnly") ? property.getProperty("readOnly") : "false");
          row.setField("NOT NULL", property.hasProperty("notNull") ? property.getProperty("notNull") : "false");
          row.setField("HIDDEN", property.hasProperty("hidden") ? property.getProperty("hidden") : "false");
          row.setField("DEFAULT", property.hasProperty("default") ? property.getProperty("default") : null);
          row.setField("MIN", property.hasProperty("min") ? property.getProperty("min") : "");
          row.setField("MAX", property.hasProperty("max") ? property.getProperty("max") : "");
          row.setField("CUSTOM", property.getProperty("custom"));
          rows.add(row);
        }
        table.writeRows(rows, -1);
      }

      if (result.hasProperty("indexes")) {
        final List<Result> indexes = result.getProperty("indexes");
        outputLine(0, "\nINDEXES (" + indexes.size() + " altogether)");

        final List<TableFormatter.TableMapRow> rows = new ArrayList<>();
        for (final Result index : indexes) {
          final TableFormatter.TableMapRow row = new TableFormatter.TableMapRow();
          row.setField("NAME", index.getProperty("name"));
          row.setField("TYPE", index.getProperty("type"));
          row.setField("UNIQUE", index.getProperty("unique"));
          row.setField("PROPERTIES", index.getProperty("properties").toString());
          rows.add(row);
        }
        table.writeRows(rows, -1);
      }

    } else
      throw new ConsoleException("Information about '" + subject + "' is not available");
  }

  private void executeHelp() {
    outputLine(1, "Help:");
    outputLine(1, "begin                                             -> begins a new transaction");
    outputLine(1, "check database                                    -> check database integrity");
    outputLine(1, "commit                                            -> commits current transaction");
    outputLine(1, "connect <path>|remote:<url> <user> <pw>           -> connects to a database");
    outputLine(1, "close                                             -> disconnects a database");
    outputLine(1, "create database <path>|remote:<url> <user> <pw>   -> creates a new database");
    outputLine(1, "create user <user> identified by <pw> [grant connect to <db>*] -> creates a user");
    outputLine(1, "drop database <path>|remote:<url> <user> <pw>     -> deletes a database");
    outputLine(1, "drop user <user>                                  -> deletes a user");
    outputLine(1, "help|?                                            -> ask for this help");
    outputLine(1, "info types                                        -> prints available types");
    outputLine(1, "info transaction                                  -> prints current transaction");
    outputLine(1, "list databases |remote:<url> <user> <pw>          -> prints list of databases");
    outputLine(1, "load <path>                                       -> runs local script");
    outputLine(1, "pwd                                               -> returns current directory");
    outputLine(1, "rollback                                          -> rolls back current transaction");
    outputLine(1, "set language = sql|sqlscript|cypher|gremlin|mongo -> sets console query language");
    outputLine(1, "-- <comment>                                      -> comment (no operation)");
    outputLine(1, "quit|exit                                         -> exits from the console");
  }

  private void checkDatabaseIsOpen() {
    if (databaseProxy == null)
      throw new ConsoleException("No active database. Open a database first");
  }

  private void checkDatabaseIsConnected() {
    if (databaseProxy != null)
      throw new ConsoleException("Database already connected, close current first");
  }

  private void checkRemoteDatabaseIsConnected() {
    if (!isRemoteDatabase())
      throw new ConsoleException("Remote database connection needed");
  }

  private void checkDatabaseIsLocked(final String url) {
    if (new File(url + "/database.lck").exists())
      throw new ConsoleException("Database appears locked by server");
  }

  private void checkIsEmpty(final String key, final String value) {
    if (value.isEmpty())
      throw new ConsoleException(key + " is empty");
  }

  private void checkHasSpaces(final String key, final String value) {
    if (value.contains(" "))
      throw new ConsoleException(key + " cannot have spaces");
  }

  private String parseLocalUrl(final String url) {
    if (url.startsWith(LOCAL_PREFIX + "//")) {
      return url.replaceFirst(LOCAL_PREFIX + "//", "/");
    } else {
      return databaseDirectory + url.replaceFirst("file://", "");
    }
  }

  private void connectToRemoteServer(final String url, final Boolean needsDatabase) {
    final String conn = url.startsWith(REMOTE_PREFIX + "//") ?
        url.substring((REMOTE_PREFIX + "//").length()) :
        url.substring(REMOTE_PREFIX.length());

    final String[] serverUserPassword = conn.trim().split(" ");
    if (serverUserPassword.length != 3)
      throw new ConsoleException("URL username and password are missing");

    final String[] serverParts = serverUserPassword[0].split("/");
    if ((needsDatabase && serverParts.length != 2) || (!needsDatabase && serverParts.length != 1))
      throw new ConsoleException("Remote URL '" + url + "' not valid");

    final String remoteServer;
    final int remotePort;

    final int portPos = serverParts[0].indexOf(":");
    if (portPos < 0) {
      remoteServer = serverParts[0];
      remotePort = RemoteDatabase.DEFAULT_PORT;
    } else {
      remoteServer = serverParts[0].substring(0, portPos);
      remotePort = Integer.parseInt(serverParts[0].substring(portPos + 1));
    }

    databaseProxy = new RemoteDatabase(remoteServer, remotePort, needsDatabase ? serverParts[1] : "", serverUserPassword[1],
        serverUserPassword[2]);
    this.remoteServer = new RemoteServer(remoteServer, remotePort, serverUserPassword[1], serverUserPassword[2]);
  }

  private void flushOutput() {
    terminal.writer().flush();
  }

  private void outputError(final Throwable e) {
    if (verboseLevel > 1) {
      try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintWriter writer = new PrintWriter(out)) {
        e.printStackTrace(writer);
        writer.flush();
        output(1, AnsiCode.format("\n$ANSI{red ERROR:\n" + out + "}\n"));
      } catch (IOException ex) {
        // IGNORE IT
      }
    } else
      output(1, AnsiCode.format("\n$ANSI{red ERROR: " + e.getMessage() + "}\n"));
  }

  private String getPrompt() {
    final String databaseName = databaseProxy != null ? databaseProxy.getName() : null;
    return PROMPT.formatted(databaseName != null ? "{" + databaseName + "}" : "");
  }

  private static boolean setGlobalConfiguration(final String key, final String value, final boolean printError) {
    final GlobalConfiguration cfg = GlobalConfiguration.findByKey(key);
    if (cfg != null) {
      if (cfg.getScope() == GlobalConfiguration.SCOPE.SERVER) {
        if (printError)
          System.err.println("Global configuration '" + key + "' is not available for console. The setting will be ignored");
      } else {
        cfg.setValue(value);
        return true;
      }
    } else {
      if (printError)
        System.err.println("Global configuration '" + key + "' not found. The setting will be ignored");
    }

    return false;
  }

  private boolean isRemoteDatabase() {
    return databaseProxy instanceof RemoteDatabase;
  }

  private RemoteServer getRemoteServer() {
    return remoteServer;
  }
}
