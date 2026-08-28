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
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.serializer.json.JSONObject;
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
import com.arcadedb.utility.RecordTableFormatter;
import com.arcadedb.utility.StringUtils;
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
import java.time.Instant;
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
  private static final String               HISTORY_FILE             = ".history";
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
  // BUILT LAZILY AND SHARED BY interactiveMode() AND BY THE MASKED PASSWORD PROMPT, SO A PASSWORD CAN BE ASKED FOR
  // WITHOUT EVER APPEARING ON A LINE THAT GOES TO THE HISTORY FILE (ISSUE #6829)
  private              LineReader           lineReader;
  // WHETHER THE PROCESS OWNS A REAL TERMINAL: A MASKED PROMPT IS ONLY POSSIBLE WHEN SOMEBODY IS THERE TO TYPE INTO IT
  private final        boolean              systemTerminal;

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

    systemTerminal = System.console() != null;
    terminal = TerminalBuilder.builder().system(systemTerminal).streams(System.in, System.out).jni(true).build();

    output(3, "%s Console v%s - %s (%s)", Constants.PRODUCT, Constants.getRawVersion(), Constants.COPYRIGHT, Constants.URL);
    output(3, "%s", Constants.SPONSOR_MSG);
  }

  /**
   * Returns the line reader, building it on first use.
   * <p>
   * The history it is given masks the passwords that the `connect` and `create user` syntaxes carry inline before they are
   * recorded, so they never reach the `.history` file - which is written after every command, in the working directory,
   * with the process umask, and survives the session indefinitely (issue #6829).
   * <p>
   * Event expansion is turned OFF, which is what makes a typed backslash reach the query engine. jline runs its own
   * shell-style unescaping in {@code LineReaderImpl.finish()} before the accepted line is ever handed to
   * {@link TerminalParser}, so teaching the parser to keep the escape character (issue #6827) fixed `-b` and `load` but
   * left the interactive prompt eating one level exactly as before. The same option also disables `!`-style history
   * expansion, which a SQL console is better off without: `!` is part of the `!=` operator, and an expansion that finds
   * a match rewrites the statement rather than failing visibly.
   */
  LineReader getLineReader() {
    if (lineReader == null) {
      final Completer completer = new StringsCompleter("align database", "begin", "rollback", "commit", "check database", "close",
          "connect", "create database", "create user", "drop database", "drop user", "export", "import", "help", "info types",
          "list databases", "load", "exit", "quit", "set", "match", "select", "insert into", "update", "delete", "pwd");

      lineReader = LineReaderBuilder.builder().terminal(terminal).parser(parser)
          .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
          .variable(LineReader.HISTORY_FILE, HISTORY_FILE).history(new DefaultHistory() {
            @Override
            public void add(final Instant time, final String line) {
              super.add(time, ConsoleCredentials.mask(line));
            }
          }).completer(completer).build();
    }
    return lineReader;
  }

  public void interactiveMode() throws IOException {
    final LineReader lineReader = getLineReader();

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
        final String keyValue = value.substring(2);
        final String[] pair = splitKeyValue(keyValue);
        // ON THE COMMAND LINE A SETTING WITH NO '=' AT ALL IS THE SAME AS ONE WITH AN EMPTY VALUE
        final String key = pair == null ? keyValue : pair[0];
        final String propertyValue = pair == null ? "" : pair[1];
        if (key.isEmpty())
          System.err.println("Ignoring malformed system property argument '" + value + "': missing key");
        else {
          System.setProperty(key, propertyValue);
          setGlobalConfiguration(key, propertyValue, true);
        }
      } else if ("-b".equalsIgnoreCase(value)) {
        batchMode = true;
      } else if ("-fae".equalsIgnoreCase(value)) {
        failAtEnd = true;
      } else {
        // TERMINATE THE ARGUMENT WITH A NEW LINE BEFORE THE SEPARATOR, SO A TRAILING LINE COMMENT DOES NOT SWALLOW THE NEXT ONE
        commands.append(value).append("\n;");
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

  /**
   * Releases everything the console holds: the pending batch, the terminal buffer, the database and the factory.
   * <p>
   * Each step gets its own try/catch rather than sharing one. With a single catch the first step decided the fate of the
   * other three, and the first step is the only one that can realistically fail: in the default batch flow `exit` had
   * already nulled the database proxy, so committing threw a NullPointerException here and the terminal was never flushed,
   * losing whatever jline still had buffered when {@code main()} called {@code System.exit} (issue #6828).
   * <p>
   * A failed exit-time commit is also reported and marks the process as errored. Swallowing it silently is the worse half
   * of the same bug: on the interactive Ctrl-D path up to {@code transactionBatchSize} statements are committed right here,
   * and the process used to exit 0 while that data was never written. The three release steps stay quiet, since at exit
   * time there is nothing left for the user to do about them.
   */
  public void close() {
    if (transactionBatchSize > 0 && currentOperationsInBatch > 0 && databaseProxy != null) {
      try {
        // COMMITTING NEEDS AN ACTIVE TRANSACTION, NOT JUST A NON-ZERO COUNTER: commit() ON AN INACTIVE TRANSACTION IS
        // NOT A NO-OP, WHICH IS WHY executeCommit()/executeClose() GUARD ON isTransactionActive() TOO. THE GUARD IS
        // INSIDE THE TRY BECAUSE IT CAN THROW ON ITS OWN: ON THE EMBEDDED PATH isTransactionActive() RESOLVES THE
        // THREAD'S CACHED TRANSACTION AND RAISES InvalidDatabaseInstanceException WHEN IT BELONGS TO A DIFFERENT
        // LocalDatabase INSTANCE FOR THE SAME PATH - RECONNECTING TO THE SAME DATABASE IN ONE SESSION DOES THAT. A
        // THROW HERE WOULD SKIP THE FLUSH AND THE TWO CLOSES ALL OVER AGAIN, JUST FROM ANOTHER TRIGGER (ISSUE #6828)
        if (databaseProxy.isTransactionActive())
          databaseProxy.commit();
      } catch (Throwable t) {
        errored = true;
        outputError(t);
      }
    }
    currentOperationsInBatch = 0;

    if (terminal != null) {
      try {
        flushOutput();
      } catch (Throwable t) {
        // IGNORE: THERE IS NOWHERE LEFT TO REPORT IT TO
      }
    }

    if (databaseProxy != null) {
      try {
        databaseProxy.close();
      } catch (Throwable t) {
        // IGNORE ANY EXCEPTION AT CLOSING
      } finally {
        databaseProxy = null;
      }
    }

    if (databaseFactory != null) {
      try {
        databaseFactory.close();
      } catch (Throwable t) {
        // IGNORE ANY EXCEPTION AT CLOSING
      } finally {
        databaseFactory = null;
      }
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

      if ("quit".equals(lineLowerCase) || "exit".equals(lineLowerCase)) {
        executeClose();
        return false;
      } else if ("help".equals(lineLowerCase) || "?".equals(line))
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
      else if (lineLowerCase.startsWith("set ") && !lineLowerCase.startsWith("set global"))
        executeSet(lineTrimmed.substring("set".length()).trim());
      else if (lineLowerCase.startsWith("pwd"))
        outputLine(3, "Current directory: " + new File(".").getAbsolutePath());
      else
        executeSQL(lineTrimmed);

      return true;
    } catch (final IOException | RuntimeException e) {
      // A NESTED COMMAND (E.G. AN UNBALANCED '{' REPORTED WHILE EXECUTING A LOADED SCRIPT) MAY HAVE ALREADY SENT ITS OWN
      // MESSAGE TO THE OUTPUT: DON'T REPORT THE SAME ERROR TWICE (ISSUE #6439)
      if (!(e instanceof final ConsoleException ce && ce.isAlreadyReported()))
        outputError(e);
      throw e;
    }
  }

  private void executeSet(final String line) {
    // THE VALUE IS EVERYTHING AFTER THE FIRST '=': IT CAN CONTAIN FURTHER SEPARATORS AND IT CAN BE EMPTY (ISSUE #6392)
    final String[] pair = splitKeyValue(line);
    if (pair == null)
      throw new ConsoleException("Invalid syntax for SET, use SET <name> = <value>");
    if (pair[0].isBlank())
      // SAY WHICH HALF IS MISSING, LIKE THE `-D<key>=<value>` PATH ALREADY DOES
      throw new ConsoleException("Invalid syntax for SET: missing name, use SET <name> = <value>");

    final String key = pair[0].trim();
    // A QUOTED VALUE HAS ITS QUOTES STRIPPED, LIKE A SHELL WOULD: OTHERWISE `SET LANGUAGE = 'SQL'` STORES THE LITERAL
    // QUOTES, AND `LANGUAGE.STARTSWITH("SQL")` IN TerminalParser.setLanguage() SILENTLY PICKS THE WRONG COMMENT
    // MARKER (ISSUE #6439)
    final String value = stripMatchingQuotes(pair[1].trim());

    // THE SETTING NAMES ARE ASCII, SO THE CASE MUST FOLD IN ENGLISH: WITH A TURKISH DEFAULT LOCALE `LIMIT` WOULD NOT MATCH
    switch (key.toLowerCase(Locale.ENGLISH)) {
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
          ((Database) databaseProxy).async().onError(e -> {
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
      // THE LINE COMMENT MARKER DEPENDS ON THE LANGUAGE: `--` WITH SQL, `//` WITH THE OTHERS
      parser.setLanguage(language);
      outputLine(3, "Set language to %s", language);
    }
    case "expandresultset" -> {
      expandResultSet = "true".equalsIgnoreCase(value);
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

  /**
   * Strips one matching pair of surrounding quotes (' or ") from a {@code SET} value, so {@code set language = 'sql'} stores
   * {@code sql} rather than the literal quotes, and unescapes the backslash escapes inside that pair. A value that does not
   * start with a quote character is returned unchanged. A value that opens with a quote character but never closes it, or
   * ends in something other than that character, is always a typo, so it is rejected rather than stored half-quoted
   * (issue #6439).
   * <p>
   * This is the one place in the console where shell-like unescaping is wanted, and since {@link TerminalParser#parse} now
   * keeps the escape characters it sees (issue #6827) it is also the one place that can do it unambiguously: the closing
   * quote is the first UNESCAPED occurrence of the opening one, so {@code 'it\'s a test'} yields {@code it's a test} and
   * {@code 'a' b'} is still rejected as trailing garbage. Only the quote character that DELIMITS this value and the
   * backslash itself are unescaped: the other quote character never needed escaping in here, so a backslash in front of
   * it is data, and so is every other backslash - which is what makes {@code set foo = 'C:\Users'} store the path.
   */
  private static String stripMatchingQuotes(final String value) {
    if (value.isEmpty())
      return value;

    final char quote = value.charAt(0);
    if (quote != '\'' && quote != '"')
      return value;

    final StringBuilder content = new StringBuilder(value.length());
    for (int i = 1; i < value.length(); ++i) {
      final char c = value.charAt(i);

      if (c == '\\' && i + 1 < value.length()) {
        final char next = value.charAt(i + 1);
        if (next == quote || next == '\\') {
          content.append(next);
          ++i;
          continue;
        }
        content.append(c);
        continue;
      }

      if (c == quote) {
        if (i == value.length() - 1)
          return content.toString();
        throw new ConsoleException("Invalid value for SET: unexpected content after the closing quote in " + value);
      }

      content.append(c);
    }

    throw new ConsoleException("Invalid value for SET: missing closing quote in " + value);
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
    // THE PENDING BATCH IS GONE WITH THE TRANSACTION: LEAVING THE COUNTER SET MADE close() TRY TO COMMIT IT AGAIN
    // (ISSUE #6828)
    currentOperationsInBatch = 0;
  }

  private void executeRollback() {
    checkDatabaseIsOpen();
    databaseProxy.rollback();
    currentOperationsInBatch = 0;
  }

  private void executeClose() {
    if (databaseProxy != null) {
      if (databaseProxy.isTransactionActive())
        databaseProxy.commit();
      databaseProxy.close();
      databaseProxy = null;
    }
    currentOperationsInBatch = 0;
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
      final String[] databaseNames = new File(databaseDirectory).list();
      if (databaseNames != null)
        for (final String f : databaseNames) {
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
      // SPLIT ON RUNS OF WHITESPACE, LIKE THE REMOTE BRANCH: WITH `split(" ")` A SECOND BLANK BEFORE THE MODE PRODUCED
      // AN EMPTY TOKEN AND `MODE.valueOf("")` FAILED WITH AN ERROR THAT NAMES NEITHER THE MODE NOR THE BLANK (#6830)
      final String[] urlParts = url.split("\\s+");

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
      password = params.substring(identifiedByPos + "IDENTIFIED BY".length(), databasesByPos).trim();
      final String databasesList = params.substring(databasesByPos + " GRANT CONNECT TO ".length()).trim();
      final String[] databasesArray = databasesList.split(",");
      final List<String> databasesName = List.of(databasesArray);
      for (final String db : databasesName) {
        final int colonPos = db.indexOf(":");
        if (colonPos > -1) {
          final String dbname = db.substring(0, colonPos).trim();
          final String dbgroup = db.substring(colonPos + 1).trim();
          databases.put(dbname, dbgroup);
        } else {
          databases.put(db, "admin");
        }
      }
    } else {
      password = params.substring(identifiedByPos + "IDENTIFIED BY".length()).trim();
    }

    // AN OMITTED PASSWORD IS ASKED FOR WITH THE ECHO MASKED INSTEAD OF BEING REJECTED, SO `create user bob identified by`
    // IS THE FORM THAT NEVER WRITES THE PASSWORD TO `.history` OR TO A BUILD LOG (ISSUE #6829). SPACES ARE NO LONGER
    // REJECTED EITHER: `IDENTIFIED BY` AND `GRANT CONNECT TO` DELIMIT THE PASSWORD POSITIONALLY, THE SERVER AND STUDIO
    // BOTH ACCEPT IT, AND REJECTING IT ONLY HERE MADE AN ACCOUNT THE CONSOLE COULD NOT CREATE (ISSUE #6830)
    getRemoteServer().createUser(userName, password.isEmpty() ? askPassword(userName) : password, databases);

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
      // LONG-RUNNING MAINTENANCE COMMANDS (issue #5372): render a live progress line while the synchronous
      // command runs, polling the local registry (embedded) or the server's progress endpoint (remote).
      final Thread progressMonitor = startProgressMonitor(line);
      try {
        resultSet = databaseProxy.command(language, line);
      } catch (Exception e) {
        errored = true;
        if (batchMode && !failAtEnd)
          throw e;
        else
          outputError(e);
        return;
      } finally {
        stopProgressMonitor(progressMonitor);
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

    // COLLECTS THE LINES OF A BLOCK COMMENT OR A JSON OBJECT THAT SPANS MULTIPLE LINES, TO EXECUTE THEM ONLY ONCE THEY ARE
    // CLOSED. WITHOUT THIS, A MULTI-LINE `CONTENT { ... }` CLAUSE WOULD HIT reportUnbalancedBrace() ON ITS FIRST LINE, WHOSE
    // '}' IS SIMPLY ON A LINE NOT READ YET (ISSUE #6439)
    final StringBuilder pending = new StringBuilder();
    // THE FILE LINE (1-BASED) OF pending'S FIRST LINE, SO AN UNBALANCED BRACE IS REPORTED AT ITS REAL POSITION IN THE FILE
    // RATHER THAN RELATIVE TO A BUFFER THAT RESTARTS AFTER EVERY EXECUTED STATEMENT (ISSUE #6439)
    int fileLineNumber = 0;
    int pendingStartLine = 1;

    try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(file, DatabaseFactory.getDefaultCharset()))) {
      while (bufferedReader.ready()) {
        // READ AS TYPED. THE LINE USED TO GO THROUGH FileUtils.decodeFromFile(), WHOSE ONLY JOB WAS TO DOUBLE EVERY `\\`
        // SO THAT THE LEVEL OF ESCAPING TerminalParser THEN CONSUMED CAME BACK OUT EVEN. THAT PAIR CANCELLED OUT ONLY
        // FOR AN EVEN NUMBER OF BACKSLASHES: A LONE `\` IN A SCRIPT WAS STILL SWALLOWED. NOW THAT THE PARSER KEEPS WHAT
        // IT READS (ISSUE #6827), NEITHER HALF IS NEEDED AND A SCRIPT MEANS WHAT IT SAYS
        final String line = bufferedReader.readLine();
        ++fileLineNumber;

        if (pending.isEmpty())
          pendingStartLine = fileLineNumber;

        pending.append(line).append('\n');

        final ParsedLine parsedLine = parser.parse(pending.toString(), 0);
        if (parser.isBlockCommentOpen() || parser.getUnbalancedBraceOffset() >= 0) {
          // THE COMMENT OR THE JSON OBJECT CONTINUES ON THE NEXT LINE
          byteReadFromFile += line.length() + 1;
          continue;
        }

        pending.setLength(0);
        execute(parsedLine, true, pendingStartLine - 1);

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

    if (!pending.isEmpty())
      // THE FILE ENDS WITH AN UNTERMINATED BLOCK COMMENT OR JSON OBJECT: EXECUTE WHAT COMES BEFORE IT. IF IT IS A GENUINELY
      // UNCLOSED '{' RATHER THAN A COMMENT, THE CALL BELOW REPORTS IT THROUGH reportUnbalancedBrace() AS USUAL
      execute(parser.parse(pending.toString(), 0), true, pendingStartLine - 1);

    elapsed = System.currentTimeMillis() - startedOn;

    output(2, "\nFile processed in " + (elapsed / 1000) + " seconds");
    flushOutput();
  }

  public boolean parse(final String line) throws IOException {
    return parse(line, false);
  }

  public boolean parse(final String line, final boolean printCommand) throws IOException {
    return execute(parser.parse(line, 0), printCommand);
  }

  /**
   * Executes the commands extracted from the text by the parser.
   */
  private boolean execute(final ParsedLine parsedLine, final boolean printCommand) throws IOException {
    return execute(parsedLine, printCommand, 0);
  }

  /**
   * @param lineNumberBase number of lines that precede {@code parsedLine.line()} in the original source, so an unbalanced brace
   *                       is reported at its real position. Zero for interactive/batch input, where the parsed text is exactly
   *                       what was typed; {@code executeLoad} passes the count of file lines already consumed before the
   *                       current buffered statement started, since that buffer restarts after every executed statement and is
   *                       otherwise unaware of its position in the file (issue #6439).
   */
  private boolean execute(final ParsedLine parsedLine, final boolean printCommand, final int lineNumberBase) throws IOException {
    if (parsedLine == null)
      return true;

    // AN UNCLOSED '{' MAKES THE PARSER FOLD EVERYTHING FROM THAT POINT TO THE END OF THE TEXT INTO THE LAST WORD INSTEAD
    // OF SPLITTING IT ON ';' - REPORT IT HERE, POINTING AT THE BRACE THAT OPENED IT, RATHER THAN LETTING THE GLUED-TOGETHER
    // TEXT FAIL DOWNSTREAM WITH A SYNTAX ERROR THAT POINTS AT CODE THE USER TYPED SEVERAL STATEMENTS AGO (ISSUE #6439).
    // THIS RUNS ONLY WHEN A LINE IS ACTUALLY SUBMITTED FOR EXECUTION, NEVER ON THE KEYSTROKE-BY-KEYSTROKE PARSE CALLS
    // JLINE MAKES FOR HIGHLIGHTING/COMPLETION WHILE THE USER IS STILL TYPING.
    final int unbalancedBraceOffset = parser.getUnbalancedBraceOffset();

    final List<String> words = parsedLine.words();
    for (int i = 0; i < words.size(); ++i) {
      final String w = words.get(i);
      final String trimmedWord = w.trim();
      if (trimmedWord.isEmpty())
        // AN EMPTY LINE (OR A LEFTOVER LINE TERMINATOR BETWEEN TWO COMMANDS) SPLITS INTO A BLANK "WORD": SKIP IT SO
        // LOAD DOES NOT ECHO AN EMPTY PROMPT FOR IT (issue #6372)
        continue;

      if (printCommand)
        // USE THE TRIMMED WORD: AN UNTERMINATED LAST COMMAND CARRIES ITS TRAILING NEWLINE AS PART OF THE WORD, WHICH
        // WOULD OTHERWISE PUSH THE RESULT DOWN BY AN EXTRA BLANK LINE (issue #6372). MASK ANY INLINE PASSWORD: THIS
        // ECHO IS WHAT BATCH MODE WRITES TO STDOUT, I.E. STRAIGHT INTO A CI LOG (issue #6829)
        output(3, getPrompt() + ConsoleCredentials.mask(trimmedWord));

      // THE UNCLOSED BRACE, IF ANY, IS ALWAYS SOMEWHERE INSIDE THE LAST WORD: EVERY SEMICOLON FROM THE BRACE ONWARD FAILED
      // TO SEPARATE ANYTHING, SO THE REST OF THE INPUT WAS NEVER SPLIT
      final boolean isUnbalancedBraceTail = i == words.size() - 1 && unbalancedBraceOffset >= 0;

      if (batchMode) {
        try {
          if (isUnbalancedBraceTail)
            reportUnbalancedBrace(parsedLine.line(), unbalancedBraceOffset, lineNumberBase);
          else if (!execute(w))
            return false;
        } catch (final Exception e) {
          errored = true;
          if (!failAtEnd)
            throw e;
        }
      } else {
        if (isUnbalancedBraceTail)
          reportUnbalancedBrace(parsedLine.line(), unbalancedBraceOffset, lineNumberBase);
        else if (!execute(w))
          return false;
      }
    }
    return true;
  }

  /**
   * Reports an unclosed '{' found while splitting the input, naming the line and column where it was opened, and rethrows so
   * the caller aborts exactly as it would for any other command error (issue #6439).
   */
  private void reportUnbalancedBrace(final String text, final int offset, final int lineNumberBase) {
    int lineNo = 1;
    int col = 1;
    for (int i = 0; i < offset && i < text.length(); ++i) {
      if (text.charAt(i) == '\n') {
        ++lineNo;
        col = 1;
      } else
        ++col;
    }

    final ConsoleException ex = new ConsoleException(
        "Unbalanced '{' at line " + (lineNumberBase + lineNo) + ", column " + col + ": no matching '}' was found, so everything "
            + "from there to the end of the input was treated as a single command instead of being split on ';'", true);
    outputError(ex);
    throw ex;
  }

  private void outputLine(final int level, final String text, final Object... args) {
    output(level, "\n" + text, args);
  }

  /** The maintenance commands that publish live progress in the operation registry (issues #5372, #5376). */
  private static final String[] PROGRESS_MONITORED_COMMANDS = { "check database", "rebuild index", "compact index",
      "backup database", "import database" };

  /**
   * Starts the live progress line for long-running maintenance commands (issue #5372), or returns null when
   * not applicable (not a monitored command, or the output is redirected to an embedding application).
   * Polling is best-effort: any failure silently stops the rendering, never the command.
   */
  private Thread startProgressMonitor(final String line) {
    if (output != null || verboseLevel < 2)
      return null;
    // Collapse whitespace runs so `REBUILD   INDEX` matches too, consistently with the Studio matcher.
    final String normalized = line.trim().toLowerCase(Locale.ENGLISH).replaceAll("\\s+", " ");
    boolean monitored = false;
    for (final String command : PROGRESS_MONITORED_COMMANDS)
      if (normalized.startsWith(command)) {
        monitored = true;
        break;
      }
    if (!monitored)
      return null;

    progressMonitorStopped = false;
    final Thread monitor = new Thread(() -> {
      int lastRenderedLength = 0;
      try {
        while (!Thread.currentThread().isInterrupted() && !progressMonitorStopped) {
          Thread.sleep(500);
          final String rendered = renderProgressLine();
          // RE-CHECK THE STOP FLAG RIGHT BEFORE PRINTING: if the command completed while we were polling, a
          // late line must not interleave with the result rendering on the main thread.
          if (rendered != null && !progressMonitorStopped) {
            // PAD WITH SPACES so a shorter update fully overwrites the previous, longer one.
            terminal.writer().print("\r" + rendered + " ".repeat(Math.max(0, lastRenderedLength - rendered.length())));
            terminal.writer().flush();
            lastRenderedLength = rendered.length();
          }
        }
      } catch (final InterruptedException e) {
        // COMMAND FINISHED
      } catch (final Exception e) {
        // BEST-EFFORT: a progress-polling failure must never disturb the running command
      } finally {
        if (lastRenderedLength > 0) {
          terminal.writer().print("\r" + " ".repeat(lastRenderedLength) + "\r");
          terminal.writer().flush();
        }
      }
    }, "ArcadeDB-Console-Progress");
    monitor.setDaemon(true);
    monitor.start();
    return monitor;
  }

  private volatile boolean progressMonitorStopped;

  private void stopProgressMonitor(final Thread monitor) {
    if (monitor == null)
      return;
    // SET THE FLAG FIRST: the interrupt alone leaves a window where a poll in flight could still print after
    // this method returns and the main thread starts rendering the command result.
    progressMonitorStopped = true;
    monitor.interrupt();
    try {
      monitor.join(2_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * One line describing the OLDEST running operation of the current database, or null when none is running.
   * Deliberate limitation: with several concurrent operations on the same database only the oldest is
   * rendered - a single console line cannot show more, and the HTTP endpoint still exposes all of them.
   */
  private String renderProgressLine() {
    if (isRemoteDatabase()) {
      final List<JSONObject> operations = ((RemoteDatabase) databaseProxy).getProgress();
      if (operations.isEmpty())
        return null;
      final JSONObject op = operations.getFirst();
      return formatProgressLine(op.getString("operation", ""), op.getString("stepName", ""),
          op.getInt("stepIndex", 0), op.getInt("totalSteps", 0), op.getInt("percentage", -1));
    }

    final List<OperationProgress> operations = OperationProgressRegistry.instance().getOperations(databaseProxy.getName());
    if (operations.isEmpty())
      return null;
    final OperationProgress op = operations.getFirst();
    return formatProgressLine(op.getOperation(), op.getStepName(), op.getStepIndex(), op.getTotalSteps(), op.getPercentage());
  }

  static String formatProgressLine(final String operation, final String stepName, final int stepIndex,
      final int totalSteps, final int percentage) {
    final StringBuilder line = new StringBuilder(128);
    line.append(operation).append(" [step ").append(stepIndex).append('/').append(totalSteps).append("] ").append(stepName);
    if (percentage >= 0) {
      final int filled = Math.min(20, percentage / 5);
      line.append(" |").append("=".repeat(filled)).append(" ".repeat(20 - filled)).append("| ").append(percentage).append('%');
    } else
      line.append(" ...");
    return line.toString();
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

    if ("types".equalsIgnoreCase(subject)) {
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
    } else if ("transaction".equalsIgnoreCase(subject))
      executeTransactionStatus();
    else if (subject.startsWith("type ")) {
      final String typeName = subject.substring("type ".length()).trim();

      final TableFormatter table = new TableFormatter((text, args) -> output(0, text, args));
      table.setMaxWidthSize(maxWidth);

      try (final ResultSet typeResult = databaseProxy.command("sql",
          "select from schema:types where name = :name", Map.of("name", typeName))) {
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

    // SPLIT ON RUNS OF WHITESPACE AND STOP AT THE PASSWORD. `split(" ")` PRODUCED AN EMPTY TOKEN FOR EVERY EXTRA BLANK,
    // AND THE LIMIT KEEPS THE PASSWORD IN ONE PIECE: A PASSWORD CONTAINING A SPACE IS ACCEPTED BY THE SERVER AND BY
    // STUDIO, SO SUCH AN ACCOUNT MUST NOT BE UNUSABLE FROM THE CONSOLE (ISSUE #6830)
    final String[] serverUserPassword = conn.trim().split("\\s+", 3);
    if (serverUserPassword.length < 2)
      // SAY WHICH HALF IS MISSING RATHER THAN SENDING THE USER LOOKING FOR THE WRONG PROBLEM
      throw new ConsoleException(
          "User name is missing, use `" + REMOTE_PREFIX + "<host>[:<port>]" + (needsDatabase ? "/<database>" : "")
              + " <user> [<password>]`");

    final String userName = serverUserPassword[1];
    if (userName.isEmpty())
      throw new ConsoleException("User name is empty");

    // AN OMITTED PASSWORD IS ASKED FOR WITH THE ECHO MASKED, SO IT NEVER APPEARS ON A LINE THAT IS SAVED TO THE HISTORY
    // FILE OR ECHOED INTO A BUILD LOG (ISSUE #6829)
    final String password = serverUserPassword.length == 3 ? serverUserPassword[2] : askPassword(userName);

    final String[] serverParts = serverUserPassword[0].split("/");
    if ((needsDatabase && serverParts.length != 2) || (!needsDatabase && serverParts.length != 1))
      // REPORT ONLY THE ADDRESS, NEVER THE WHOLE ARGUMENT: `url` STILL CARRIES THE INLINE PASSWORD, AND THIS MESSAGE
      // GOES TO THE INTERACTIVE OUTPUT AND TO THE BATCH LOG. IT IS ALSO THE MORE PRECISE HALF - THE ADDRESS IS WHAT
      // FAILED TO SPLIT (ISSUE #6829)
      throw new ConsoleException(
          "Remote URL '" + REMOTE_PREFIX + serverUserPassword[0] + "' is not valid, expected " + REMOTE_PREFIX
              + "<host>[:<port>]" + (needsDatabase ? "/<database>" : ""));

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

    databaseProxy = new RemoteDatabase(remoteServer, remotePort, needsDatabase ? serverParts[1] : "", userName, password);
    this.remoteServer = new RemoteServer(remoteServer, remotePort, userName, password);
  }

  /**
   * Reads a password from the terminal with the echo masked.
   * <p>
   * This is the only way to authenticate without writing the password down somewhere that outlives the command: the
   * `connect` syntax carries it inline, the line reader saves every line to `.history`, and batch mode echoes the same
   * line to stdout (issue #6829). Without a real terminal there is nobody to type it, so the caller gets the same
   * "password is missing" it would have got before.
   */
  private String askPassword(final String userName) {
    if (batchMode || !systemTerminal)
      throw new ConsoleException("Password for user '" + userName + "' is missing");

    final LineReader reader = getLineReader();

    // BELT AND BRACES ON THE ONE LINE THAT IS A BARE PASSWORD. jline ALREADY DROPS A MASKED LINE - ITS
    // SimpleMaskingCallback.history() RETURNS null AND LineReaderImpl.finish() SKIPS A null - BUT THAT IS A LIBRARY
    // INTERNAL, AND ConsoleCredentials CANNOT BE THE SAFETY NET HERE: IT RECOGNISES A PASSWORD BY THE COMMAND KEYWORD
    // IN FRONT OF IT, AND THIS LINE HAS NONE. SAYING IT OUT LOUD KEEPS THE GUARANTEE LOCAL TO THE CODE THAT NEEDS IT
    final Object previousSetting = reader.getVariable(LineReader.DISABLE_HISTORY);
    reader.setVariable(LineReader.DISABLE_HISTORY, true);
    final String password;
    try {
      password = reader.readLine("Password for '" + userName + "': ", '*');
    } catch (final UserInterruptException | EndOfFileException e) {
      // CTRL-C / CTRL-D AT THE PROMPT IS A DECISION, NOT A FAILURE: SAY SO INSTEAD OF LETTING A jline EXCEPTION
      // SURFACE AS AN OPAQUE ERROR FROM connect / create user
      throw new ConsoleException("Password entry cancelled");
    } finally {
      reader.setVariable(LineReader.DISABLE_HISTORY, previousSetting);
    }

    if (password == null || password.isEmpty())
      throw new ConsoleException("Password for user '" + userName + "' is empty");
    return password;
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

  /**
   * Splits a `&lt;key&gt;=&lt;value&gt;` pair on the FIRST '=' only. Shared by the `-D&lt;key&gt;=&lt;value&gt;`
   * arguments (issue #5928) and by the SET command (issue #6392), which used to disagree on the rule; delegates to
   * {@link StringUtils#splitKeyValue} so the Postgres wire's SET command and the Studio include directive
   * (issue #6423) follow the same rule instead of each carrying their own truncating copy.
   */
  static String[] splitKeyValue(final String pair) {
    return StringUtils.splitKeyValue(pair);
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
