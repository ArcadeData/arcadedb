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
package com.arcadedb.console;

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
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

import java.io.*;
import java.util.*;

public class Console {
  private static final String           PROMPT               = "%n%s> ";
  private final        boolean          system               = System.console() != null;
  private final        Terminal         terminal;
  private final        LineReader       lineReader;
  private final        TerminalParser   parser               = new TerminalParser();
  private              RemoteDatabase   remoteDatabase;
  private              ConsoleOutput    output;
  private              DatabaseFactory  databaseFactory;
  private              DatabaseInternal localDatabase;
  private              int              limit                = 20;
  private              int              maxMultiValueEntries = 10;
  private              Boolean          expandResultset;
  private              ResultSet        resultSet;
  private              String           rootDirectory;
  private              String           databaseDirectory;

  private String getPrompt() {
    return String.format(PROMPT, localDatabase != null ? "{" + localDatabase.getName() + "}" : "");
  }

  public Console(final boolean interactive) throws IOException {
    setRootPath("");

    GlobalConfiguration.PROFILE.setValue("low-cpu");

    terminal = TerminalBuilder.builder().system(system).streams(System.in, System.out).jansi(true).build();
    Completer completer = new StringsCompleter("begin", "rollback", "commit", "check database", "close", "connect", "create database", "drop database",
        "export", "import", "help", "info types", "load", "exit", "quit", "set", "match", "select", "insert into", "update", "delete", "pwd");

    lineReader = LineReaderBuilder.builder().terminal(terminal).parser(parser).variable("history-file", ".history").history(new DefaultHistory())
        .completer(completer).build();

    output("%s Console v.%s - %s (%s)%n", Constants.PRODUCT, Constants.getRawVersion(), Constants.COPYRIGHT, Constants.URL);

    if (!interactive)
      return;

    lineReader.getHistory().load();

    try {
      while (true) {

        try {
          String line = lineReader.readLine(getPrompt());
          if (line == null)
            continue;

          lineReader.getHistory().save();

          if (!parse(line, false))
            return;

        } catch (UserInterruptException e) {
          return;
        } catch (EndOfFileException e) {
          return;
        } catch (Exception e) {
          terminal.writer().print("\nError: " + e.getMessage() + "\n");
        }
      }
    } finally {
      close();
    }
  }

  public static void main(String[] args) throws IOException {
    new Console(true);
  }

  public void close() {
    if (terminal != null)
      terminal.writer().flush();

    if (remoteDatabase != null) {
      remoteDatabase.close();
      remoteDatabase = null;
    }

    if (localDatabase != null) {
      localDatabase.close();
      localDatabase = null;
    }

    if (databaseFactory != null) {
      databaseFactory.close();
      databaseFactory = null;
    }
  }

  public Console setRootPath(final String rootDirectory) {
    this.rootDirectory = rootDirectory;

    if (this.rootDirectory == null || this.rootDirectory.isEmpty())
      this.rootDirectory = "";
    else if (this.rootDirectory.endsWith("/"))
      this.rootDirectory = this.rootDirectory.substring(0, this.rootDirectory.length() - 1);

    if (!new File(this.rootDirectory + "config").exists() && new File(this.rootDirectory + "../config").exists()) {
      databaseDirectory = new File(this.rootDirectory).getAbsoluteFile().getParentFile().getPath() + "/databases";
    } else
      databaseDirectory = this.rootDirectory + "/databases";

    return this;
  }

  public void setOutput(final ConsoleOutput output) {
    this.output = output;
  }

  private boolean execute(final String line) throws IOException {
    if (line != null && !line.isEmpty()) {
      if (line.startsWith("begin"))
        executeBegin();
      else if (line.startsWith("close"))
        executeClose();
      else if (line.startsWith("commit"))
        executeCommit();
      else if (line.startsWith("connect"))
        executeConnect(line);
      else if (line.startsWith("create database"))
        executeCreateDatabase(line);
      else if (line.startsWith("drop database"))
        executeDropDatabase(line);
      else if (line.equalsIgnoreCase("help") || line.equals("?"))
        executeHelp();
      else if (line.startsWith("info"))
        executeInfo(line.substring("info".length()).trim());
      else if (line.startsWith("load"))
        executeLoad(line.substring("load".length()).trim());
      else if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
        executeClose();
        return false;
      } else if (line.startsWith("pwd"))
        terminal.writer().print("Current directory: " + new File(".").getAbsolutePath());
      else if (line.startsWith("rollback"))
        executeRollback();
      else if (line.startsWith("set"))
        executeSet(line.substring("set".length()).trim());
      else {
        executeSQL(line);
      }
    }

    output("%n");

    return true;
  }

  private void executeSet(final String line) {
    if (line == null || line.isEmpty())
      return;

    final String[] parts = line.split("=");
    if (parts.length != 2)
      return;

    final String key = parts[0].trim();
    final String value = parts[1].trim();

    if ("limit".equalsIgnoreCase(key)) {
      limit = Integer.parseInt(value);
      output("Set new limit = %d", limit);
    } else if ("expandResultset".equalsIgnoreCase(key)) {
      expandResultset = value.equalsIgnoreCase("true");
    } else if ("maxMultiValueEntries".equalsIgnoreCase(key))
      maxMultiValueEntries = Integer.parseInt(value);
  }

  private void executeTransactionStatus() {
    checkDatabaseIsOpen();

    final TransactionContext tx = localDatabase.getTransaction();
    if (tx.isActive()) {
      final ResultInternal row = new ResultInternal();
      row.setPropertiesFromMap(tx.getStats());
      printRecord(row);

    } else
      output("%nTransaction is not Active");
  }

  private void executeBegin() {
    checkDatabaseIsOpen();
    if (localDatabase != null)
      localDatabase.begin();
    else
      remoteDatabase.begin();
  }

  private void executeCommit() {
    checkDatabaseIsOpen();
    if (localDatabase != null)
      localDatabase.commit();
    else
      remoteDatabase.commit();
  }

  private void executeRollback() {
    checkDatabaseIsOpen();
    if (localDatabase != null)
      localDatabase.rollback();
    else
      remoteDatabase.rollback();
  }

  private void executeClose() {
    if (localDatabase != null) {
      if (localDatabase.isTransactionActive())
        localDatabase.commit();

      localDatabase.close();
      localDatabase = null;
    }

    if (remoteDatabase != null) {
      remoteDatabase.close();
      remoteDatabase = null;
    }
  }

  private void executeConnect(final String line) {
    final String url = line.substring("connect".length()).trim();

    final String[] urlParts = url.split(" ");

    if (localDatabase != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!urlParts[0].isEmpty()) {
      if (urlParts[0].startsWith("remote:")) {
        connectToRemoteServer(url);

        terminal.writer().printf("%nConnected%n");
        terminal.writer().flush();

      } else {
        PaginatedFile.MODE mode = PaginatedFile.MODE.READ_WRITE;
        if (urlParts.length > 1)
          mode = PaginatedFile.MODE.valueOf(urlParts[1].toUpperCase());

        databaseFactory = new DatabaseFactory(urlParts[0]);
        localDatabase = (DatabaseInternal) databaseFactory.setAutoTransaction(true).open(mode);

      }
    } else
      throw new ConsoleException("URL missing");
  }

  private void executeCreateDatabase(final String line) {
    String url = line.substring("create database".length()).trim();
    if (localDatabase != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty()) {
      if (url.startsWith("remote:")) {
        connectToRemoteServer(url);
        remoteDatabase.create();

        terminal.writer().printf("%nDatabase created%n");
        terminal.writer().flush();

      } else {
        if (url.startsWith("file://"))
          url = url.substring("file://".length());

        if (!databaseDirectory.endsWith("/"))
          url = databaseDirectory + "/" + url;
        else
          url = databaseDirectory + url;

        databaseFactory = new DatabaseFactory(url);
        localDatabase = (DatabaseInternal) databaseFactory.setAutoTransaction(true).create();
      }
    } else
      throw new ConsoleException("URL missing");
  }

  private void executeDropDatabase(final String line) {
    final String url = line.substring("drop database".length()).trim();
    if (localDatabase != null || remoteDatabase != null)
      terminal.writer().print("Database already connected, to connect to a different database close the current one first\n");
    else if (!url.isEmpty()) {
      if (url.startsWith("remote:")) {
        connectToRemoteServer(url);
        remoteDatabase.drop();

        terminal.writer().printf("%nDatabase dropped%n");
        terminal.writer().flush();

      } else {
        databaseFactory = new DatabaseFactory(url);
        localDatabase = (DatabaseInternal) databaseFactory.setAutoTransaction(true).open();
        localDatabase.drop();
      }
    } else
      throw new ConsoleException("URL missing");

    remoteDatabase = null;
    localDatabase = null;
  }

  private void printRecord(final Result currentRecord) {
    if (currentRecord == null)
      return;

    final Document rec = currentRecord.getElement().orElse(null);

    if (rec instanceof Vertex)
      output("%nVERTEX @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());
    else if (rec instanceof Edge)
      output("%nEDGE @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());
    else if (rec != null)
      output("%nDOCUMENT @type:%s @rid:%s", rec.getTypeName(), rec.getIdentity());

    final List<TableFormatter.TableRow> resultSet = new ArrayList<>();

    Object value;
    for (String fieldName : currentRecord.getPropertyNames()) {
      value = currentRecord.getProperty(fieldName);
      if (value instanceof byte[])
        value = "byte[" + ((byte[]) value).length + "]";
      else if (value instanceof Iterator<?>) {
        final List<Object> coll = new ArrayList<>();
        while (((Iterator<?>) value).hasNext())
          coll.add(((Iterator<?>) value).next());
        value = coll;
      } else if (MultiValue.isMultiValue(value)) {
        value = TableFormatter.getPrettyFieldMultiValue(MultiValue.getMultiValueIterator(value), maxMultiValueEntries);
      }

      final ResultInternal row = new ResultInternal();
      resultSet.add(new RecordTableFormatter.TableRecordRow(row));

      row.setProperty("NAME", fieldName);
      row.setProperty("VALUE", value);
    }

    final TableFormatter formatter = new TableFormatter(new TableFormatter.TableOutput() {
      @Override
      public void onMessage(String text, Object... args) {
        output(text, args);
      }
    });
    formatter.writeRows(resultSet, -1);

    output("%n");
  }

  private void executeSQL(final String line) {
    checkDatabaseIsOpen();

    final long beginTime = System.currentTimeMillis();

    if (remoteDatabase != null)
      resultSet = remoteDatabase.command("SQL", line);
    else
      resultSet = localDatabase.command("SQL", line);

    final long elapsed;

    Boolean expandOnThisQuery = expandResultset;

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
      final TableFormatter table = new TableFormatter(new TableFormatter.TableOutput() {
        @Override
        public void onMessage(String text, Object... args) {
          output(text, args);
        }
      });
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

    terminal.writer().printf("%nCommand executed in %dms%n", elapsed);
  }

  private void executeLoad(final String fileName) throws IOException {
    if (fileName.isEmpty())
      throw new ArcadeDBException("File name is empty");

    final File file = new File(fileName);
    if (!file.exists())
      throw new ArcadeDBException("File name '" + fileName + "' not found");

    try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(file, DatabaseFactory.getDefaultCharset()))) {
      while (bufferedReader.ready())
        parse(bufferedReader.readLine(), true);
    }
  }

  public boolean parse(final String line, final boolean printCommand) throws IOException {
    final ParsedLine parsed = parser.parse(line, 0);

    for (String w : parsed.words()) {
      if (printCommand)
        terminal.writer().printf(getPrompt() + w);

      if (!execute(w))
        return false;
    }
    return true;
  }

  private void output(final String text, final Object... args) {
    if (output != null)
      output.onOutput(String.format(text, args));
    else
      terminal.writer().printf(text, args);
  }

  private void executeInfo(final String subject) {
    if (subject == null || subject.isEmpty())
      return;

    checkDatabaseIsOpen();

    if (subject.equalsIgnoreCase("types")) {
      output("%nAVAILABLE TYPES");

      final TableFormatter table = new TableFormatter(new TableFormatter.TableOutput() {
        @Override
        public void onMessage(String text, Object... args) {
          output(text, args);
        }
      });

      if (remoteDatabase != null) {
        executeSQL("select from schema:types");
        return;
      }

      final List<TableFormatter.TableMapRow> rows = new ArrayList<>();
      for (DocumentType type : localDatabase.getSchema().getTypes()) {
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
    else
      throw new ConsoleException("Information about '" + subject + "' is not available");
  }

  private void executeHelp() {
    output("%nHELP");
    output("%nbegin                               -> begins a new transaction");
    output("%ncheck database                      -> check database integrity");
    output("%nclose                               -> closes the database");
    output("%ncreate database <path>|remote:<url> -> creates a new database");
    output("%ncommit                              -> commits current transaction");
    output("%nconnect <path>|remote:<url>         -> connects to a database stored on <path>");
    output("%nhelp|?                              -> ask for this help");
    output("%ninfo types                          -> print available types");
    output("%ninfo transaction                    -> print current transaction");
    output("%nrollback                            -> rollbacks current transaction");
    output("%nquit or exit                        -> exits from the console");
    output("%n");
  }

  private void checkDatabaseIsOpen() {
    if (localDatabase == null && remoteDatabase == null)
      throw new ArcadeDBException("No active database. Open a database first");
  }

  private void connectToRemoteServer(final String url) {
    final String conn = url.substring("remote:".length());

    final String[] serverUserPassword = conn.split(" ");
    if (serverUserPassword.length != 3)
      throw new ConsoleException("URL username and password are missing");

    final String[] serverParts = serverUserPassword[0].split("/");
    if (serverParts.length != 2)
      throw new ConsoleException("Remote URL '" + url + "' not valid");

    String remoteServer;
    int remotePort;

    final int portPos = serverParts[0].indexOf(":");
    if (portPos < 0) {
      remoteServer = serverParts[0];
      remotePort = RemoteDatabase.DEFAULT_PORT;
    } else {
      remoteServer = serverParts[0].substring(0, portPos);
      remotePort = Integer.parseInt(serverParts[0].substring(portPos + 1));
    }

    remoteDatabase = new RemoteDatabase(remoteServer, remotePort, serverParts[1], serverUserPassword[1], serverUserPassword[2]);
  }
}
