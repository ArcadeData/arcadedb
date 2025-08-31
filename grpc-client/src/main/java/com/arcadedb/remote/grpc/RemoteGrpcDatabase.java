package com.arcadedb.remote.grpc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteImmutableDocument;
import com.arcadedb.remote.RemoteImmutableEdge;
import com.arcadedb.remote.RemoteImmutableVertex;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteTransactionExplicitLock;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.BatchAck;
import com.arcadedb.server.grpc.BeginTransactionRequest;
import com.arcadedb.server.grpc.BeginTransactionResponse;
import com.arcadedb.server.grpc.CommitInsert;
import com.arcadedb.server.grpc.CommitTransactionRequest;
import com.arcadedb.server.grpc.CommitTransactionResponse;
import com.arcadedb.server.grpc.CreateRecordRequest;
import com.arcadedb.server.grpc.CreateRecordResponse;
import com.arcadedb.server.grpc.DatabaseCredentials;
import com.arcadedb.server.grpc.DeleteRecordRequest;
import com.arcadedb.server.grpc.DeleteRecordResponse;
import com.arcadedb.server.grpc.DropDatabaseRequest;
import com.arcadedb.server.grpc.DropDatabaseResponse;
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.ExecuteCommandResponse;
import com.arcadedb.server.grpc.ExecuteQueryRequest;
import com.arcadedb.server.grpc.ExecuteQueryResponse;
import com.arcadedb.server.grpc.GetRecordRequest;
import com.arcadedb.server.grpc.GetRecordResponse;
import com.arcadedb.server.grpc.InsertChunk;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertRequest;
import com.arcadedb.server.grpc.InsertResponse;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.grpc.QueryResult;
import com.arcadedb.server.grpc.RollbackTransactionRequest;
import com.arcadedb.server.grpc.RollbackTransactionResponse;
import com.arcadedb.server.grpc.StartInsert;
import com.arcadedb.server.grpc.StreamQueryRequest;
import com.arcadedb.server.grpc.TransactionContext;
import com.arcadedb.server.grpc.TransactionIsolation;
import com.arcadedb.server.grpc.UpdateRecordRequest;
import com.arcadedb.server.grpc.UpdateRecordResponse;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import io.grpc.CallCredentials;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Remote Database implementation using gRPC protocol instead of HTTP. Extends
 * RemoteDatabase to maintain compatibility while overriding network operations.
 * It's not thread safe. For multi-thread usage create one instance of
 * RemoteGrpcDatabase per thread.
 *
 * @author Oleg Cohen (oleg.cohen@gmail.com)
 */
public class RemoteGrpcDatabase extends RemoteDatabase {
	
	private final ManagedChannel channel;
	
	private final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub blockingStub;
	private final ArcadeDbServiceGrpc.ArcadeDbServiceStub asyncStub;
	
	private String transactionId;
	
	private final RemoteSchema schema;

	private final String userName;
	private final String userPassword;
	private String databaseName;
	private RemoteTransactionExplicitLock explicitLock;

	public RemoteGrpcDatabase(final String server, final int port, final String databaseName, final String userName, final String userPassword) {
		this(server, port, databaseName, userName, userPassword, new ContextConfiguration());
	}

	public RemoteGrpcDatabase(final String server, final int port, final String databaseName, final String userName, final String userPassword,
			final ContextConfiguration configuration) {

		super(server, 2480, databaseName, userName, userPassword, configuration);

		this.userName = userName;
		this.userPassword = userPassword;
		
		this.databaseName = databaseName;

		// Create gRPC channel
		this.channel = createChannel(server, port);

		// Create stubs with authentication
		CallCredentials credentials = createCallCredentials(userName, userPassword);

		this.blockingStub = createBlockingStub(channel);

		this.asyncStub = createAsyncStub(channel);

		// Create gRPC-specific schema
		this.schema = new RemoteSchema(this);
	}
	
	/**
	 * Creates the gRPC channel. Override this for custom channel configuration.
	 */
	protected ManagedChannel createChannel(String server, int port) {
		return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS by default
				.build();
	}

	/**
	 * Creates call credentials for authentication
	 */
	protected CallCredentials createCallCredentials(String userName, String userPassword) {
		return new CallCredentials() {
			@Override
			public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
				Metadata headers = new Metadata();
				headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), databaseName);
				applier.apply(headers);
			}

			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}

	protected CallCredentials createCredentials() {
		return new CallCredentials() {
			@Override
			public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
				Metadata headers = new Metadata();
				headers.put(Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), userName);
				headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), userPassword);
				headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), databaseName);
				applier.apply(headers);
			}

			// x-arcade-user: root" -H "x-arcade-password: oY9uU2uJ8nD8iY7t" -H "x-arcade-database: local_shakeiq_curonix_poc-app"
			@Override
			public void thisUsesUnstableApi() {
				// Required by the interface
			}
		};
	}

	/**
	 * Override this method to customize blocking stub creation
	 */
	protected ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub createBlockingStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newBlockingStub(channel).withCallCredentials(createCredentials());
	}

	protected ArcadeDbServiceGrpc.ArcadeDbServiceStub createAsyncStub(ManagedChannel channel) {
		return ArcadeDbServiceGrpc.newStub(channel).withCallCredentials(createCredentials()).withCompression("gzip");
	}

	@Override
	public String getDatabasePath() {
		return "grpc://" + channel.authority() + "/" + getName();
	}

	@Override
	public RemoteSchema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		if (transactionId != null) {
			rollback();
		}
		super.close();

		// Shutdown gRPC channel
		channel.shutdown();
		try {
			if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
				channel.shutdownNow();
			}
		}
		catch (InterruptedException e) {
			channel.shutdownNow();
		}
	}

	@Override
	public void drop() {
		checkDatabaseIsOpen();

		DropDatabaseRequest request = DropDatabaseRequest.newBuilder().setDatabaseName(getName()).setCredentials(buildCredentials()).build();

		try {
			DropDatabaseResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.dropDatabase(request);
			if (!response.getSuccess()) {
				throw new DatabaseOperationException("Failed to drop database: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			throw new DatabaseOperationException("Error on deleting database", e);
		}

		close();
	}

	@Override
	public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
		checkDatabaseIsOpen();
		if (transactionId != null)
			throw new TransactionException("Transaction already begun");

		BeginTransactionRequest request = BeginTransactionRequest.newBuilder().setDatabase(getName()).setCredentials(buildCredentials())
				.setIsolation(mapIsolationLevel(isolationLevel)).build();

		try {
			BeginTransactionResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.beginTransaction(request);
			transactionId = response.getTransactionId();
			// Store transaction ID in parent class session management
			setSessionId(transactionId);
		}
		catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction begin", e);
		}
	}

	@Override
	public void commit() {
		checkDatabaseIsOpen();
		stats.txCommits.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		CommitTransactionRequest request = CommitTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			CommitTransactionResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.commitTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to commit transaction: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
		}
		finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public void rollback() {
		checkDatabaseIsOpen();
		stats.txRollbacks.incrementAndGet();

		if (transactionId == null)
			throw new TransactionException("Transaction not begun");

		RollbackTransactionRequest request = RollbackTransactionRequest.newBuilder()
				.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build())
				.setCredentials(buildCredentials()).build();

		try {
			RollbackTransactionResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.rollbackTransaction(request);
			if (!response.getSuccess()) {
				throw new TransactionException("Failed to rollback transaction: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			throw new TransactionException("Error on transaction rollback", e);
		}
		finally {
			transactionId = null;
			setSessionId(null);
		}
	}

	@Override
	public Record lookupByRID(final RID rid, final boolean loadContent) {
		checkDatabaseIsOpen();
		stats.readRecord.incrementAndGet();

		if (rid == null)
			throw new IllegalArgumentException("Record is null");

		GetRecordRequest request = GetRecordRequest.newBuilder().setDatabase(getName()).setRid(rid.toString()).setCredentials(buildCredentials())
				.build();

		try {
			GetRecordResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.getRecord(request);
			if (!response.getFound()) {
				throw new RecordNotFoundException("Record " + rid + " not found", rid);
			}
			return grpcRecordToRecord(response.getRecord());
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return null;
		}
	}

	@Override
	public void deleteRecord(final Record record) {
		checkDatabaseIsOpen();
		stats.deleteRecord.incrementAndGet();

		if (record.getIdentity() == null)
			throw new IllegalArgumentException("Cannot delete a non persistent record");

		DeleteRecordRequest request = DeleteRecordRequest.newBuilder().setDatabase(getName()).setRid(record.getIdentity().toString())
				.setCredentials(buildCredentials()).build();

		try {
			DeleteRecordResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.deleteRecord(request);
			if (!response.getSuccess()) {
				throw new DatabaseOperationException("Failed to delete record: " + response.getMessage());
			}
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
		}
	}

	@Override
	public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
		String query = "select from `" + typeName + "`";
		if (!polymorphic)
			query += " where @type = '" + typeName + "'";
		return streamQuery(query);
	}

	@Override
	public Iterator<Record> iterateBucket(final String bucketName) {
		return streamQuery("select from bucket:`" + bucketName + "`");
	}

	@Override
	public ResultSet query(final String language, final String query, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		ExecuteQueryRequest.Builder requestBuilder = ExecuteQueryRequest.newBuilder().setDatabase(getName()).setQuery(query)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToProto(params));
		}

		try {
			ExecuteQueryResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeQuery(requestBuilder.build());
			return createGrpcResultSet(response);
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	@Override
	public ResultSet query(final String language, final String query, final Object... args) {
		checkDatabaseIsOpen();
		stats.queries.incrementAndGet();

		final Map<String, Object> params = mapArgs(args);
		
		return query(language, query, params);
	}
	
	@Override
	public ResultSet command(final String language, final String command, final Map<String, Object> params) {
		checkDatabaseIsOpen();
		stats.commands.incrementAndGet();

		ExecuteCommandRequest.Builder requestBuilder = ExecuteCommandRequest.newBuilder().setDatabase(getName()).setCommand(command)
				.setCredentials(buildCredentials());

		if (transactionId != null) {
			requestBuilder.setTransaction(TransactionContext.newBuilder().setTransactionId(transactionId).setDatabase(getName()).build());
		}

		if (params != null && !params.isEmpty()) {
			requestBuilder.putAllParameters(convertParamsToProto(params));
		}

		try {
			ExecuteCommandResponse response = blockingStub
					.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
					.executeCommand(requestBuilder.build());

			// Create result set with command execution info
			InternalResultSet resultSet = new InternalResultSet();
			if (response.getAffectedRecords() > 0) {
				Map<String, Object> result = new HashMap<>();
				result.put("affected", response.getAffectedRecords());
				result.put("executionTime", response.getExecutionTimeMs());
				resultSet.add(new ResultInternal(result));
			}
			return resultSet;
		}
		catch (StatusRuntimeException e) {
			handleGrpcException(e);
			return new InternalResultSet();
		}
	}

	public com.arcadedb.server.grpc.ExecuteCommandResponse execSql(String db, String sql, Map<String,Object> params, long timeoutMs) {
	    return executeCommand(db, sql, params, "sql", /*returnRows*/ false, /*maxRows*/ 0, txBeginCommit(), timeoutMs);
	}
	
	public com.arcadedb.server.grpc.ExecuteCommandResponse executeCommand(
	        String database,
	        String command,
	        Map<String, Object> params,
	        String language,
	        boolean returnRows,
	        int maxRows,
	        com.arcadedb.server.grpc.TransactionContext tx,
	        long timeoutMs) {

	    var reqB = com.arcadedb.server.grpc.ExecuteCommandRequest.newBuilder()
	        .setDatabase(database)
	        .setCommand(command)
	        .putAllParameters(convertParamsToProto(params))
	        .setLanguage(langOrDefault(language))
	        .setReturnRows(returnRows)
	        .setMaxRows(maxRows > 0 ? maxRows : 0);

	    if (tx != null) reqB.setTransaction(tx);
	    // credentials: if your stub builds creds implicitly, set here if required
	    reqB.setCredentials(buildCredentials());

	    return blockingStub
	        .withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
	        .executeCommand(reqB.build());
	}
	
	@Override
	protected RID saveRecord(final MutableDocument record) {
		stats.createRecord.incrementAndGet();

		RID rid = record.getIdentity();
		if (rid != null) {
			// Update existing record
			UpdateRecordRequest request = UpdateRecordRequest.newBuilder().setDatabase(getName()).setRid(rid.toString())
					.putAllProperties(convertParamsToProto(record.toMap(false))).setCredentials(buildCredentials()).build();

			try {
				UpdateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
						.updateRecord(request);
				
				if (!response.getSuccess()) {
					throw new DatabaseOperationException("Failed to update record");
				}
				return rid;
			}
			catch (StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			}
		}
		else {
			// Create new record
			CreateRecordRequest request = CreateRecordRequest.newBuilder().setDatabase(getName()).setType(record.getTypeName())
					.putAllProperties(convertParamsToProto(record.toMap(false))).setCredentials(buildCredentials()).build();

			try {
				CreateRecordResponse response = blockingStub
						.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
						.createRecord(request);
				
				if (!response.getSuccess()) {
					throw new DatabaseOperationException("Failed to create record");
				}
				return new RID(this, response.getRecord().getRid());
			}
			catch (StatusRuntimeException e) {
				handleGrpcException(e);
				return null;
			}
		}
	}
	
//	/**
//	 * Executes a streaming query that returns results in batches.
//	 * Useful for processing large result sets without loading everything into memory.
//	 * 
//	 * @param language the query language (typically "sql")
//	 * @param query the query string
//	 * @param batchSize the number of records per batch (default 100 if <= 0)
//	 * @return Iterator of records that fetches results in batches
//	 */
//	public Iterator<Record> queryStream(final String language, final String query, final int batchSize) {
//	    checkDatabaseIsOpen();
//	    stats.queries.incrementAndGet();
//	    
//	    StreamQueryRequest request = StreamQueryRequest.newBuilder()
//	        .setDatabase(getName())
//	        .setQuery(query)
//	        .setCredentials(buildCredentials())
//	        .setBatchSize(batchSize > 0 ? batchSize : 100)
//	        .build();
//
//	    Iterator<QueryResult> responseIterator = blockingStub
//	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
//	        .streamQuery(request);
//
//	    return new Iterator<Record>() {
//	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();
//
//	        @Override
//	        public boolean hasNext() {
//	            if (currentBatch.hasNext()) {
//	                return true;
//	            }
//	            if (responseIterator.hasNext()) {
//	                QueryResult result = responseIterator.next();
//	                currentBatch = result.getRecordsList().iterator();
//	                return currentBatch.hasNext();
//	            }
//	            return false;
//	        }
//
//	        @Override
//	        public Record next() {
//	            if (!hasNext()) {
//	                throw new NoSuchElementException();
//	            }
//	            return grpcRecordToRecord(currentBatch.next());
//	        }
//	    };
//	}
//
//	/**
//	 * Executes a streaming query with parameters.
//	 */
//	public Iterator<Record> queryStream(final String language, final String query, 
//	                                   final Map<String, Object> params, final int batchSize) {
//	    checkDatabaseIsOpen();
//	    stats.queries.incrementAndGet();
//	    
//	    StreamQueryRequest.Builder requestBuilder = StreamQueryRequest.newBuilder()
//	        .setDatabase(getName())
//	        .setQuery(query)
//	        .setCredentials(buildCredentials())
//	        .setBatchSize(batchSize > 0 ? batchSize : 100);
//	    
//	    if (params != null && !params.isEmpty()) {
//	        requestBuilder.putAllParameters(convertParamsToProto(params));
//	    }
//
//	    Iterator<QueryResult> responseIterator = blockingStub
//	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
//	        .streamQuery(requestBuilder.build());
//
//	    // Same iterator implementation as above
//	    return new Iterator<Record>() {
//	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();
//
//	        @Override
//	        public boolean hasNext() {
//	            if (currentBatch.hasNext()) {
//	                return true;
//	            }
//	            if (responseIterator.hasNext()) {
//	                QueryResult result = responseIterator.next();
//	                currentBatch = result.getRecordsList().iterator();
//	                return currentBatch.hasNext();
//	            }
//	            return false;
//	        }
//
//	        @Override
//	        public Record next() {
//	            if (!hasNext()) {
//	                throw new NoSuchElementException();
//	            }
//	            return grpcRecordToRecord(currentBatch.next());
//	        }
//	    };
//	}
//
//	/**
//	 * Convenience method with default batch size
//	 */
//	public Iterator<Record> queryStream(final String language, final String query) {
//	    return queryStream(language, query, 100);
//	}
	
	/*
StreamQueryRequest req = StreamQueryRequest.newBuilder()
    .setDatabase(db)
    .setQuery(sql)
    .setBatchSize(500)
    .setRetrievalMode(StreamQueryRequest.RetrievalMode.PAGED)
    .setTransaction(TransactionContext.newBuilder()
        .setBegin(true)
        .setCommit(true)   // or setRollback(true)
        .build())
    .build(); 
	 */
	
	// Convenience: default batch size stays 100, default mode = CURSOR
	public Iterator<Record> queryStream(final String language,
	                                    final String query) {
	    return queryStream(language, query, /*batchSize*/100,
	            StreamQueryRequest.RetrievalMode.CURSOR);
	}

	public Iterator<Record> queryStream(final String language,
	                                    final String query,
	                                    final int batchSize) {
	    return queryStream(language, query, batchSize,
	            StreamQueryRequest.RetrievalMode.CURSOR);
	}

	// NEW: choose retrieval mode
	public Iterator<Record> queryStream(final String language,
	                                    final String query,
	                                    final int batchSize,
	                                    final StreamQueryRequest.RetrievalMode mode) {
	    checkDatabaseIsOpen();
	    stats.queries.incrementAndGet();

	    StreamQueryRequest request = StreamQueryRequest.newBuilder()
	        .setDatabase(getName())
	        .setQuery(query)
	        .setCredentials(buildCredentials())
	        .setBatchSize(batchSize > 0 ? batchSize : 100)
	        .setRetrievalMode(mode) // <--- NEW
	        .build();

	    Iterator<QueryResult> responseIterator = blockingStub
	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
	        .streamQuery(request);

	    return new Iterator<Record>() {
	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

	        @Override
	        public boolean hasNext() {
	            if (currentBatch.hasNext()) return true;

	            while (responseIterator.hasNext()) {
	                QueryResult result = responseIterator.next();

	                // Defensive: skip any empty batches the server might send
	                if (result.getRecordsCount() == 0) {
	                    if (result.getIsLastBatch()) return false;
	                    continue;
	                }

	                currentBatch = result.getRecordsList().iterator();
	                return true;
	            }
	            return false;
	        }

	        @Override
	        public Record next() {
	            if (!hasNext()) throw new NoSuchElementException();
	            return grpcRecordToRecord(currentBatch.next());
	        }
	    };
	}

	// PARAMETERIZED variant with retrieval mode
	public Iterator<Record> queryStream(final String language,
	                                    final String query,
	                                    final Map<String, Object> params,
	                                    final int batchSize,
	                                    final StreamQueryRequest.RetrievalMode mode) {
	    checkDatabaseIsOpen();
	    stats.queries.incrementAndGet();

	    StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder()
	        .setDatabase(getName())
	        .setQuery(query)
	        .setCredentials(buildCredentials())
	        .setBatchSize(batchSize > 0 ? batchSize : 100)
	        .setRetrievalMode(mode); // <--- NEW

	    if (params != null && !params.isEmpty()) {
	        b.putAllParameters(convertParamsToProto(params));
	    }

	    Iterator<QueryResult> responseIterator = blockingStub
	        .withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
	        .streamQuery(b.build());

	    return new Iterator<Record>() {
	        private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

	        @Override
	        public boolean hasNext() {
	            if (currentBatch.hasNext()) return true;

	            while (responseIterator.hasNext()) {
	                QueryResult result = responseIterator.next();

	                if (result.getRecordsCount() == 0) {
	                    if (result.getIsLastBatch()) return false;
	                    continue;
	                }

	                currentBatch = result.getRecordsList().iterator();
	                return true;
	            }
	            return false;
	        }

	        @Override
	        public Record next() {
	            if (!hasNext()) throw new NoSuchElementException();
	            return grpcRecordToRecord(currentBatch.next());
	        }
	    };
	}

	// Keep the old signature working (defaults to CURSOR)
	public Iterator<Record> queryStream(final String language,
	                                    final String query,
	                                    final Map<String, Object> params,
	                                    final int batchSize) {
	    return queryStream(language, query, params, batchSize,
	            StreamQueryRequest.RetrievalMode.CURSOR);
	}
	
	public static final class QueryBatch {
	    private final List<Record> records;
	    private final int totalInBatch;
	    private final long runningTotal;
	    private final boolean lastBatch;

	    public QueryBatch(List<Record> list, int totalInBatch, long runningTotal, boolean lastBatch) {
	        this.records = list;
	        this.totalInBatch = totalInBatch;
	        this.runningTotal = runningTotal;
	        this.lastBatch = lastBatch;
	    }
	    public List<Record> records() { return records; }
	    public int totalInBatch() { return totalInBatch; }
	    public long runningTotal() { return runningTotal; }
	    public boolean isLastBatch() { return lastBatch; }
	}

	public Iterator<QueryBatch> queryStreamBatches(final String language,
	                                               final String query,
	                                               final Map<String, Object> params,
	                                               final int batchSize,
	                                               final StreamQueryRequest.RetrievalMode mode) {
	    checkDatabaseIsOpen();
	    stats.queries.incrementAndGet();

	    StreamQueryRequest.Builder b = StreamQueryRequest.newBuilder()
	        .setDatabase(getName())
	        .setQuery(query)
	        .setCredentials(buildCredentials())
	        .setBatchSize(batchSize > 0 ? batchSize : 100)
	        .setRetrievalMode(mode);

	    if (params != null && !params.isEmpty()) {
	        b.putAllParameters(convertParamsToProto(params));
	    }

	    Iterator<QueryResult> responseIterator = blockingStub
	        .withWaitForReady() // optional, improves robustness
	        .withDeadlineAfter(/* e.g. */ 10, TimeUnit.MINUTES)
	        .streamQuery(b.build());

	    return new Iterator<QueryBatch>() {
	        private QueryBatch nextBatch = null;
	        private boolean drained = false;

	        @Override
	        public boolean hasNext() {
	            if (nextBatch != null) return true;
	            if (drained) return false;

	            while (responseIterator.hasNext()) {
	                QueryResult qr = responseIterator.next();
	                int n = qr.getTotalRecordsInBatch(); // server-populated
	                // Guard: some servers could omit this; fallback to list size.
	                if (n == 0) n = qr.getRecordsCount();

	                if (qr.getRecordsCount() == 0 && !qr.getIsLastBatch()) {
	                    // skip empty non-terminal batch
	                    continue;
	                }

	                List<Record> converted = new ArrayList<>(qr.getRecordsCount());
	                for (com.arcadedb.server.grpc.Record gr : qr.getRecordsList()) {
	                    converted.add(grpcRecordToRecord(gr));
	                }

	                nextBatch = new QueryBatch(
	                    converted,
	                    n,
	                    qr.getRunningTotalEmitted(),
	                    qr.getIsLastBatch()
	                );

	                if (qr.getIsLastBatch()) {
	                    drained = true; // no more after this (even if server sent empty terminal)
	                }
	                return true;
	            }

	            drained = true;
	            return false;
	        }

	        @Override
	        public QueryBatch next() {
	            if (!hasNext()) throw new NoSuchElementException();
	            QueryBatch out = nextBatch;
	            nextBatch = null;
	            return out;
	        }
	    };
	}
	
	public Iterator<com.arcadedb.server.grpc.Record> queryStream(
	        String database,
	        String sql,
	        Map<String, Object> params,
	        int batchSize,
	        com.arcadedb.server.grpc.StreamQueryRequest.RetrievalMode mode,
	        com.arcadedb.server.grpc.TransactionContext tx,
	        long timeoutMs) {

	    var reqB = com.arcadedb.server.grpc.StreamQueryRequest.newBuilder()
	        .setDatabase(database)
	        .setQuery(sql)
	        .putAllParameters(convertParamsToProto(params))
	        .setCredentials(buildCredentials())
	        .setBatchSize(batchSize > 0 ? batchSize : 100)
	        .setRetrievalMode(mode);

	    if (tx != null) reqB.setTransaction(tx);

	    Iterator<com.arcadedb.server.grpc.QueryResult> it = blockingStub
	        .withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
	        .streamQuery(reqB.build());

	    return new Iterator<>() {
	        private Iterator<com.arcadedb.server.grpc.Record> curr = java.util.Collections.emptyIterator();
	        public boolean hasNext() {
	            if (curr.hasNext()) return true;
	            if (it.hasNext()) { curr = it.next().getRecordsList().iterator(); return hasNext(); }
	            return false;
	        }
	        public com.arcadedb.server.grpc.Record next() {
	            if (!hasNext()) throw new java.util.NoSuchElementException();
	            return curr.next();
	        }
	    };
	}	
	
	public Iterator<QueryBatch> queryStreamBatches(
	        String passLabel,
	        String sql,
	        Map<String,Object> params,
	        int batchSize,
	        com.arcadedb.server.grpc.StreamQueryRequest.RetrievalMode mode,
	        com.arcadedb.server.grpc.TransactionContext tx,
	        long timeoutMs) {

	    var reqB = com.arcadedb.server.grpc.StreamQueryRequest.newBuilder()
	        .setDatabase(getName())
	        .setQuery(sql)
	        .putAllParameters(convertParamsToProto(params))
	        .setCredentials(buildCredentials())
	        .setBatchSize(batchSize > 0 ? batchSize : 100)
	        .setRetrievalMode(mode);

	    if (tx != null) reqB.setTransaction(tx);

	    Iterator<com.arcadedb.server.grpc.QueryResult> respIter = blockingStub
	        .withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
	        .streamQuery(reqB.build());

	    return new Iterator<>() {
	        public boolean hasNext() { return respIter.hasNext(); }
	        public QueryBatch next() {
	            
	        	var qr = respIter.next();
	        	
	        	
                List<Record> converted = new ArrayList<>(qr.getRecordsCount());
                for (com.arcadedb.server.grpc.Record gr : qr.getRecordsList()) {
                    converted.add(grpcRecordToRecord(gr));
                }
	            
	            return new QueryBatch(
	            		converted,
	            	    qr.getTotalRecordsInBatch(),             // int totalInBatch
	            	    qr.getRunningTotalEmitted(),             // long runningTotal
	            	    qr.getIsLastBatch()                      // boolean lastBatch
	            	);	            
	        }
	    };
	}	
	
	// 	    public QueryBatch(List<Record> records, int totalInBatch, long runningTotal, boolean lastBatch) {

	public String createVertex(String cls, Map<String,Object> props, long timeoutMs) {
	    var req = com.arcadedb.server.grpc.CreateRecordRequest.newBuilder()
	        .setDatabase(getName())
	        .setType(cls)
	        .putAllProperties(convertParamsToProto(props))
	        .setCredentials(buildCredentials())
	        .build();
	    var res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).createRecord(req);
	    // adjust to your response field name
	    return res.getIdentity(); // e.g., "#12:0"
	}

	public boolean updateRecord(String rid, Map<String,Object> props, long timeoutMs) {
	    var req = com.arcadedb.server.grpc.UpdateRecordRequest.newBuilder()
	        .setDatabase(getName())
	        .setRid(rid)
	        .putAllProperties(convertParamsToProto(props))
	        .setCredentials(buildCredentials())
	        // .setTransaction(txBeginCommit()) // if you add tx to this RPC
	        .build();
	    var res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).updateRecord(req);
	    return res.getSuccess();
	}

	public boolean deleteRecord(String rid, long timeoutMs) {
	    var req = com.arcadedb.server.grpc.DeleteRecordRequest.newBuilder()
	        .setDatabase(getName())
	        .setRid(rid)
	        .setCredentials(buildCredentials())
	        .build();
	    var res = blockingStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS).deleteRecord(req);
	    return res.getDeleted();
	}	
	
	@Override
	public RemoteTransactionExplicitLock acquireLock() {
	    // Need gRPC-specific implementation
	    if (explicitLock == null)
	        explicitLock = new RemoteGrpcTransactionExplicitLock(this);
	    
	    return explicitLock;
	}	
	
	@Override
	public long countBucket(final String bucketName) {
	    checkDatabaseIsOpen();
	    stats.countBucket.incrementAndGet();
	    ResultSet result = query("sql", "select count(*) as count from bucket:" + bucketName);
	    if (result.hasNext()) {
	        Number count = result.next().getProperty("count");
	        return count != null ? count.longValue() : 0;
	    }
	    return 0;
	}
	
	@Override
	public long countType(final String typeName, final boolean polymorphic) {
	    checkDatabaseIsOpen();
	    stats.countType.incrementAndGet();
	    final String appendix = polymorphic ? "" : " where @type = '" + typeName + "'";
	    ResultSet result = query("sql", "select count(*) as count from " + typeName + appendix);
	    if (result.hasNext()) {
	        Number count = result.next().getProperty("count");
	        return count != null ? count.longValue() : 0;
	    }
	    return 0;
	}
	
	@Override
	public Record lookupByRID(final RID rid) {
	    return lookupByRID(rid, true);
	}

	@Override
	public boolean existsRecord(RID rid) {
	    stats.existsRecord.incrementAndGet();
	    if (rid == null)
	        throw new IllegalArgumentException("Record is null");
	    try {
	        return lookupByRID(rid, false) != null;
	    } catch (RecordNotFoundException e) {
	        return false;
	    }
	}
	
		
	public com.arcadedb.server.grpc.InsertSummary insertBulk(
	        final com.arcadedb.server.grpc.InsertOptions options,
	        final java.util.List<com.arcadedb.server.grpc.Record> protoRows,
	        final long timeoutMs) {

	    // Ensure options carry DB + credentials as the server expects
	    com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();

	    if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
	        ob.setDatabase(getName());                 // your wrapper's DB name
	    }
	    
	    boolean missingCreds =
	            !options.hasCredentials()
	            || options.getCredentials().getUsername().isEmpty();
	    if (missingCreds) {
	        ob.setCredentials(buildCredentials());     // your existing helper used by queries
	    }
	    
	    InsertOptions newOptions = ob.build();
	    
	    com.arcadedb.server.grpc.BulkInsertRequest req =
	        com.arcadedb.server.grpc.BulkInsertRequest.newBuilder()
	            .setOptions(newOptions)
	            .addAllRows(protoRows)
	            .build();

	    return blockingStub
	        .withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
	        .bulkInsert(req);
	}

	// Convenience overload that accepts domain rows (convert first)
	public com.arcadedb.server.grpc.InsertSummary insertBulkAsListOfMaps(
	        final com.arcadedb.server.grpc.InsertOptions options,
	        final java.util.List<java.util.Map<String, Object>> rows,
	        final long timeoutMs) {

	    java.util.List<com.arcadedb.server.grpc.Record> protoRows =
	        rows.stream()
	            .map(this::toProtoRecordFromMap) // your converter
	            .collect(java.util.stream.Collectors.toList());

	    return insertBulk(options, protoRows, timeoutMs);
	}
	
	public com.arcadedb.server.grpc.InsertSummary ingestStream(
	        final com.arcadedb.server.grpc.InsertOptions options,             // note: InsertStream RPC does not carry options
	        final java.util.List<com.arcadedb.server.grpc.Record> protoRows,
	        final int chunkSize,
	        final long timeoutMs) throws InterruptedException {

	    final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
	    final java.util.concurrent.atomic.AtomicReference<com.arcadedb.server.grpc.InsertSummary> summaryRef =
	            new java.util.concurrent.atomic.AtomicReference<>();
	    
	    // Ensure options carry DB + credentials as the server expects
	    com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();

	    if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
	        ob.setDatabase(getName());                 // your wrapper's DB name
	    }
	    
	    boolean missingCreds =
	            !options.hasCredentials()
	            || options.getCredentials().getUsername().isEmpty();
	    if (missingCreds) {
	        ob.setCredentials(buildCredentials());     // your existing helper used by queries
	    }	    

	    io.grpc.stub.StreamObserver<com.arcadedb.server.grpc.InsertSummary> resp =
	            new io.grpc.stub.StreamObserver<>() {
	                @Override public void onNext(com.arcadedb.server.grpc.InsertSummary value) { summaryRef.set(value); }
	                @Override public void onError(Throwable t) { done.countDown(); }
	                @Override public void onCompleted() { done.countDown(); }
	            };

	    // Use the write service async stub, per-call deadline
	    var stub =
	            this.asyncStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

	    final io.grpc.stub.StreamObserver<com.arcadedb.server.grpc.InsertChunk> req = stub.insertStream(resp);

	    // Stream chunks
	    final String sessionId = "sess-" + System.nanoTime();
	    long seq = 1;
	    for (int i = 0; i < protoRows.size(); i += chunkSize) {
	        final int end = Math.min(i + chunkSize, protoRows.size());
	        final com.arcadedb.server.grpc.InsertChunk chunk =
	                com.arcadedb.server.grpc.InsertChunk.newBuilder()
	                        .setSessionId(sessionId)
	                        .setChunkSeq(seq++)
	                        .addAllRows(protoRows.subList(i, end))
	                        .build();
	        req.onNext(chunk);
	    }
	    req.onCompleted();

	    done.await(timeoutMs + 5_000, java.util.concurrent.TimeUnit.MILLISECONDS);

	    final com.arcadedb.server.grpc.InsertSummary s = summaryRef.get();
	    return (s != null)
	            ? s
	            : com.arcadedb.server.grpc.InsertSummary.newBuilder().setReceived(protoRows.size()).build();
	}
	
	// Convenience overload
	public com.arcadedb.server.grpc.InsertSummary ingestStreamAsListOfMaps(
	        final com.arcadedb.server.grpc.InsertOptions options,
	        final java.util.List<java.util.Map<String,Object>> rows,
	        final int chunkSize,
	        final long timeoutMs) throws InterruptedException {

	    java.util.List<com.arcadedb.server.grpc.Record> protoRows =
	        rows.stream().map(this::toProtoRecordFromMap).collect(java.util.stream.Collectors.toList());
	    return ingestStream(options, protoRows, chunkSize, timeoutMs);
	}
	
	/** Push records using InsertBidirectional with per-batch ACKs. */
	public InsertSummary ingestBidi(List<Record> rows, InsertOptions opts, int chunkSize, int maxInflight) throws InterruptedException {
		
		final String sessionId = UUID.randomUUID().toString();
		
	    // Ensure options carry DB + credentials as the server expects
	    com.arcadedb.server.grpc.InsertOptions.Builder ob = opts.toBuilder();

	    if (opts.getDatabase() == null || opts.getDatabase().isEmpty()) {
	        ob.setDatabase(getName());                 // your wrapper's DB name
	    }
	    
	    boolean missingCreds =
	            !opts.hasCredentials()
	            || opts.getCredentials().getUsername().isEmpty();
	    if (missingCreds) {
	        ob.setCredentials(buildCredentials());     // your existing helper used by queries
	    }
		
		final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong(1);
		final java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.concurrent.atomic.AtomicInteger acked = new java.util.concurrent.atomic.AtomicInteger(0);
		final java.util.List<BatchAck> acks = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
		final java.util.concurrent.atomic.AtomicReference<InsertSummary> committed = new java.util.concurrent.atomic.AtomicReference<>();

		io.grpc.stub.ClientResponseObserver<InsertRequest, InsertResponse> obs = new io.grpc.stub.ClientResponseObserver<>() {
			
			io.grpc.stub.ClientCallStreamObserver<InsertRequest> req;

			@Override
			public void beforeStart(io.grpc.stub.ClientCallStreamObserver<InsertRequest> r) {
				this.req = r;
				r.disableAutoInboundFlowControl();
				r.setOnReadyHandler(this::drain);
			}

			private int cursor = 0;

			private void drain() {
				if (!req.isReady())
					return;
				if (seq.get() == 1) { // send START once
					req.onNext(InsertRequest.newBuilder().setStart(StartInsert.newBuilder().setOptions(ob.build())).build());
				}
				while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
					
					if (cursor >= rows.size())
						break;
					
					int end = Math.min(cursor + chunkSize, rows.size());
							
					List<com.arcadedb.server.grpc.Record> protoRows = rows.stream()
						    .map(RemoteGrpcDatabase.this::toProtoRecordFromDbRecord)
						    .toList();

						InsertChunk chunk = InsertChunk.newBuilder()
						    .setSessionId(sessionId)
						    .setChunkSeq(seq.getAndIncrement())
						    .addAllRows(protoRows)   
						    .build();					
					
					req.onNext(InsertRequest.newBuilder().setChunk(chunk).build());
					cursor = end;
					sent.incrementAndGet();
				}
				req.request(1);
				if (cursor >= rows.size() && acked.get() >= sent.get()) {
					req.onNext(InsertRequest.newBuilder().setCommit(CommitInsert.newBuilder().setSessionId(sessionId)).build());
					req.onCompleted();
				}
			}

			@Override
			public void onNext(InsertResponse v) {
				switch (v.getMsgCase()) {
				case STARTED -> {
					/* ok */ }
				case BATCH_ACK -> {
					acks.add(v.getBatchAck());
					acked.incrementAndGet();
					drain();
				}
				case COMMITTED -> committed.set(v.getCommitted().getSummary());
				case MSG_NOT_SET -> {
				}
				}
				req.request(1);
			}

			@Override
			public void onError(Throwable t) {
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
		};

		asyncStub.insertBidirectional(obs);
		
		done.await(5, java.util.concurrent.TimeUnit.MINUTES);

		InsertSummary s = committed.get();
		if (s != null)
			return s;

		long ins = acks.stream().mapToLong(BatchAck::getInserted).sum();
		long upd = acks.stream().mapToLong(BatchAck::getUpdated).sum();
		long ign = acks.stream().mapToLong(BatchAck::getIgnored).sum();
		long fail = acks.stream().mapToLong(BatchAck::getFailed).sum();
		
		return InsertSummary.newBuilder().setReceived(rows.size()).setInserted(ins).setUpdated(upd).setIgnored(ign).setFailed(fail).build();
	}	
	
	/**
	 * Bidirectional streaming ingest with per-batch ACKs.
	 *
	 * @param options InsertOptions (database, target class, conflict mode, tx mode, etc.)
	 * @param rowsProto list of PROTO Records (com.arcadedb.server.grpc.Record)
	 * @param chunkSize rows per chunk
	 * @param maxInflight max unacked chunks
	 * @param timeoutMs overall timeout for the ingest (client-side)
	 */
	public com.arcadedb.server.grpc.InsertSummary ingestBidi(
	        final com.arcadedb.server.grpc.InsertOptions options,
	        final java.util.List<java.util.Map<String, Object>> rows,
	        final int chunkSize,
	        final int maxInflight,
	        final long timeoutMs) throws InterruptedException {
		
	    // Ensure options carry DB + credentials as the server expects
	    com.arcadedb.server.grpc.InsertOptions.Builder ob = options.toBuilder();

	    if (options.getDatabase() == null || options.getDatabase().isEmpty()) {
	        ob.setDatabase(getName());                 // your wrapper's DB name
	    }
	    
	    boolean missingCreds =
	            !options.hasCredentials()
	            || options.getCredentials().getUsername().isEmpty();
	    if (missingCreds) {
	        ob.setCredentials(buildCredentials());     // your existing helper used by queries
	    }
		
	    // Pre-convert to PROTO records (outside the observer!)
	    final java.util.List<com.arcadedb.server.grpc.Record> protoRows =
	            rows.stream()
	                .map(RemoteGrpcDatabase.this::toProtoRecordFromMap)
	                .collect(java.util.stream.Collectors.toList());

	    final String sessionId = "sess-" + System.nanoTime();
	    final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
	    final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong(1);
	    final java.util.concurrent.atomic.AtomicInteger cursor = new java.util.concurrent.atomic.AtomicInteger(0);
	    final java.util.concurrent.atomic.AtomicInteger sent = new java.util.concurrent.atomic.AtomicInteger(0);
	    final java.util.concurrent.atomic.AtomicInteger acked = new java.util.concurrent.atomic.AtomicInteger(0);
	    final java.util.List<com.arcadedb.server.grpc.BatchAck> acks =
	            java.util.Collections.synchronizedList(new java.util.ArrayList<>());
	    final java.util.concurrent.atomic.AtomicReference<com.arcadedb.server.grpc.InsertSummary> committed =
	            new java.util.concurrent.atomic.AtomicReference<>();

	    io.grpc.stub.ClientResponseObserver<com.arcadedb.server.grpc.InsertRequest,
	                                        com.arcadedb.server.grpc.InsertResponse> observer =
	            new io.grpc.stub.ClientResponseObserver<>() {
	                io.grpc.stub.ClientCallStreamObserver<com.arcadedb.server.grpc.InsertRequest> req;

	                @Override
	                public void beforeStart(io.grpc.stub.ClientCallStreamObserver<com.arcadedb.server.grpc.InsertRequest> r) {
	                    this.req = r;
	                    r.disableAutoInboundFlowControl();
	                    r.setOnReadyHandler(this::drain);
	                }

	                private void drain() {
	                    if (!req.isReady()) return;

	                    if (seq.get() == 1) {
	                        req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder()
	                                .setStart(com.arcadedb.server.grpc.StartInsert.newBuilder().setOptions(ob.build()))
	                                .build());
	                    }

	                    while (req.isReady() && (sent.get() - acked.get()) < maxInflight) {
	                        int start = cursor.get();
	                        if (start >= protoRows.size()) break;
	                        int end = Math.min(start + chunkSize, protoRows.size());

	                        com.arcadedb.server.grpc.InsertChunk chunk =
	                                com.arcadedb.server.grpc.InsertChunk.newBuilder()
	                                        .setSessionId(sessionId)
	                                        .setChunkSeq(seq.getAndIncrement())
	                                        .addAllRows(protoRows.subList(start, end))
	                                        .build();

	                        req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder().setChunk(chunk).build());
	                        cursor.set(end);
	                        sent.incrementAndGet();
	                    }

	                    req.request(1);

	                    if (cursor.get() >= protoRows.size() && acked.get() >= sent.get()) {
	                        req.onNext(com.arcadedb.server.grpc.InsertRequest.newBuilder()
	                                .setCommit(com.arcadedb.server.grpc.CommitInsert.newBuilder().setSessionId(sessionId))
	                                .build());
	                        req.onCompleted();
	                    }
	                }

	                @Override
	                public void onNext(com.arcadedb.server.grpc.InsertResponse v) {
	                    switch (v.getMsgCase()) {
	                        case STARTED -> { /* ok */ }
	                        case BATCH_ACK -> {
	                            acks.add(v.getBatchAck());
	                            acked.incrementAndGet();
	                            drain();
	                        }
	                        case COMMITTED -> committed.set(v.getCommitted().getSummary());
	                        case MSG_NOT_SET -> {}
	                    }
	                    req.request(1);
	                }

	                @Override public void onError(Throwable t) { done.countDown(); }
	                @Override public void onCompleted() { done.countDown(); }
	            };

	    var stub = this.asyncStub.withDeadlineAfter(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
	    stub.insertBidirectional(observer);

	    done.await(timeoutMs + 5_000, java.util.concurrent.TimeUnit.MILLISECONDS);

	    var summary = committed.get();
	    if (summary != null) return summary;

	    long ins = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getInserted).sum();
	    long upd = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getUpdated).sum();
	    long ign = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getIgnored).sum();
	    long fail = acks.stream().mapToLong(com.arcadedb.server.grpc.BatchAck::getFailed).sum();

	    return com.arcadedb.server.grpc.InsertSummary.newBuilder()
	            .setReceived(protoRows.size())
	            .setInserted(ins)
	            .setUpdated(upd)
	            .setIgnored(ign)
	            .setFailed(fail)
	            .build();
	}

	// Map -> PROTO Value
	private com.arcadedb.server.grpc.Record toProtoRecord(Map<String, Object> row) {
	    com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
	    // Example if your proto uses a map<string, google.protobuf.Value> fields/properties
	    row.forEach((k, v) -> b.putProperties(k, toProtoValue(v)));
	    return b.build();
	}

	// Map -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromMap(java.util.Map<String, Object> row) {
	    com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
	    row.forEach((k, v) -> b.putProperties(k, toProtoValue(v)));
	    return b.build();
	}
	
	// Domain Record (storage) -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromDbRecord(
	        com.arcadedb.database.Record rec) {

	    // Promote to a read-only Document; null if this record isn't a document
	    com.arcadedb.database.Document doc = rec.asDocument();
	    if (doc == null) {
	        return com.arcadedb.server.grpc.Record.getDefaultInstance();
	    }

	    com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();

	    // Iterate document properties
	    for (String name : doc.getPropertyNames()) {
	        // Depending on your ArcadeDB version, use ONE of these:
	        Object v = doc.get(name);           // <-- common in ArcadeDB
	        // Object v = doc.getProperty(name); // <-- use this if doc.get(...) doesn't exist in your API
	        b.putProperties(name, toProtoValue(v));  // or putFields(...) if your proto uses "fields"
	    }
	    return b.build();
	}	
	

	// google.protobuf.Value -> Java (mirror of your toProtoValue)
	private static Object fromProtoValue(com.google.protobuf.Value v) {
	  if (v == null) return null;
	  switch (v.getKindCase()) {
	    case NULL_VALUE:   return null;
	    case BOOL_VALUE:   return v.getBoolValue();
	    case NUMBER_VALUE: return v.getNumberValue();
	    case STRING_VALUE: return v.getStringValue();
	    case LIST_VALUE: {
	      java.util.List<Object> out = new java.util.ArrayList<>();
	      for (com.google.protobuf.Value item : v.getListValue().getValuesList()) {
	        out.add(fromProtoValue(item));
	      }
	      return out;
	    }
	    case STRUCT_VALUE: {
	      java.util.Map<String,Object> m = new java.util.LinkedHashMap<>();
	      v.getStructValue().getFieldsMap().forEach((k,vv) -> m.put(k, fromProtoValue(vv)));
	      return m; // server-side schema can later coerce EMBEDDED if needed
	    }
	    case KIND_NOT_SET:
	    default:
	      return null;
	  }
	}
	
	// Query Result -> PROTO Record
	private com.arcadedb.server.grpc.Record toProtoRecordFromResult(Result res) {
	    com.arcadedb.server.grpc.Record.Builder b = com.arcadedb.server.grpc.Record.newBuilder();
	    for (String name : res.getPropertyNames()) {
	        Object v = res.getProperty(name);
	        b.putProperties(name, toProtoValue(v));   // or putFields(...)
	    }
	    return b.build();
	}	

	// Convert Java object -> protobuf Value (extend as needed)
	private com.google.protobuf.Value toProtoValue(Object v) {
	    com.google.protobuf.Value.Builder vb = com.google.protobuf.Value.newBuilder();
	    if (v == null) return vb.setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
	    if (v instanceof String s)  return vb.setStringValue(s).build();
	    if (v instanceof Boolean b) return vb.setBoolValue(b).build();
	    if (v instanceof Number n)  return vb.setNumberValue(n.doubleValue()).build();
	    if (v instanceof java.time.Instant t)
	        return vb.setStringValue(t.toString()).build(); // or a Struct for Timestamp if you prefer
	    if (v instanceof java.util.Map<?,?> m) {
	        var sb = com.google.protobuf.Struct.newBuilder();
	        for (var e : m.entrySet())
	            sb.putFields(String.valueOf(e.getKey()), toProtoValue(e.getValue()));
	        return vb.setStructValue(sb.build()).build();
	    }
	    if (v instanceof java.util.List<?> list) {
	        var lb = com.google.protobuf.ListValue.newBuilder();
	        for (Object e : list) lb.addValues(toProtoValue(e));
	        return vb.setListValue(lb.build()).build();
	    }
	    return vb.setStringValue(String.valueOf(v)).build();
	}	
	
	// Override HTTP-specific methods to prevent usage
//	@Override
//	HttpRequest.Builder createRequestBuilder(String httpMethod, String url) {
//		throw new UnsupportedOperationException("HTTP operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected ResultSet createResultSet(JSONObject response) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected Result json2Result(JSONObject result) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}
//
//	@Override
//	protected Record json2Record(JSONObject result) {
//		throw new UnsupportedOperationException("JSON operations not supported in gRPC implementation");
//	}

	// gRPC-specific helper methods

	private DatabaseCredentials buildCredentials() {
		return DatabaseCredentials.newBuilder().setUsername(getUserName()).setPassword(getUserPassword()).build();
	}

	private TransactionIsolation mapIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL level) {
		switch (level) {
//      case READ_UNCOMMITTED:
//        return TransactionIsolation.READ_UNCOMMITTED;
		case READ_COMMITTED:
			return TransactionIsolation.READ_COMMITTED;
		case REPEATABLE_READ:
			return TransactionIsolation.REPEATABLE_READ;
//      case SERIALIZABLE:
//        return TransactionIsolation.SERIALIZABLE;
		default:
			return TransactionIsolation.READ_COMMITTED;
		}
	}
	
	// ---- TX helpers -------------------------------------------------------------
	private static com.arcadedb.server.grpc.TransactionContext txBeginCommit() {
	    return com.arcadedb.server.grpc.TransactionContext.newBuilder()
	        .setBegin(true).setCommit(true).build();
	}
	private static com.arcadedb.server.grpc.TransactionContext txNone() {
	    return com.arcadedb.server.grpc.TransactionContext.getDefaultInstance();
	}

	// Optional: language defaulting (server defaults to "sql" too)
	private static String langOrDefault(String language) {
	    return (language == null || language.isEmpty()) ? "sql" : language;
	}	

	private Iterator<Record> streamQuery(final String query) {
		StreamQueryRequest request = StreamQueryRequest.newBuilder().setDatabase(getName()).setQuery(query).setCredentials(buildCredentials())
				.setBatchSize(100).build();

		Iterator<QueryResult> responseIterator = blockingStub
				.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS)
				.streamQuery(request);

		return new Iterator<Record>() {
			private Iterator<com.arcadedb.server.grpc.Record> currentBatch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (currentBatch.hasNext()) {
					return true;
				}
				if (responseIterator.hasNext()) {
					QueryResult result = responseIterator.next();
					currentBatch = result.getRecordsList().iterator();
					return currentBatch.hasNext();
				}
				return false;
			}

			@Override
			public Record next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				return grpcRecordToRecord(currentBatch.next());
			}
		};
	}

	private ResultSet createGrpcResultSet(ExecuteQueryResponse response) {
		InternalResultSet resultSet = new InternalResultSet();
		for (QueryResult queryResult : response.getResultsList()) {
			for (com.arcadedb.server.grpc.Record record : queryResult.getRecordsList()) {
				resultSet.add(grpcRecordToResult(record));
			}
		}
		return resultSet;
	}

	private Result grpcRecordToResult(com.arcadedb.server.grpc.Record grpcRecord) {
		Record record = grpcRecordToRecord(grpcRecord);
		if (record == null) {
			Map<String, Object> properties = new HashMap<>();
			grpcRecord.getPropertiesMap().forEach((k, v) -> properties.put(k, valueToObject(v)));
			return new ResultInternal(properties);
		}
		return new ResultInternal(record);
	}

	private Record grpcRecordToRecord(com.arcadedb.server.grpc.Record grpcRecord) {
		Map<String, Object> map = new HashMap<>();

		// Convert properties
		grpcRecord.getPropertiesMap().forEach((k, v) -> map.put(k, valueToObject(v)));

		// Add metadata
		map.put("@rid", grpcRecord.getRid());
		map.put("@type", grpcRecord.getType());
		map.put("@cat", mapRecordType(grpcRecord));

		String cat = (String) map.get("@cat");
		switch (cat) {
		case "d":
			return new RemoteImmutableDocument(this, map);
		case "v":
			return new RemoteImmutableVertex(this, map);
		case "e":
			return new RemoteImmutableEdge(this, map);
		default:
			return null;
		}
	}

	private String mapRecordType(com.arcadedb.server.grpc.Record grpcRecord) {
		// Determine record category from type name
		String typeName = grpcRecord.getType();

		// Check schema to determine actual type
		try {
			if (getSchema().existsType(typeName)) {
				Object type = getSchema().getType(typeName);
				if (type instanceof com.arcadedb.schema.VertexType) {
					return "v";
				}
				else if (type instanceof com.arcadedb.schema.EdgeType) {
					return "e";
				}
			}
		}
		catch (Exception e) {
			// Fall back to name-based detection
		}

		// Fall back to name-based detection
		if (typeName.contains("Vertex") || typeName.startsWith("V_")) {
			return "v";
		}
		else if (typeName.contains("Edge") || typeName.startsWith("E_")) {
			return "e";
		}
		else {
			return "d";
		}
	}

	private Map<String, Value> convertParamsToProto(Map<String, Object> params) {
		Map<String, Value> protoParams = new HashMap<>();
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			protoParams.put(entry.getKey(), objectToValue(entry.getValue()));
		}
		return protoParams;
	}

	private Value objectToValue(Object obj) {
		Value.Builder builder = Value.newBuilder();
		if (obj == null) {
			builder.setNullValue(com.google.protobuf.NullValue.NULL_VALUE);
		}
		else if (obj instanceof Boolean) {
			builder.setBoolValue((Boolean) obj);
		}
		else if (obj instanceof Number) {
			builder.setNumberValue(((Number) obj).doubleValue());
		}
		else if (obj instanceof String) {
			builder.setStringValue((String) obj);
		}
		else if (obj instanceof RID) {
			builder.setStringValue(obj.toString());
		}
		else if (obj instanceof Map) {
			Struct.Builder structBuilder = Struct.newBuilder();
			((Map<?, ?>) obj).forEach((k, v) -> structBuilder.putFields(k.toString(), objectToValue(v)));
			builder.setStructValue(structBuilder.build());
		}
		else if (obj instanceof Collection) {
			com.google.protobuf.ListValue.Builder listBuilder = com.google.protobuf.ListValue.newBuilder();
			((Collection<?>) obj).forEach(item -> listBuilder.addValues(objectToValue(item)));
			builder.setListValue(listBuilder.build());
		}
		else {
			builder.setStringValue(obj.toString());
		}
		return builder.build();
	}

	private Object valueToObject(Value value) {
		switch (value.getKindCase()) {
		case NULL_VALUE:
			return null;
		case NUMBER_VALUE:
			return value.getNumberValue();
		case STRING_VALUE:
			return value.getStringValue();
		case BOOL_VALUE:
			return value.getBoolValue();
		case STRUCT_VALUE:
			Map<String, Object> map = new HashMap<>();
			value.getStructValue().getFieldsMap().forEach((k, v) -> map.put(k, valueToObject(v)));
			return map;
		case LIST_VALUE:
			List<Object> list = new ArrayList<>();
			value.getListValue().getValuesList().forEach(v -> list.add(valueToObject(v)));
			return list;
		default:
			return null;
		}
	}

	private void handleGrpcException(StatusRuntimeException e) {
		Status status = e.getStatus();
		switch (status.getCode()) {
		case NOT_FOUND:
			throw new RecordNotFoundException(status.getDescription(), null);
		case ALREADY_EXISTS:
			throw new DuplicatedKeyException("", "", null);
		case ABORTED:
			throw new ConcurrentModificationException(status.getDescription());
		case DEADLINE_EXCEEDED:
			throw new TimeoutException(status.getDescription());
		case PERMISSION_DENIED:
			throw new SecurityException(status.getDescription());
		case UNAVAILABLE:
			throw new NeedRetryException(status.getDescription());
		default:
			throw new RemoteException("gRPC error: " + status.getDescription(), e);
		}
	}

	/**
	 * Extended version with compression support
	 */
	public static class RemoteGrpcDatabaseWithCompression extends RemoteGrpcDatabase {

		public RemoteGrpcDatabaseWithCompression(String server, int port, String databaseName, String userName, String userPassword,
				ContextConfiguration configuration) {
			super(server, port, databaseName, userName, userPassword, configuration);
		}

		@Override
		protected ManagedChannel createChannel(String server, int port) {

			return ManagedChannelBuilder.forAddress(server, port).usePlaintext() // No TLS/SSL
					.compressorRegistry(CompressorRegistry.getDefaultInstance())
					.decompressorRegistry(DecompressorRegistry.getDefaultInstance())
					.maxInboundMessageSize(100 * 1024 * 1024) // 100MB max message size
					.keepAliveTime(30, TimeUnit.SECONDS) // Keep-alive configuration
					.keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();
		}
	}
}