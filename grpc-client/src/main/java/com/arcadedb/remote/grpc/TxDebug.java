package com.arcadedb.remote.grpc;

final class TxDebug {
  final    long                                   id          = System.nanoTime(); // local correlation id
  final    Thread                                 ownerThread = Thread.currentThread();
  final    String                                 dbName;
  volatile String                                 txLabel; // optional
  final    Exception                              beginSite   = new Exception("begin site"); // capture stack
  final    java.util.concurrent.atomic.AtomicLong rpcSeq      = new java.util.concurrent.atomic.AtomicLong();
  volatile boolean                                beginRpcSent, committed, rolledBack;

  @SuppressWarnings("unused")
  TxDebug(String db, String label) {
    this.dbName = db;
    this.txLabel = label;
  }
}
