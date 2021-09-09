package com.orientechnologies.orient.distributed.impl.structural.submit;

import com.orientechnologies.orient.distributed.impl.structural.raft.OStructuralSubmit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OStructuralSubmitRequest extends OStructuralSubmit {

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;

  int getRequestType();
}
