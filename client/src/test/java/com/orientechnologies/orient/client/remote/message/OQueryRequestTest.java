package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by luigidellaquila on 14/12/16. */
public class OQueryRequestTest {

  @Before
  public void before() {
    ODatabaseRecordThreadLocal.instance().remove();
  }

  @Test
  public void testWithPositionalParams() throws IOException {
    Object[] params = new Object[] {1, "Foo"};
    OQueryRequest request =
        new OQueryRequest(
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY,
            ORecordSerializerNetworkFactory.INSTANCE.current(),
            123);

    MockChannel channel = new MockChannel();
    request.write(channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());

    Assert.assertFalse(other.isNamedParams());
    Assert.assertArrayEquals(request.getPositionalParameters(), other.getPositionalParameters());

    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }

  @Test
  public void testWithNamedParams() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("foo", "bar");
    params.put("baz", 12);
    OQueryRequest request =
        new OQueryRequest(
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY,
            ORecordSerializerNetworkFactory.INSTANCE.current(),
            123);

    MockChannel channel = new MockChannel();
    request.write(channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());
    Assert.assertTrue(other.isNamedParams());
    Assert.assertEquals(request.getNamedParameters(), other.getNamedParameters());
    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }

  @Test
  public void testWithNoParams() throws IOException {
    Map<String, Object> params = null;
    OQueryRequest request =
        new OQueryRequest(
            "sql",
            "select from Foo where a = ?",
            params,
            OQueryRequest.QUERY,
            ORecordSerializerNetworkFactory.INSTANCE.current(),
            123);

    MockChannel channel = new MockChannel();
    request.write(channel, null);

    channel.close();

    OQueryRequest other = new OQueryRequest();
    other.read(channel, -1, ORecordSerializerNetworkFactory.INSTANCE.current());

    Assert.assertEquals(request.getCommand(), other.getCommand());
    Assert.assertTrue(other.isNamedParams());
    Assert.assertEquals(request.getNamedParameters(), other.getNamedParameters());
    Assert.assertEquals(request.getOperationType(), other.getOperationType());
    Assert.assertEquals(request.getRecordsPerPage(), other.getRecordsPerPage());
  }
}
