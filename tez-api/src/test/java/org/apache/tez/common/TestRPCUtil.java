/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezException;
import org.junit.Assert;

import org.apache.hadoop.ipc.RemoteException;
import org.junit.Test;

import com.google.protobuf.ServiceException;

public class TestRPCUtil {

  @Test (timeout=1000)
  public void testUnknownExceptionUnwrapping() {
    Class<? extends Throwable> exception = TezException.class;
    String className = "UnknownException.class";
    verifyRemoteExceptionUnwrapping(exception, className);
  }

  @Test
  public void testRemoteIOExceptionUnwrapping() {
    Class<? extends Throwable> exception = IOException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteIOExceptionDerivativeUnwrapping() {
    // Test IOException sub-class
    Class<? extends Throwable> exception = FileNotFoundException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteTezExceptionUnwrapping() {
    Class<? extends Throwable> exception = TezException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());

  }

  @Test
  public void testRemoteTezExceptionDerivativeUnwrapping() {
    Class<? extends Throwable> exception = SessionNotRunning.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testRemoteRuntimeExceptionUnwrapping() {
    Class<? extends Throwable> exception = NullPointerException.class;
    verifyRemoteExceptionUnwrapping(exception, exception.getName());
  }

  @Test
  public void testUnexpectedRemoteExceptionUnwrapping() {
    // Non IOException, TezException thrown by the remote side.
    Class<? extends Throwable> exception = Exception.class;
    verifyRemoteExceptionUnwrapping(RemoteException.class, exception.getName());
  }

  @Test
  public void testRemoteTezExceptionWithoutStringConstructor() {
    // Derivatives of TezException should always define a string constructor.
    Class<? extends Throwable> exception = TezTestExceptionNoConstructor.class;
    verifyRemoteExceptionUnwrapping(RemoteException.class, exception.getName());
  }

  @Test
  public void testRPCServiceExceptionUnwrapping() {
    String message = "ServiceExceptionMessage";
    ServiceException se = new ServiceException(message);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    } catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue(IOException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  @Test
  public void testRPCIOExceptionUnwrapping() {
    String message = "DirectIOExceptionMessage";
    IOException ioException = new FileNotFoundException(message);
    ServiceException se = new ServiceException(ioException);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    } catch (Throwable thrown) {
      t = thrown;
    }
    Assert.assertTrue(FileNotFoundException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  @Test
  public void testRPCRuntimeExceptionUnwrapping() {
    String message = "RPCRuntimeExceptionUnwrapping";
    RuntimeException re = new NullPointerException(message);
    ServiceException se = new ServiceException(re);

    Throwable t = null;
    try {
      RPCUtil.unwrapAndThrowException(se);
    }  catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue(NullPointerException.class.isInstance(t));
    Assert.assertTrue(t.getMessage().contains(message));
  }

  private void verifyRemoteExceptionUnwrapping(
      Class<? extends Throwable> expectedLocalException,
      String realExceptionClassName) {
    verifyRemoteExceptionUnwrapping(expectedLocalException, realExceptionClassName, true);
  }

  private void verifyRemoteExceptionUnwrapping(
      Class<? extends Throwable> expectedLocalException,
      String realExceptionClassName, boolean allowIO) {
    String message = realExceptionClassName + "Message";
    RemoteException re = new RemoteException(realExceptionClassName, message);
    ServiceException se = new ServiceException(re);

    Throwable t = null;
    try {
      if (allowIO) {
        RPCUtil.unwrapAndThrowException(se);
      } else {
        RPCUtil.unwrapAndThrowNonIOException(se);
      }
    } catch (Throwable thrown) {
      t = thrown;
    }

    Assert.assertTrue("Expected exception [" + expectedLocalException
        + "] but found " + t, expectedLocalException.isInstance(t));
    Assert.assertTrue(
        "Expected message [" + message + "] but found " + t.getMessage(), t
            .getMessage().contains(message));
  }


  @Test (timeout=1000)
  public void testRemoteNonIOExceptionUnwrapping() {
    Class<? extends Throwable> exception = TezException.class;
    verifyRemoteExceptionUnwrapping(exception, IOException.class.getName(), false);
  }


  private static class TezTestExceptionNoConstructor extends
      Exception {
    private static final long serialVersionUID = 1L;
  }

}
