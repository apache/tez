/*
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

package org.apache.tez.dag.app;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PluginWrapperTestHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(PluginWrapperTestHelpers.class);

  public static void testDelegation(Class<?> delegateClass, Class<?> rawClass,
                                    Set<String> skipMethods) throws Exception {
    TrackingAnswer answer = new TrackingAnswer();
    Object mock = mock(rawClass, answer);
    Constructor ctor = delegateClass.getConstructor(rawClass);
    Object wrapper = ctor.newInstance(mock);

    // Run through all the methods on the wrapper, and invoke the methods. Constructs
    // arguments and return types for each of them.
    Method[] methods = delegateClass.getMethods();
    for (Method method : methods) {
      if (method.getDeclaringClass().equals(delegateClass) &&
          !skipMethods.contains(method.getName())) {

        assertTrue(method.getExceptionTypes().length == 1);
        assertEquals(Exception.class, method.getExceptionTypes()[0]);

        LOG.info("Checking method [{}] with parameterTypes [{}]", method.getName(), Arrays.toString(method.getParameterTypes()));

        Object[] params = constructMethodArgs(method);
        Object result = method.invoke(wrapper, params);

        // Validate the correct arguments are forwarded, and the real instance is invoked.
        assertEquals(method.getName(), answer.lastMethodName);
        assertArrayEquals(params, answer.lastArgs);

        // Validate the results.
        // Handle auto-boxing
        if (answer.compareAsPrimitive) {
          assertEquals(answer.lastRetValue, result);
        } else {
          assertTrue("Expected: " + System.identityHashCode(answer.lastRetValue) + ", actual=" +
              System.identityHashCode(result), answer.lastRetValue == result);
        }
      }
    }


  }

  public static Object[] constructMethodArgs(Method method) throws IllegalAccessException,
      InstantiationException {
    Class<?>[] paramTypes = method.getParameterTypes();
    Object[] params = new Object[paramTypes.length];
    for (int i = 0; i < paramTypes.length; i++) {
      params[i] = constructSingleArg(paramTypes[i]);
    }
    return params;
  }

  private static Object constructSingleArg(Class<?> clazz) {
    if (clazz.isPrimitive() || clazz.equals(String.class)) {
      return getValueForPrimitiveOrString(clazz);
    } else if (clazz.isEnum()) {
      if (clazz.getEnumConstants().length == 0) {
        return null;
      } else {
        return clazz.getEnumConstants()[0];
      }
    } else if (clazz.isArray() &&
        (clazz.getComponentType().isPrimitive() || clazz.getComponentType().equals(String.class))) {
      // Cannot mock. For now using null. Also does not handle deeply nested arrays.
      return null;
    } else {
      return mock(clazz);
    }
  }

  private static Object getValueForPrimitiveOrString(Class<?> clazz) {
    if (clazz.equals(String.class)) {
      return "teststring";
    } else if (clazz.equals(byte.class)) {
      return 'b';
    } else if (clazz.equals(short.class)) {
      return 2;
    } else if (clazz.equals(int.class)) {
      return 224;
    } else if (clazz.equals(long.class)) {
      return 445l;
    } else if (clazz.equals(float.class)) {
      return 2.24f;
    } else if (clazz.equals(double.class)) {
      return 4.57d;
    } else if (clazz.equals(boolean.class)) {
      return true;
    } else if (clazz.equals(char.class)) {
      return 'c';
    } else if (clazz.equals(void.class)) {
      return null;
    } else {
      throw new RuntimeException("Unrecognized type: " + clazz.getName());
    }
  }

  public static class TrackingAnswer implements Answer {

    public String lastMethodName;
    public Object[] lastArgs;
    public Object lastRetValue;
    boolean compareAsPrimitive;

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      lastArgs = invocation.getArguments();
      lastMethodName = invocation.getMethod().getName();
      Class<?> retType = invocation.getMethod().getReturnType();
      lastRetValue = constructSingleArg(retType);
      compareAsPrimitive = retType.isPrimitive() || retType.isEnum() || retType.equals(String.class);

      return lastRetValue;
    }
  }
}
