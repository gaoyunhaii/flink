/*
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

package org.apache.flink.util;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReflectionUtilTest {

	@Test
	public void testCheckingOverriding() throws NoSuchMethodException {
		Method method = Interface.class.getMethod("method", String.class);
		assertNotNull("test method is not found", method);

		assertFalse(ReflectionUtil.hasOverrideMethod(NotImplementInterfaceClass.class, method));
		assertFalse(ReflectionUtil.hasOverrideMethod(NotOverrideClass.class, method));
		assertTrue(ReflectionUtil.hasOverrideMethod(OverrideClass.class, method));
	}

	public interface Interface {

		default void method(String i) {

		}

	}

	public class NotImplementInterfaceClass { }

	public class NotOverrideClass implements Interface {

		void method(int overloading) { }

	}

	public class OverrideClass implements Interface {

		@Override
		public void method(String i) { }
	}
}
