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

package org.apache.flink.api.java.typeutils.javaruntime;

import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;

import java.lang.reflect.Type;

import static org.apache.flink.util.Preconditions.checkArgument;

public class JavaTypeClass implements AbstractTypeClass, JavaTypeProvider {

	private final Class<?> clazz;

	public JavaTypeClass(Class<?> clazz) {
		this.clazz = clazz;
	}

	@Override
	public boolean isAssignableFrom(AbstractTypeClass o2) {
		checkArgument(o2 instanceof JavaTypeClass);
		return clazz.isAssignableFrom(((JavaTypeClass) o2).clazz);
	}

	@Override
	public boolean hasSuper(String className) {
		return hasSuper(clazz, className);
	}

	private boolean hasSuper(Class<?> clazz, String className) {
		if (clazz.getName().equals(className)) {
			return true;
		}

		for (Class<?> inter : clazz.getInterfaces()) {
			if (hasSuper(inter, className)) {
				return true;
			}
		}

		return hasSuper(clazz.getSuperclass(), className);
	}

	@Override
	public String getName() {
		return clazz.getName();
	}

	@Override
	public Type getType() {
		return clazz;
	}
}
