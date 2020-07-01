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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class TypeClassFactoryFinder {

	private static final List<AbstractTypeClassFactory> FACTORIES = new ArrayList<>();

	static {
		ServiceLoader.load(AbstractTypeClassFactory.class, TypeClassFactoryFinder.class.getClassLoader())
			.forEach(FACTORIES::add);
		FACTORIES.add(new JavaTypeClassFactory());
	}

	public static AbstractTypeClass forName(String name) {
		for (AbstractTypeClassFactory factory : FACTORIES) {
			AbstractTypeClass clazz = factory.forName(name);
			if (clazz != null) {
				return clazz;
			}
		}

		throw new RuntimeException("Unable to create the clazz for " + name);
	}
}
