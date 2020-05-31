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

package org.apache.flink.api.scala.codegen;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

public class ScalaUnresovledTypeVariable<D extends GenericDeclaration> implements TypeVariable<D> {

	private final D declaration;
	private final String name;

	public ScalaUnresovledTypeVariable(D declaration, String name) {
		this.declaration = declaration;
		this.name = name;
	}

	@Override
	public D getGenericDeclaration() {
		return declaration;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Type[] getBounds() {
		return new Type[0];
	}

	@Override
	public AnnotatedType[] getAnnotatedBounds() {
		return new AnnotatedType[0];
	}

	@Override
	public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
		return null;
	}

	@Override
	public Annotation[] getAnnotations() {
		return new Annotation[0];
	}

	@Override
	public Annotation[] getDeclaredAnnotations() {
		return new Annotation[0];
	}
}
