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

package org.apache.flink.api.scala.typeutils.analyzer;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Objects;

/**
 * The type variable representing Scala unresolved type parameters.
 */
public class ScalaTypeVariable<D extends GenericDeclaration> implements TypeVariable<D> {

	private final D genericDeclaration;

	private final String name;

	private final Type[] bounds;

	private final TypeInformation<?> typeInformation;

	public ScalaTypeVariable(D genericDeclaration, String name, Type[] bounds, TypeInformation<?> typeInformation) {
		this.genericDeclaration = genericDeclaration;
		this.name = name;
		this.bounds = bounds;
		this.typeInformation = typeInformation;
	}

	@Override
	public Type[] getBounds() {
		return bounds;
	}

	@Override
	public D getGenericDeclaration() {
		return genericDeclaration;
	}

	@Override
	public String getName() {
		return name;
	}

	public TypeInformation<?> getTypeInformation() {
		return typeInformation;
	}

	@Override
	public int hashCode() {
		return Objects.hash(genericDeclaration, name);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TypeVariable)) {
			return false;
		}

		TypeVariable<?> tv = (TypeVariable<?>) obj;
		return genericDeclaration.equals(tv.getGenericDeclaration()) && name.equals(tv.getName());
	}

	// The following methods are not supported.

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
