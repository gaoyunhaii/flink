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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;

import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.bindTypeVariablesWithTypeInformationFromInput;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getAllDeclaredFields;
import static org.apache.flink.api.java.typeutils.TypeResolve.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolve.materializeTypeVariable;
import static org.apache.flink.api.java.typeutils.TypeResolve.resolveTypeFromTypeHierarchy;

class PojoTypeExtractor {

	private static final Logger LOG = LoggerFactory.getLogger(PojoTypeExtractor.class);


	/**
	 * Extract the {@link TypeInformation} for the POJO type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}.
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type is not a pojo type
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	static <OUT> TypeInformation<OUT> extract(
		final Type type, final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		final Class<OUT> clazz;
		final ParameterizedType parameterizedType;

		if (type instanceof Class<?>) {
			clazz = (Class<OUT>) type;
			parameterizedType = null;
		} else if (type instanceof ParameterizedType) {
			clazz = (Class<OUT>) typeToClass(type);
			parameterizedType = (ParameterizedType) type;
		} else {
			return null;
		}

		final List<ParameterizedType> typeHierarchy;
		if (!Modifier.isPublic(clazz.getModifiers())) {
			LOG.info("Class " + clazz.getName() + " is not public so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			// TODO:: maybe we should return null
			return new GenericTypeInfo<>(clazz);
		}

		// add the hierarchy of the POJO itself if it is generic
		if (parameterizedType != null) {
			typeHierarchy = buildParameterizedTypeHierarchy(parameterizedType, Object.class);
		} else { // create a type hierarchy, if the incoming only contains the most bottom one or none.
			typeHierarchy = buildParameterizedTypeHierarchy(clazz, Object.class);
		}

		final List<Field> fields;
		try {
			fields = getAllDeclaredFields(clazz, false);
		} catch (InvalidTypesException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Unable to handle type {} " + clazz + " as POJO. Message: " + e.getMessage(), e);
			}
			return null;
		}
		if (fields.size() == 0) {
			LOG.info("No fields were detected for " + clazz + " so it cannot be used as a POJO type " +
				"and must be processed as GenericType. Please read the Flink documentation " +
				"on \"Data Types & Serialization\" for details of the effect on performance.");
			// TODO:: maybe we should always return null
			return new GenericTypeInfo<>(clazz);
		}

		List<PojoField> pojoFields = new ArrayList<>();
		for (Field field : fields) {
			Type fieldType = field.getGenericType();
			if (!isValidPojoField(field, clazz, typeHierarchy)) {
				LOG.info("Class " + clazz + " cannot be used as a POJO type because not all fields are valid POJO fields, " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
			try {
				List<ParameterizedType> fieldTypeHierarchy = new ArrayList<>(typeHierarchy);
				Type resolveFieldType = resolveTypeFromTypeHierarchy(fieldType, fieldTypeHierarchy, true);

				TypeInformation<?> ti = createTypeInfo(resolveFieldType, typeVariableBindings, extractingClasses);
				pojoFields.add(new PojoField(field, ti));
			} catch (InvalidTypesException e) {
				// TODO:: This exception handle leads to inconsistent behaviour when Tuple & TypeFactory fail.
				Class<?> genericClass = Object.class;
				if (isClassType(fieldType)) {
					genericClass = typeToClass(fieldType);
				}
				pojoFields.add(new PojoField(field, new GenericTypeInfo<>((Class<OUT>) genericClass)));
			}
		}

		CompositeType<OUT> pojoType = new PojoTypeInfo<>(clazz, pojoFields);

		//
		// Validate the correctness of the pojo.
		// returning "null" will result create a generic type information.
		//
		List<Method> methods = getAllDeclaredMethods(clazz);
		for (Method method : methods) {
			if (method.getName().equals("readObject") || method.getName().equals("writeObject")) {
				LOG.info("Class " + clazz + " contains custom serialization methods we do not call, so it cannot be used as a POJO type " +
					"and must be processed as GenericType. Please read the Flink documentation " +
					"on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
		}

		// Try retrieving the default constructor, if it does not have one
		// we cannot use this because the serializer uses it.
		Constructor defaultConstructor = null;
		try {
			defaultConstructor = clazz.getDeclaredConstructor();
		} catch (NoSuchMethodException e) {
			if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
				LOG.info(clazz + " is abstract or an interface, having a concrete "
					+ "type can increase performance.");
			} else {
				LOG.info(clazz + " is missing a default constructor so it cannot be used as a POJO type "
					+ "and must be processed as GenericType. Please read the Flink documentation "
					+ "on \"Data Types & Serialization\" for details of the effect on performance.");
				return null;
			}
		}
		if (defaultConstructor != null && !Modifier.isPublic(defaultConstructor.getModifiers())) {
			LOG.info("The default constructor of " + clazz + " is not Public so it cannot be used as a POJO type "
				+ "and must be processed as GenericType. Please read the Flink documentation "
				+ "on \"Data Types & Serialization\" for details of the effect on performance.");
			return null;
		}

		// everything is checked, we return the pojo
		return pojoType;
	}

	/**
	 * Checks if the given field is a valid pojo field:
	 * - it is public
	 * OR
	 *  - there are getter and setter methods for the field.
	 *
	 * @param f field to check
	 * @param clazz class of field
	 * @param typeHierarchy type hierarchy for materializing generic types
	 */
	private static boolean isValidPojoField(Field f, Class<?> clazz, List<ParameterizedType> typeHierarchy) {
		if (Modifier.isPublic(f.getModifiers())) {
			return true;
		} else {
			boolean hasGetter = false, hasSetter = false;
			final String fieldNameLow = f.getName().toLowerCase().replaceAll("_", "");

			Type fieldType = f.getGenericType();
			Class<?> fieldTypeWrapper = ClassUtils.primitiveToWrapper(f.getType());

			TypeVariable<?> fieldTypeGeneric = null;
			if (fieldType instanceof TypeVariable) {
				fieldTypeGeneric = (TypeVariable<?>) fieldType;
				fieldType = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) fieldType);
			}
			for (Method m : clazz.getMethods()) {
				final String methodNameLow = m.getName().endsWith("_$eq") ?
					m.getName().toLowerCase().replaceAll("_", "").replaceFirst("\\$eq$", "_\\$eq") :
					m.getName().toLowerCase().replaceAll("_", "");

				// check for getter
				if (// The name should be "get<FieldName>" or "<fieldName>" (for scala) or "is<fieldName>" for boolean fields.
					(methodNameLow.equals("get" + fieldNameLow)
						|| methodNameLow.equals("is" + fieldNameLow)
						|| methodNameLow.equals(fieldNameLow))
						&& m.getParameterTypes().length == 0 // no arguments for the getter
						// return type is same as field type (or the generic variant of it)
						&& (m.getGenericReturnType().equals(fieldType)
						|| (m.getReturnType().equals(fieldTypeWrapper))
						|| (fieldTypeGeneric != null && m.getGenericReturnType().equals(fieldTypeGeneric)))) {
					hasGetter = true;
				}
				// check for setters (<FieldName>_$eq for scala)
				if ((methodNameLow.equals("set" + fieldNameLow) || methodNameLow.equals(fieldNameLow + "_$eq"))
					&& m.getParameterTypes().length == 1 // one parameter of the field's type
					&& (m.getGenericParameterTypes()[0].equals(fieldType)
					|| (m.getParameterTypes()[0].equals(fieldTypeWrapper))
					|| (fieldTypeGeneric != null && m.getGenericParameterTypes()[0].equals(fieldTypeGeneric)))
					// return type is void (or the class self).
					&& (m.getReturnType().equals(Void.TYPE) || m.getReturnType().equals(clazz))) {
					hasSetter = true;
				}
			}
			if (hasGetter && hasSetter) {
				return true;
			} else {
				if (!hasGetter) {
					LOG.info(clazz + " does not contain a getter for field " + f.getName());
				}
				if (!hasSetter) {
					LOG.info(clazz + " does not contain a setter for field " + f.getName());
				}
				return false;
			}
		}
	}

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from the mapping relation between the fields
	 * and {@link TypeInformation}.
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@code null} if the
	 * typeInformation is not {@link PojoTypeInfo}.
	 */
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariable(
		final Type type,
		final TypeInformation<?> typeInformation) {

		if (typeInformation instanceof PojoTypeInfo && isClassType(type)) {
			final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();
			// build the entire type hierarchy for the pojo
			final List<ParameterizedType> pojoHierarchy = buildParameterizedTypeHierarchy(type, Object.class);
			// build the entire type hierarchy for the pojo
			final List<Field> fields = getAllDeclaredFields(typeToClass(type), false);
			for (Field field : fields) {
				final Type fieldType = field.getGenericType();
				final Type resolvedFieldType = resolveTypeFromTypeHierarchy(fieldType, pojoHierarchy, true);
				final Map<TypeVariable<?>, TypeInformation<?>> sub =
					bindTypeVariablesWithTypeInformationFromInput(resolvedFieldType, getTypeOfPojoField(typeInformation, field));
				typeVariableBindings.putAll(sub);
			}

			return typeVariableBindings.isEmpty() ? Collections.emptyMap() : typeVariableBindings;
		}
		return null;
	}

	private static TypeInformation<?> getTypeOfPojoField(TypeInformation<?> pojoInfo, Field field) {
		for (int j = 0; j < pojoInfo.getArity(); j++) {
			PojoField pf = ((PojoTypeInfo<?>) pojoInfo).getPojoFieldAt(j);
			if (pf.getField().getName().equals(field.getName())) {
				return pf.getTypeInformation();
			}
		}
		return null;
	}
}
