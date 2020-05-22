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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getAllDeclaredFields;
import static org.apache.flink.api.java.typeutils.TypeExtractor.materializeTypeVariable;
import static org.apache.flink.api.java.typeutils.TypeExtractor.resolveTypeFromTypeHierarchy;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * TypeInformation for "Java Beans"-style types. Flink refers to them as POJOs,
 * since the conditions are slightly different from Java Beans.
 * A type is considered a FLink POJO type, if it fulfills the conditions below.
 * <ul>
 *   <li>It is a public class, and standalone (not a non-static inner class)</li>
 *   <li>It has a public no-argument constructor.</li>
 *   <li>All fields are either public, or have public getters and setters.</li>
 * </ul>
 * @param <T> The type represented by this type information.
 */
@Public
public class PojoTypeInfo<T> extends CompositeType<T> {

	private static final Logger LOG = LoggerFactory.getLogger(PojoTypeInfo.class);

	private static final long serialVersionUID = 1L;

	private static final String REGEX_FIELD = "[\\p{L}_$][\\p{L}\\p{Digit}_$]*";
	private static final String REGEX_NESTED_FIELDS = "(" + REGEX_FIELD + ")(\\.(.+))?";
	private static final String REGEX_NESTED_FIELDS_WILDCARD = REGEX_NESTED_FIELDS
					+ "|\\" + ExpressionKeys.SELECT_ALL_CHAR
					+ "|\\" + ExpressionKeys.SELECT_ALL_CHAR_SCALA;

	private static final Pattern PATTERN_NESTED_FIELDS = Pattern.compile(REGEX_NESTED_FIELDS);
	private static final Pattern PATTERN_NESTED_FIELDS_WILDCARD = Pattern.compile(REGEX_NESTED_FIELDS_WILDCARD);

	private final PojoField[] fields;

	private final int totalFields;

	@PublicEvolving
	public PojoTypeInfo(Class<T> typeClass, List<PojoField> fields) {
		super(typeClass);

		checkArgument(Modifier.isPublic(typeClass.getModifiers()),
				"POJO %s is not public", typeClass);

		this.fields = fields.toArray(new PojoField[0]);

		Arrays.sort(this.fields, Comparator.comparing(o -> o.getField().getName()));

		int counterFields = 0;

		for (PojoField field : fields) {
			counterFields += field.getTypeInformation().getTotalFields();
		}

		totalFields = counterFields;
	}

	@Override
	@PublicEvolving
	public boolean isBasicType() {
		return false;
	}

	@Override
	@PublicEvolving
	public boolean isTupleType() {
		return false;
	}

	@Override
	@PublicEvolving
	public int getArity() {
		return fields.length;
	}

	@Override
	@PublicEvolving
	public int getTotalFields() {
		return totalFields;
	}

	@Override
	@PublicEvolving
	public boolean isSortKeyType() {
		// Support for sorting POJOs that implement Comparable is not implemented yet.
		// Since the order of fields in a POJO type is not well defined, sorting on fields
		//   gives only some undefined order.
		return false;
	}

	@Override
	@PublicEvolving
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {

		Matcher matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression);
		if (!matcher.matches()) {
			throw new InvalidFieldReferenceException("Invalid POJO field reference \"" + fieldExpression + "\".");
		}

		String field = matcher.group(0);
		if (field.equals(ExpressionKeys.SELECT_ALL_CHAR) || field.equals(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
			// handle select all
			int keyPosition = 0;
			for (PojoField pField : fields) {
				if (pField.getTypeInformation() instanceof CompositeType) {
					CompositeType<?> cType = (CompositeType<?>) pField.getTypeInformation();
					cType.getFlatFields(ExpressionKeys.SELECT_ALL_CHAR, offset + keyPosition, result);
					keyPosition += cType.getTotalFields() - 1;
				} else {
					result.add(
						new NamedFlatFieldDescriptor(
							pField.getField().getName(),
							offset + keyPosition,
							pField.getTypeInformation()));
				}
				keyPosition++;
			}
			return;
		} else {
			field = matcher.group(1);
		}

		// get field
		int fieldPos = -1;
		TypeInformation<?> fieldType = null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].getTypeInformation();
				break;
			}
		}
		if (fieldPos == -1) {
			throw new InvalidFieldReferenceException("Unable to find field \"" + field + "\" in type " + this + ".");
		}
		String tail = matcher.group(3);
		if (tail == null) {
			if (fieldType instanceof CompositeType) {
				// forward offset
				for (int i = 0; i < fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				// add all fields of composite type
				((CompositeType<?>) fieldType).getFlatFields("*", offset, result);
			} else {
				// we found the field to add
				// compute flat field position by adding skipped fields
				int flatFieldPos = offset;
				for (int i = 0; i < fieldPos; i++) {
					flatFieldPos += this.getTypeAt(i).getTotalFields();
				}
				result.add(new FlatFieldDescriptor(flatFieldPos, fieldType));
			}
		} else {
			if (fieldType instanceof CompositeType<?>) {
				// forward offset
				for (int i = 0; i < fieldPos; i++) {
					offset += this.getTypeAt(i).getTotalFields();
				}
				((CompositeType<?>) fieldType).getFlatFields(tail, offset, result);
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \"" + tail
					+ "\" not possible on atomic type " + fieldType + ".");
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	@PublicEvolving
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {

		Matcher matcher = PATTERN_NESTED_FIELDS.matcher(fieldExpression);
		if (!matcher.matches()) {
			if (fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR) || fieldExpression.startsWith(ExpressionKeys.SELECT_ALL_CHAR_SCALA)) {
				throw new InvalidFieldReferenceException("Wildcard expressions are not allowed here.");
			} else {
				throw new InvalidFieldReferenceException("Invalid format of POJO field expression \"" + fieldExpression + "\".");
			}
		}

		String field = matcher.group(1);
		// get field
		int fieldPos = -1;
		TypeInformation<?> fieldType = null;
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(field)) {
				fieldPos = i;
				fieldType = fields[i].getTypeInformation();
				break;
			}
		}
		if (fieldPos == -1) {
			throw new InvalidFieldReferenceException("Unable to find field \"" + field + "\" in type " + this + ".");
		}

		String tail = matcher.group(3);
		if (tail == null) {
			// we found the type
			return (TypeInformation<X>) fieldType;
		} else {
			if (fieldType instanceof CompositeType<?>) {
				return ((CompositeType<?>) fieldType).getTypeAt(tail);
			} else {
				throw new InvalidFieldReferenceException("Nested field expression \"" + tail
					+ "\" not possible on atomic type " + fieldType + ".");
			}
		}
	}

	@Override
	@PublicEvolving
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		@SuppressWarnings("unchecked")
		TypeInformation<X> typed = (TypeInformation<X>) fields[pos].getTypeInformation();
		return typed;
	}

	@Override
	@PublicEvolving
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		return new PojoTypeComparatorBuilder();
	}

	@PublicEvolving
	public PojoField getPojoFieldAt(int pos) {
		if (pos < 0 || pos >= this.fields.length) {
			throw new IndexOutOfBoundsException();
		}
		return this.fields[pos];
	}

	@PublicEvolving
	public String[] getFieldNames() {
		String[] result = new String[fields.length];
		for (int i = 0; i < fields.length; i++) {
			result[i] = fields[i].getField().getName();
		}
		return result;
	}

	@Override
	@PublicEvolving
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].getField().getName().equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	@PublicEvolving
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		if (config.isForceKryoEnabled()) {
			return new KryoSerializer<>(getTypeClass(), config);
		}

		if (config.isForceAvroEnabled()) {
			return AvroUtils.getAvroUtils().createAvroSerializer(getTypeClass());
		}

		return createPojoSerializer(config);
	}

	public PojoSerializer<T> createPojoSerializer(ExecutionConfig config) {
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[fields.length];
		Field[] reflectiveFields = new Field[fields.length];

		for (int i = 0; i < fields.length; i++) {
			fieldSerializers[i] = fields[i].getTypeInformation().createSerializer(config);
			reflectiveFields[i] = fields[i].getField();
		}

		return new PojoSerializer<>(getTypeClass(), fieldSerializers, reflectiveFields, config);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PojoTypeInfo) {
			@SuppressWarnings("unchecked")
			PojoTypeInfo<T> pojoTypeInfo = (PojoTypeInfo<T>) obj;

			return pojoTypeInfo.canEqual(this) &&
				super.equals(pojoTypeInfo) &&
				Arrays.equals(fields, pojoTypeInfo.fields) &&
				totalFields == pojoTypeInfo.totalFields;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * (31 * Arrays.hashCode(fields) + totalFields) + super.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof PojoTypeInfo;
	}

	@Override
	public String toString() {
		List<String> fieldStrings = new ArrayList<>();
		for (PojoField field : fields) {
			fieldStrings.add(field.getField().getName() + ": " + field.getTypeInformation().toString());
		}
		return "PojoType<" + getTypeClass().getName()
				+ ", fields = [" + StringUtils.join(fieldStrings, ", ") + "]"
				+ ">";
	}

	// --------------------------------------------------------------------------------------------

	private class PojoTypeComparatorBuilder implements TypeComparatorBuilder<T> {

		private ArrayList<TypeComparator> fieldComparators;
		private ArrayList<Field> keyFields;

		PojoTypeComparatorBuilder() {
			fieldComparators = new ArrayList<>();
			keyFields = new ArrayList<>();
		}

		@Override
		public void initializeTypeComparatorBuilder(int size) {
			fieldComparators.ensureCapacity(size);
			keyFields.ensureCapacity(size);
		}

		@Override
		public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
			fieldComparators.add(comparator);
			keyFields.add(fields[fieldId].getField());
		}

		@Override
		public TypeComparator<T> createTypeComparator(ExecutionConfig config) {
			checkState(
				keyFields.size() > 0,
				"No keys were defined for the PojoTypeComparatorBuilder.");

			checkState(
				fieldComparators.size() > 0,
				"No type comparators were defined for the PojoTypeComparatorBuilder.");

			checkState(
				keyFields.size() == fieldComparators.size(),
				"Number of key fields and field comparators is not equal.");

			return new PojoComparator<>(
				keyFields.toArray(new Field[0]),
				fieldComparators.toArray(new TypeComparator[0]),
				createSerializer(config),
				getTypeClass());
		}
	}

	/**
	 * Java Doc. //TODO::
	 */
	public static class NamedFlatFieldDescriptor extends FlatFieldDescriptor {

		private String fieldName;

		public NamedFlatFieldDescriptor(String name, int keyPosition, TypeInformation<?> type) {
			super(keyPosition, type);
			this.fieldName = name;
		}

		public String getFieldName() {
			return fieldName;
		}

		@Override
		public String toString() {
			return "NamedFlatFieldDescriptor [name=" + fieldName + " position=" + getPosition() + " typeInfo=" + getType() + "]";
		}
	}

	/**
	 * Extract the {@link TypeInformation} for the POJO type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}.
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type is not a pojo type
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	protected static <OUT> TypeInformation<OUT> extractTypeInformationFroPOJOType(
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
}
