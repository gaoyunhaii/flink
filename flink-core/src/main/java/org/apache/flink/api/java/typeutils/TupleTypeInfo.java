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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.runtime.Tuple0Serializer;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.types.Value;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.countFieldsInClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeExtractor.resolveTypeFromTypeHierarchy;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TypeInformation} for the tuple types of the Java API.
 *
 * @param <T> The type of the tuple.
 */
@Public
public final class TupleTypeInfo<T extends Tuple> extends TupleTypeInfoBase<T> {

	private static final long serialVersionUID = 1L;

	protected final String[] fieldNames;

	@SuppressWarnings("unchecked")
	@PublicEvolving
	public TupleTypeInfo(TypeInformation<?>... types) {
		this((Class<T>) Tuple.getTupleClass(types.length), types);
	}

	@PublicEvolving
	public TupleTypeInfo(Class<T> tupleType, TypeInformation<?>... types) {
		super(tupleType, types);

		checkArgument(
			types.length <= Tuple.MAX_ARITY,
			"The tuple type exceeds the maximum supported arity.");

		this.fieldNames = new String[types.length];

		for (int i = 0; i < types.length; i++) {
			fieldNames[i] = "f" + i;
		}
	}

	@Override
	@PublicEvolving
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	@PublicEvolving
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@SuppressWarnings("unchecked")
	@Override
	@PublicEvolving
	public TupleSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		if (getTypeClass() == Tuple0.class) {
			return (TupleSerializer<T>) Tuple0Serializer.INSTANCE;
		}

		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[getArity()];
		for (int i = 0; i < types.length; i++) {
			fieldSerializers[i] = types[i].createSerializer(executionConfig);
		}

		Class<T> tupleClass = getTypeClass();

		return new TupleSerializer<>(tupleClass, fieldSerializers);
	}

	@Override
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		return new TupleTypeComparatorBuilder();
	}

	private class TupleTypeComparatorBuilder implements TypeComparatorBuilder<T> {

		private final ArrayList<TypeComparator> fieldComparators = new ArrayList<>();
		private final ArrayList<Integer> logicalKeyFields = new ArrayList<>();

		@Override
		public void initializeTypeComparatorBuilder(int size) {
			fieldComparators.ensureCapacity(size);
			logicalKeyFields.ensureCapacity(size);
		}

		@Override
		public void addComparatorField(int fieldId, TypeComparator<?> comparator) {
			fieldComparators.add(comparator);
			logicalKeyFields.add(fieldId);
		}

		@Override
		public TypeComparator<T> createTypeComparator(ExecutionConfig config) {
			checkState(
				fieldComparators.size() > 0,
				"No field comparators were defined for the TupleTypeComparatorBuilder."
			);

			checkState(
				logicalKeyFields.size() > 0,
				"No key fields were defined for the TupleTypeComparatorBuilder."
			);

			checkState(
				fieldComparators.size() == logicalKeyFields.size(),
				"The number of field comparators and key fields is not equal."
			);

			final int maxKey = Collections.max(logicalKeyFields);

			checkState(
				maxKey >= 0,
				"The maximum key field must be greater or equal than 0."
			);

			TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];

			for (int i = 0; i <= maxKey; i++) {
				fieldSerializers[i] = types[i].createSerializer(config);
			}

			return new TupleComparator<>(
				listToPrimitives(logicalKeyFields),
				fieldComparators.toArray(new TypeComparator[0]),
				fieldSerializers
			);
		}
	}

	@Override
	public Map<String, TypeInformation<?>> getGenericParameters() {
		Map<String, TypeInformation<?>> m = new HashMap<>(types.length);
		for (int i = 0; i < types.length; i++) {
			m.put("T" + i, types[i]);
		}
		return m;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TupleTypeInfo) {
			@SuppressWarnings("unchecked")
			TupleTypeInfo<T> other = (TupleTypeInfo<T>) obj;
			return other.canEqual(this) &&
				super.equals(other) &&
				Arrays.equals(fieldNames, other.fieldNames);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}

	@Override
	public String toString() {
		return "Java " + super.toString();
	}

	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static <X extends Tuple> TupleTypeInfo<X> getBasicTupleTypeInfo(Class<?>... basicTypes) {
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}

		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}

			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				throw new IllegalArgumentException("Type at position " + i + " is not a basic type.");
			}
			infos[i] = info;
		}

		return new TupleTypeInfo<>(infos);
	}

	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <X extends Tuple> TupleTypeInfo<X> getBasicAndBasicValueTupleTypeInfo(Class<?>... basicTypes) {
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}

		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}

			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				try {
					info = ValueTypeInfo.getValueTypeInfo((Class<Value>) type);
					if (!((ValueTypeInfo<?>) info).isBasicValueType()) {
						throw new IllegalArgumentException("Type at position " + i + " is not a basic or value type.");
					}
				} catch (ClassCastException | InvalidTypesException e) {
					throw new IllegalArgumentException("Type at position " + i + " is not a basic or value type.", e);
				}
			}
			infos[i] = info;
		}

		return (TupleTypeInfo<X>) new TupleTypeInfo<>(infos);
	}

	private static int[] listToPrimitives(ArrayList<Integer> ints) {
		int[] result = new int[ints.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = ints.get(i);
		}
		return result;
	}

	/**
	 * Extract the {@link TypeInformation} for the {@link Tuple}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the type or {@code null} if the type information of the generic parameter of
	 * the {@link Tuple} could not be extracted
	 * @throws InvalidTypesException if the type is sub type of {@link Tuple} but not a generic class or if the type equals {@link Tuple}.
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	static TypeInformation<?> extractTypeInformationForTuple(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		if (!(isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type)))) {
			return null;
		}

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();

		Type curT = type;

		// do not allow usage of Tuple as type
		if (typeToClass(type).equals(Tuple.class)) {
			throw new InvalidTypesException(
				"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
		}

		// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
		// collect the types while moving up for a later top-down
		while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
			if (curT instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) curT);
			}
			curT = typeToClass(curT).getGenericSuperclass();
		}

		if (curT == Tuple0.class) {
			return new TupleTypeInfo(Tuple0.class);
		}

		// check if immediate child of Tuple has generics
		if (curT instanceof Class<?>) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		if (curT instanceof ParameterizedType) {
			typeHierarchy.add((ParameterizedType) curT);
		}

		curT = resolveTypeFromTypeHierarchy(curT, typeHierarchy, true);

		// create the type information for the subtypes
		final TypeInformation<?>[] subTypesInfo =
			createSubTypesInfo(type, (ParameterizedType) curT, typeVariableBindings, extractingClasses);

		if (subTypesInfo == null) {
			return null;
		}
		// return tuple info
		return new TupleTypeInfo(typeToClass(type), subTypesInfo);
	}

	/**
	 * Creates the TypeInformation for all generic type of {@link Tuple}.
	 *
	 * @param originalType most concrete subclass
	 * @param definingType type that defines the number of subtypes (e.g. Tuple2 -> 2 subtypes)
	 * @param typeVariableBindings the mapping relation between the type variable and the typeinformation
	 * @param extractingClasses the classes that the type is nested into.
	 * @return array containing TypeInformation of sub types or null if definingType contains
	 *     more subtypes (fields) that defined
	 */
	private static TypeInformation<?>[] createSubTypesInfo(
		final Type originalType,
		final ParameterizedType definingType,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		Preconditions.checkArgument(isClassType(originalType), "originalType has an unexpected type");

		final int typeArgumentsLength = definingType.getActualTypeArguments().length;
		// check the origin type contains additional fields.
		final int fieldCount = countFieldsInClass(typeToClass(originalType));
		if (fieldCount > typeArgumentsLength) {
			return null;
		}

		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];

		for (int i = 0; i < typeArgumentsLength; i++) {
			subTypesInfo[i] = createTypeInfo(definingType.getActualTypeArguments()[i], typeVariableBindings, extractingClasses);
		}
		return subTypesInfo;
	}
}
