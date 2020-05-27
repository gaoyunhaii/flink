package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;

class ArrayTypeExtractor {

	/**
	 * Extract {@link TypeInformation} for the array type.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}. This
	 *                             is used to extract the {@link TypeInformation} for {@link TypeVariable}.
	 * @param extractingClasses contains the classes that type extractor stack is extracting for {@link TypeInformation}.
	 *                             This is used to check whether there is a recursive type.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type is not an array type.
	 */
	@Nullable
	static TypeInformation<?> extract(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		TypeInformation<?> typeInformation =
			extractTypeInformationForGenericArray(type, typeVariableBindings, extractingClasses);
		if (typeInformation != null) {
			return typeInformation;
		}

		return extractTypeInformationForClassArray(type, typeVariableBindings, extractingClasses);
	}

	/**
	 * Infer the {@link TypeInformation} of {@link TypeVariable} from the given {@link TypeInformation} that is
	 * {@link BasicArrayTypeInfo} or {@link PrimitiveArrayTypeInfo} or {@link ObjectArrayTypeInfo}.
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@code null}
	 * if the type is not {@link GenericArrayType}
	 */
	@Nullable
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariables(
		final Type type,
		final TypeInformation<?> typeInformation) {

		if (type instanceof GenericArrayType) {
			TypeInformation<?> componentInfo = null;
			if (typeInformation instanceof BasicArrayTypeInfo) {
				componentInfo = ((BasicArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
			} else if (typeInformation instanceof PrimitiveArrayTypeInfo) {
				componentInfo = BasicTypeInfo.getInfoFor(typeInformation.getTypeClass().getComponentType());
			} else if (typeInformation instanceof ObjectArrayTypeInfo) {
				componentInfo = ((ObjectArrayTypeInfo<?, ?>) typeInformation).getComponentInfo();
			}
			Preconditions.checkNotNull(componentInfo, "found unexpected array type information");
			return TypeVariableBinder.bindTypeVariables(((GenericArrayType) type).getGenericComponentType(), componentInfo);
		}
		return null;
	}

	/**
	 * Extract {@link TypeInformation} for {@link GenericArrayType}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}. This
	 *                             is used to extract the {@link TypeInformation} for {@link TypeVariable}.
	 * @param extractingClasses contains the classes that type extractor stack is extracting for {@link TypeInformation}.
	 *                             This is used to check whether there is a recursive type.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type is not a {@link GenericArrayType}
	 */
	@Nullable
	private static TypeInformation<?> extractTypeInformationForGenericArray(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		final Class<?> classArray;

		if (type instanceof GenericArrayType) {
			final GenericArrayType genericArray = (GenericArrayType) type;

			final Type componentType = ((GenericArrayType) type).getGenericComponentType();
			if (componentType instanceof Class) {
				final Class<?> componentClass = (Class<?>) componentType;

				classArray = (java.lang.reflect.Array.newInstance(componentClass, 0).getClass());
				return TypeExtractor.extract(classArray, typeVariableBindings, extractingClasses);
			} else {
				final TypeInformation<?> componentInfo = TypeExtractor.extract(
					genericArray.getGenericComponentType(),
					typeVariableBindings,
					extractingClasses);

				return ObjectArrayTypeInfo.getInfoFor(
					java.lang.reflect.Array.newInstance(componentInfo.getTypeClass(), 0).getClass(),
					componentInfo);
			}
		}
		return null;
	}

	/**
	 * Extract {@link TypeInformation} for the class array.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}. This
	 *                             is used to extract the {@link TypeInformation} for {@link TypeVariable}.
	 * @param extractingClasses contains the classes that type extractor stack is extracting for {@link TypeInformation}.
	 *                             This is used to check whether there is a recursive type.
	 * @return the {@link TypeInformation} of the given type if it is a array class or {@code null} if the type is not the array class.
	 */
	@Nullable
	private static TypeInformation<?> extractTypeInformationForClassArray(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		if (type instanceof Class && ((Class) type).isArray()) {
			final Class<?> classArray = (Class<?>) type;
			// primitive arrays: int[], byte[], ...
			final PrimitiveArrayTypeInfo<?> primitiveArrayInfo = PrimitiveArrayTypeInfo.getInfoFor(classArray);
			if (primitiveArrayInfo != null) {
				return primitiveArrayInfo;
			}

			// basic type arrays: String[], Integer[], Double[]
			final BasicArrayTypeInfo<?, ?> basicArrayInfo = BasicArrayTypeInfo.getInfoFor(classArray);
			if (basicArrayInfo != null) {
				return basicArrayInfo;
			} else {
				final TypeInformation<?> componentTypeInfo = TypeExtractor.extract(
					classArray.getComponentType(),
					typeVariableBindings,
					extractingClasses);

				return ObjectArrayTypeInfo.getInfoFor(classArray, componentTypeInfo);
			}
		}
		return null;
	}

}
