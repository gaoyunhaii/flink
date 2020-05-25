package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Value;

import javax.annotation.Nullable;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

class DefaultExtractor {

	/**
	 * TODO:: JAVADOC.
	 * @param type
	 * @param typeVariableBindings
	 * @param extractingClasses
	 * @return
	 */
	static TypeInformation<?> extract(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		TypeInformation<?> typeInformation;

		if ((typeInformation = extractTypeInformationForTypeVariable(type, typeVariableBindings)) != null) {
			return typeInformation;
		}

		if ((typeInformation = extractTypeInformationForRecursiveType(type, extractingClasses)) != null) {
			return typeInformation;
		}

		if ((typeInformation = extractTypeInformationForBasicType(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = extractTypeInformationForSQLTimeType(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = extractTypeInformationForValue(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = extractTypeInformationForEnum(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = ArrayTypeExtractor.extract(type, typeVariableBindings, extractingClasses)) != null) {
			return typeInformation;
		}

		if ((typeInformation = TypeInfoFactoryExtractor.extract(type, typeVariableBindings, extractingClasses)) != null) {
			return typeInformation;
		}

		if ((typeInformation = PojoTypeExtractor.extract(type, typeVariableBindings, extractingClasses)) != null) {
			return typeInformation;
		}

		if (isClassType(type)) {
			return new GenericTypeInfo<>(typeToClass(type));
		}

		return null;
	}

	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Recursive type
	// ------------------------------------------------------------------------

	private static TypeInformation<?> extractTypeInformationForRecursiveType(final Type type, final List<Class<?>> extractingClasses) {

		if (isClassType(type)) {
			// recursive types are handled as generic type info
			final Class<?> clazz = typeToClass(type);
			if (countTypeInHierarchy(extractingClasses, clazz) > 1) {
				return new GenericTypeInfo<>(clazz);
			}
		}
		return null;
	}

	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Basic type
	// ------------------------------------------------------------------------

	private static TypeInformation<?> extractTypeInformationForBasicType(final Type type) {
		if (isClassType(type)) {
			return BasicTypeInfo.getInfoFor(typeToClass(type));
		}
		return null;
	}

	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Basic type
	// ------------------------------------------------------------------------

	private static TypeInformation<?> extractTypeInformationForSQLTimeType(final Type type) {
		if (isClassType(type)) {
			return SqlTimeTypeInfo.getInfoFor(typeToClass(type));
		}
		return null;
	}

	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Enum
	// ------------------------------------------------------------------------
	@SuppressWarnings("unchecked")
	private static TypeInformation<?> extractTypeInformationForEnum(final Type type) {
		if (isClassType(type)) {
			if (Enum.class.isAssignableFrom(typeToClass(type))) {
				return new EnumTypeInfo(typeToClass(type));
			}
		}
		return null;
	}

	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Value type.
	// ------------------------------------------------------------------------

	private static TypeInformation<?> extractTypeInformationForValue(final Type type) {
		if (isClassType(type)) {
			if (Value.class.isAssignableFrom(typeToClass(type))) {
				Class<? extends Value> valueClass = typeToClass(type).asSubclass(Value.class);
				return ValueTypeInfo.getValueTypeInfo(valueClass);
			}
		}
		return null;
	}

	/**
	 * Extract the {@link TypeInformation} for the {@link TypeVariable}. This method find the {@link TypeInformation} of
	 * the type in the the given mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 * if the given type is {@link TypeVariable}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}.
	 * @return the {@link TypeInformation} of the given type if the mapping contains the type or{@code null} if the
	 * type is not {@link TypeVariable}
	 * @throws InvalidTypesException if the {@link TypeVariable} can not be found in the given mapping relation.
	 */
	@Nullable
	private static TypeInformation<?> extractTypeInformationForTypeVariable(
		final Type type, final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings) {

		if (type instanceof TypeVariable) {
			final TypeInformation<?> typeInfo = typeVariableBindings.get(type);
			if (typeInfo != null) {
				return typeInfo;
			} else {
				throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) type).getName() + "' in '"
					+ ((TypeVariable<?>) type).getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
					+ "The type extraction currently supports types with generic variables only in cases where "
					+ "all variables in the return type can be deduced from the input type(s). "
					+ "Otherwise the type has to be specified explicitly using type information.");
			}
		} else {
			return null;
		}
	}

	/**
	 * @return number of items with equal type or same raw type
	 */
	private static int countTypeInHierarchy(List<Class<?>> typeHierarchy, Type type) {
		int count = 0;
		for (Type t : typeHierarchy) {
			if (t == type || (isClassType(type) && t == typeToClass(type)) || (isClassType(t) && typeToClass(t) == type)) {
				count++;
			}
		}
		return count;
	}
}
