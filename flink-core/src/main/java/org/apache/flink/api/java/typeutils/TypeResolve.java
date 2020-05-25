package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.VisibleForTesting;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.sameTypeVars;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This class is used to resolve the type from the type hierarchy.
 */
public class TypeResolve {

	/**
	 * Resolve all {@link TypeVariable}s of the type from the type hierarchy.
	 * @param type the type needed to be resolved
	 * @param typeHierarchy the set of types which the {@link TypeVariable} could be resolved from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 *                               (Some code path resolves the component type of a GenericArrayType. Some code path
	 *                               does not resolve the component type of a GenericArray. A example case is
	 *                               {@code testParameterizedArrays()})
	 * @return resolved type
	 */
	@VisibleForTesting
	static Type resolveTypeFromTypeHierarchy(
		final Type type,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		Type resolvedType = type;

		if (type instanceof TypeVariable) {
			resolvedType = materializeTypeVariable(typeHierarchy, (TypeVariable) type);
		}

		if (resolvedType instanceof ParameterizedType) {
			return resolveParameterizedType((ParameterizedType) resolvedType, typeHierarchy, resolveGenericArray);
		} else if (resolveGenericArray && resolvedType instanceof GenericArrayType) {
			return resolveGenericArrayType((GenericArrayType) resolvedType, typeHierarchy);
		}

		return resolvedType;
	}

	/**
	 * Build the parameterized type hierarchy from {@code subClass} to {@code baseClass}.
	 * @param subClass the begin class of the type hierarchy
	 * @param baseClass the end class of the type hierarchy
	 * @param traverseInterface whether to traverse the interface type
	 * @return the parameterized type hierarchy.
	 */
	static List<ParameterizedType> buildParameterizedTypeHierarchy(
		final Class<?> subClass,
		final Class<?> baseClass,
		final boolean traverseInterface) {

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();

		if (baseClass.equals(subClass) || !baseClass.isAssignableFrom(subClass)) {
			return Collections.emptyList();
		}

		if (traverseInterface) {
			final Type[] interfaceTypes = subClass.getGenericInterfaces();

			for (Type type : interfaceTypes) {
				if (baseClass.isAssignableFrom(typeToClass(type))) {
					final List<ParameterizedType> subTypeHierarchy = buildParameterizedTypeHierarchy(typeToClass(type), baseClass, traverseInterface);
					if (type instanceof ParameterizedType) {
						typeHierarchy.add((ParameterizedType) type);
					}
					typeHierarchy.addAll(subTypeHierarchy);
					return typeHierarchy;
				}
			}
		}

		if (baseClass.isAssignableFrom(subClass)) {
			final Type type = subClass.getGenericSuperclass();
			if (type != null) {
				final List<ParameterizedType> subTypeHierarchy = buildParameterizedTypeHierarchy(typeToClass(type), baseClass, traverseInterface);
				if (type instanceof ParameterizedType) {
					typeHierarchy.add((ParameterizedType) type);
				}
				typeHierarchy.addAll(subTypeHierarchy);
				return typeHierarchy;
			}
		}
		return Collections.emptyList();
	}

	/**
	 * Build the parameterized type hierarchy from {@code type} to the {@code baseClass}.
	 * @param type the begin type of the type hierarchy
	 * @param baseClass the end type of the type hierarchy
	 * @return the parameterized type hierarchy.
	 */
	static List<ParameterizedType> buildParameterizedTypeHierarchy(final Type type, final Class<?> baseClass) {
		if (isClassType(type)) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();
			if (type instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) type);
			}
			typeHierarchy.addAll(buildParameterizedTypeHierarchy(typeToClass(type), baseClass, false));
			return typeHierarchy.size() == 0 ? Collections.emptyList() : typeHierarchy;
		}

		return Collections.emptyList();
	}

	/**
	 * Resolve all {@link TypeVariable}s of a {@link ParameterizedType}.
	 * @param parameterizedType the {@link ParameterizedType} needed to be resolved.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 * @return resolved {@link ParameterizedType}
	 */
	private static Type resolveParameterizedType(
		final ParameterizedType parameterizedType,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		final Type[] actualTypeArguments = new Type[parameterizedType.getActualTypeArguments().length];

		int i = 0;
		for (Type type : parameterizedType.getActualTypeArguments()) {
			actualTypeArguments[i] = resolveTypeFromTypeHierarchy(type, typeHierarchy, resolveGenericArray);
			i++;
		}

		return new ResolvedParameterizedType(parameterizedType.getRawType(),
			parameterizedType.getOwnerType(),
			actualTypeArguments,
			parameterizedType.getTypeName());
	}

	/**
	 * Tries to find a concrete value (Class, ParameterizedType etc. ) for a TypeVariable by traversing the type
	 * hierarchy downwards.
	 *
	 * @param typeHierarchy the type hierarchy
	 * @param typeVar the type variable needed to be concreted
	 * @return the concrete value or
	 * 		   the most bottom type variable in the hierarchy
	 */
	@VisibleForTesting
	static Type materializeTypeVariable(List<ParameterizedType> typeHierarchy, TypeVariable<?> typeVar) {
		TypeVariable<?> inTypeTypeVar = typeVar;
		// iterate thru hierarchy from top to bottom until type variable gets a class assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			ParameterizedType curT = typeHierarchy.get(i);
			Class<?> rawType = (Class<?>) curT.getRawType();

			for (int paramIndex = 0; paramIndex < rawType.getTypeParameters().length; paramIndex++) {

				TypeVariable<?> curVarOfCurT = rawType.getTypeParameters()[paramIndex];

				// check if variable names match
				if (sameTypeVars(curVarOfCurT, inTypeTypeVar)) {
					Type curVarType = curT.getActualTypeArguments()[paramIndex];

					// another type variable level
					if (curVarType instanceof TypeVariable<?>) {
						inTypeTypeVar = (TypeVariable<?>) curVarType;
					} else {
						return curVarType;
					}
				}
			}
		}
		// can not be materialized, most likely due to type erasure
		// return the type variable of the deepest level
		return inTypeTypeVar;
	}

	/**
	 * Resolve the component type of {@link GenericArrayType}.
	 * @param genericArrayType the {@link GenericArrayType} needed to be resolved.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @return resolved {@link GenericArrayType}
	 */
	private static Type resolveGenericArrayType(final GenericArrayType genericArrayType, final List<ParameterizedType> typeHierarchy) {

		final Type resolvedComponentType =
			resolveTypeFromTypeHierarchy(genericArrayType.getGenericComponentType(), typeHierarchy, true);

		return new ResolvedGenericArrayType(genericArrayType.getTypeName(), resolvedComponentType);
	}

	private static class ResolvedGenericArrayType implements GenericArrayType {

		private final Type componentType;

		private final String typeName;

		ResolvedGenericArrayType(String typeName, Type componentType) {
			this.componentType = componentType;
			this.typeName = typeName;
		}

		@Override
		public Type getGenericComponentType() {
			return componentType;
		}

		public String getTypeName() {
			return typeName;
		}
	}

	private static class ResolvedParameterizedType implements ParameterizedType {

		private final Type rawType;

		private final Type ownerType;

		private final Type[] actualTypeArguments;

		private final String typeName;

		ResolvedParameterizedType(Type rawType, Type ownerType, Type[] actualTypeArguments, String typeName) {
			this.rawType = rawType;
			this.ownerType = ownerType;
			this.actualTypeArguments = actualTypeArguments;
			this.typeName = typeName;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return actualTypeArguments;
		}

		@Override
		public Type getRawType() {
			return rawType;
		}

		@Override
		public Type getOwnerType() {
			return ownerType;
		}

		public String getTypeName() {
			return typeName;
		}
	}
}
