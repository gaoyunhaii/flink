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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils.LambdaExecutable;
import org.apache.flink.util.Preconditions;

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

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.checkAndExtractLambda;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility for reflection analysis on classes, to determine the return type of implementations of transformation
 * functions.
 *
 * <p>NOTES FOR USERS OF THIS CLASS:
 * Automatic type extraction is a hacky business that depends on a lot of variables such as generics,
 * compiler, interfaces, etc. The type extraction fails regularly with either {@link MissingTypeInfo} or
 * hard exceptions. Whenever you use methods of this class, make sure to provide a way to pass custom
 * type information as a fallback.
 */
@Public
public class TypeExtractor {

	/*
	 * NOTE: Most methods of the TypeExtractor work with a so-called "typeHierarchy".
	 * The type hierarchy describes all types (Classes, ParameterizedTypes, TypeVariables etc. ) and intermediate
	 * types from a given type of a function or type (e.g. MyMapper, Tuple2) until a current type
	 * (depends on the method, e.g. MyPojoFieldType).
	 *
	 * Thus, it fully qualifies types until tuple/POJO field level.
	 *
	 * A typical typeHierarchy could look like:
	 *
	 * UDF: MyMapFunction.class
	 * top-level UDF: MyMapFunctionBase.class
	 * RichMapFunction: RichMapFunction.class
	 * MapFunction: MapFunction.class
	 * Function's OUT: Tuple1<MyPojo>
	 * user-defined POJO: MyPojo.class
	 * user-defined top-level POJO: MyPojoBase.class
	 * POJO field: Tuple1<String>
	 * Field type: String.class
	 *
	 */

	public static final int[] NO_INDEX = new int[] {};

	// --------------------------------------------------------------------------------------------
	//  Function specific methods
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(
		MapFunction<IN, OUT> mapInterface,
		TypeInformation<IN> inType) {

		return getMapReturnTypes(mapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(
		MapFunction<IN, OUT> mapInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			mapInterface,
			MapFunction.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(
		FlatMapFunction<IN, OUT> flatMapInterface,
		TypeInformation<IN> inType) {

		return getFlatMapReturnTypes(flatMapInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(
		FlatMapFunction<IN, OUT> flatMapInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			flatMapInterface,
			FlatMapFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	/**
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(
		FoldFunction<IN, OUT> foldInterface,
		TypeInformation<IN> inType) {

		return getFoldReturnTypes(foldInterface, inType, null, false);
	}

	/**
	 * @deprecated will be removed in a future version
	 */
	@PublicEvolving
	@Deprecated
	public static <IN, OUT> TypeInformation<OUT> getFoldReturnTypes(
		FoldFunction<IN, OUT> foldInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			foldInterface,
			FoldFunction.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, ACC> TypeInformation<ACC> getAggregateFunctionAccumulatorType(
		AggregateFunction<IN, ACC, ?> function,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getAggregateFunctionReturnType(
		AggregateFunction<IN, ?, OUT> function,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			function,
			AggregateFunction.class,
			0,
			2,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(
		MapPartitionFunction<IN, OUT> mapPartitionInterface,
		TypeInformation<IN> inType) {

		return getMapPartitionReturnTypes(mapPartitionInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapPartitionReturnTypes(
		MapPartitionFunction<IN, OUT> mapPartitionInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			mapPartitionInterface,
			MapPartitionFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(
		GroupReduceFunction<IN, OUT> groupReduceInterface,
		TypeInformation<IN> inType) {

		return getGroupReduceReturnTypes(groupReduceInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupReduceReturnTypes(
		GroupReduceFunction<IN, OUT> groupReduceInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			groupReduceInterface,
			GroupReduceFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(
		GroupCombineFunction<IN, OUT> combineInterface,
		TypeInformation<IN> inType) {

		return getGroupCombineReturnTypes(combineInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getGroupCombineReturnTypes(
		GroupCombineFunction<IN, OUT> combineInterface, TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			combineInterface,
			GroupCombineFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(
		FlatJoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getFlatJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getFlatJoinReturnTypes(
		FlatJoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			joinInterface,
			FlatJoinFunction.class,
			0,
			1,
			2,
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(
		JoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getJoinReturnTypes(joinInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getJoinReturnTypes(
		JoinFunction<IN1, IN2, OUT> joinInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			joinInterface,
			JoinFunction.class,
			0,
			1,
			2,
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(
		CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getCoGroupReturnTypes(coGroupInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCoGroupReturnTypes(
		CoGroupFunction<IN1, IN2, OUT> coGroupInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			coGroupInterface,
			CoGroupFunction.class,
			0,
			1,
			2,
			new int[]{2, 0},
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(
		CrossFunction<IN1, IN2, OUT> crossInterface,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		return getCrossReturnTypes(crossInterface, in1Type, in2Type, null, false);
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getCrossReturnTypes(
		CrossFunction<IN1, IN2, OUT> crossInterface,
		TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		return getBinaryOperatorReturnType(
			crossInterface,
			CrossFunction.class,
			0,
			1,
			2,
			NO_INDEX,
			in1Type,
			in2Type,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(
		KeySelector<IN, OUT> selectorInterface,
		TypeInformation<IN> inType) {

		return getKeySelectorTypes(selectorInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(
		KeySelector<IN, OUT> selectorInterface,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			selectorInterface,
			KeySelector.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(Partitioner<T> partitioner) {
		return getPartitionerTypes(partitioner, null, false);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(
		Partitioner<T> partitioner,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			partitioner,
			Partitioner.class,
			-1,
			0,
			new int[]{0},
			null,
			functionName,
			allowMissing);
	}

	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN> TypeInformation<IN> getInputFormatTypes(InputFormat<IN, ?> inputFormatInterface) {
		if (inputFormatInterface instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<IN>) inputFormatInterface).getProducedType();
		}
		return privateCreateTypeInfo(
			InputFormat.class,
			inputFormatInterface.getClass(),
			0,
			null,
			null);
	}

	// --------------------------------------------------------------------------------------------
	//  Generic extraction methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the unary operator's return type.
	 *
	 * <p>This method can extract a type in 4 different ways:
	 *
	 * <p>1. By using the generics of the base class like {@code MyFunction<X, Y, Z, IN, OUT>}.
	 *    This is what outputTypeArgumentIndex (in this example "4") is good for.
	 *
	 * <p>2. By using input type inference {@code SubMyFunction<T, String, String, String, T>}.
	 *    This is what inputTypeArgumentIndex (in this example "0") and inType is good for.
	 *
	 * <p>3. By using the static method that a compiler generates for Java lambdas.
	 *    This is what lambdaOutputTypeArgumentIndices is good for. Given that MyFunction has
	 *    the following single abstract method:
	 *
	 * <pre>
	 * {@code void apply(IN value, Collector<OUT> value)}
	 * </pre>
	 *
	 * <p>Lambda type indices allow the extraction of a type from lambdas. To extract the
	 *    output type <b>OUT</b> from the function one should pass {@code new int[] {1, 0}}.
	 *    "1" for selecting the parameter and 0 for the first generic in this type.
	 *    Use {@code TypeExtractor.NO_INDEX} for selecting the return type of the lambda for
	 *    extraction or if the class cannot be a lambda because it is not a single abstract
	 *    method interface.
	 *
	 * <p>4. By using interfaces such as {@link TypeInfoFactory} or {@link ResultTypeQueryable}.
	 *
	 * <p>See also comments in the header of this class.
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param inputTypeArgumentIndex Index of input generic type in the base class specification (ignored if inType is null)
	 * @param outputTypeArgumentIndex Index of output generic type in the base class specification
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the input type. See example.
	 * @param inType Type of the input elements (In case of an iterable, it is the element type) or null
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing (this generates a MissingTypeInfo for postponing an exception)
	 * @param <IN> Input type
	 * @param <OUT> Output type
	 * @return TypeInformation of the return type of the function
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getUnaryOperatorReturnType(
		Function function,
		Class<?> baseClass,
		int inputTypeArgumentIndex,
		int outputTypeArgumentIndex,
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		Preconditions.checkArgument(inType == null || inputTypeArgumentIndex >= 0, "Input type argument index was not provided");
		Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
		Preconditions.checkArgument(
			lambdaOutputTypeArgumentIndices != null,
			"Indices for output type arguments within lambda not provided");

		// explicit result type has highest precedence
		if (function instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) function).getProducedType();
		}

		// perform extraction
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				// paramLen is the total number of parameters of the provided lambda, it includes parameters added through closure
				final int paramLen = exec.getParameterTypes().length;

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);

				// number of parameters the SAM of implemented interface has; the parameter indexing applies to this range
				final int baseParametersLen = sam.getParameterTypes().length;

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						baseClass,
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
					TypeExtractionUtils.validateLambdaType(baseClass, output);
				}

				return (TypeInformation<OUT>) createTypeInfo(output, Collections.emptyMap(), Collections.emptyList());
			} else {
				return privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, inType, null);
			}
		}
		catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<OUT>) new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
			} else {
				throw e;
			}
		}
	}

	/**
	 * Returns the binary operator's return type.
	 *
	 * <p>This method can extract a type in 4 different ways:
	 *
	 * <p>1. By using the generics of the base class like {@code MyFunction<X, Y, Z, IN, OUT>}.
	 *    This is what outputTypeArgumentIndex (in this example "4") is good for.
	 *
	 * <p>2. By using input type inference {@code SubMyFunction<T, String, String, String, T>}.
	 *    This is what inputTypeArgumentIndex (in this example "0") and inType is good for.
	 *
	 * <p>3. By using the static method that a compiler generates for Java lambdas.
	 *    This is what lambdaOutputTypeArgumentIndices is good for. Given that MyFunction has
	 *    the following single abstract method:
	 *
	 * <pre>
	 * {@code void apply(IN value, Collector<OUT> value) }
	 * </pre>
	 *
	 * <p>Lambda type indices allow the extraction of a type from lambdas. To extract the
	 *    output type <b>OUT</b> from the function one should pass {@code new int[] {1, 0}}.
	 *    "1" for selecting the parameter and 0 for the first generic in this type.
	 *    Use {@code TypeExtractor.NO_INDEX} for selecting the return type of the lambda for
	 *    extraction or if the class cannot be a lambda because it is not a single abstract
	 *    method interface.
	 *
	 * <p>4. By using interfaces such as {@link TypeInfoFactory} or {@link ResultTypeQueryable}.
	 *
	 * <p>See also comments in the header of this class.
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param input1TypeArgumentIndex Index of first input generic type in the class specification (ignored if in1Type is null)
	 * @param input2TypeArgumentIndex Index of second input generic type in the class specification (ignored if in2Type is null)
	 * @param outputTypeArgumentIndex Index of output generic type in the class specification
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the output type. See example.
	 * @param in1Type Type of the left side input elements (In case of an iterable, it is the element type)
	 * @param in2Type Type of the right side input elements (In case of an iterable, it is the element type)
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing (this generates a MissingTypeInfo for postponing an exception)
	 * @param <IN1> Left side input type
	 * @param <IN2> Right side input type
	 * @param <OUT> Output type
	 * @return TypeInformation of the return type of the function
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> getBinaryOperatorReturnType(
		Function function,
		Class<?> baseClass,
		int input1TypeArgumentIndex,
		int input2TypeArgumentIndex,
		int outputTypeArgumentIndex,
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type,
		String functionName,
		boolean allowMissing) {

		Preconditions.checkArgument(in1Type == null || input1TypeArgumentIndex >= 0, "Input 1 type argument index was not provided");
		Preconditions.checkArgument(in2Type == null || input2TypeArgumentIndex >= 0, "Input 2 type argument index was not provided");
		Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
		Preconditions.checkArgument(
			lambdaOutputTypeArgumentIndices != null,
			"Indices for output type arguments within lambda not provided");

		// explicit result type has highest precedence
		if (function instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) function).getProducedType();
		}

		// perform extraction
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);
				final int baseParametersLen = sam.getParameterTypes().length;

				// parameters must be accessed from behind, since JVM can add additional parameters e.g.
				// when using local variables inside lambda function
				final int paramLen = exec.getParameterTypes().length;

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						baseClass,
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
					TypeExtractionUtils.validateLambdaType(baseClass, output);
				}

				return (TypeInformation<OUT>) createTypeInfo(output, Collections.emptyMap(), Collections.emptyList());
			}
			else {
				return privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, in1Type, in2Type);
			}
		}
		catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<OUT>) new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
			} else {
				throw e;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Create type information
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public static <T> TypeInformation<T> createTypeInfo(Class<T> type) {
		return (TypeInformation<T>) createTypeInfo((Type) type);
	}

	public static TypeInformation<?> createTypeInfo(Type t) {
		TypeInformation<?> ti = createTypeInfo(t, Collections.emptyMap(), Collections.emptyList());
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	/**
	 * Creates a {@link TypeInformation} from the given parameters.
	 *
	 * <p>If the given {@code instance} implements {@link ResultTypeQueryable}, its information
	 * is used to determine the type information. Otherwise, the type information is derived
	 * based on the given class information.
	 *
	 * @param instance			instance to determine type information for
	 * @param baseClass			base class of {@code instance}
	 * @param clazz				class of {@code instance}
	 * @param returnParamPos	index of the return type in the type arguments of {@code clazz}
	 * @param <OUT>				output type
	 * @return type information
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <OUT> TypeInformation<OUT> createTypeInfo(Object instance, Class<?> baseClass, Class<?> clazz, int returnParamPos) {

		if (instance instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) instance).getProducedType();
		} else {
			return createTypeInfo(baseClass, clazz, returnParamPos, null, null);
		}
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfo(
		Class<?> baseClass,
		Class<?> clazz,
		int returnParamPos,
		TypeInformation<IN1> in1Type,
		TypeInformation<IN2> in2Type) {

		TypeInformation<OUT> ti =  privateCreateTypeInfo(baseClass, clazz, returnParamPos, in1Type, in2Type);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	/**
	 * Creates type information from a given Class such as Integer, String[] or POJOs.
	 *
	 * <p>This method does not support ParameterizedTypes such as Tuples or complex type hierarchies.
	 * In most cases {@link TypeExtractor#createTypeInfo(Type)} is the recommended method for type extraction
	 * (a Class is a child of Type).
	 *
	 * @param clazz a Class to create TypeInformation for
	 * @return TypeInformation that describes the passed Class
	 */
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		return createTypeInfo(clazz, Collections.emptyMap(), Collections.emptyList());
	}

	// --------------------------------------------------------------------------------------------
	//  Create type information for object.
	// --------------------------------------------------------------------------------------------
	@SuppressWarnings("unchecked")
	public static <X> TypeInformation<X> getForObject(X value) {
		checkNotNull(value);

		TypeInformation<X> typeInformation;
		if ((typeInformation = (TypeInformation<X>) TypeInfoFactoryExtractor.extract(value.getClass(), Collections.emptyMap(), Collections.emptyList())) != null) {
			return typeInformation;
		}

		if ((typeInformation = (TypeInformation<X>) TupleTypeExtractor.extract(value)) != null) {
			return typeInformation;
		}

		if ((typeInformation = (TypeInformation<X>) RowTypeExtractor.extract(value)) != null) {
			return typeInformation;
		}

		return createTypeInfo(value.getClass(), Collections.emptyMap(), Collections.emptyList());
	}

	// --------------------------------------------------------------------------------------------
	//  Extract type parameters
	// --------------------------------------------------------------------------------------------

	/**
	 * Resolve the type of the {@code pos}-th generic parameter of the {@code baseClass} from a parameterized type hierarchy
	 * that is built from {@code clazz} to {@code baseClass}. If the {@code pos}-th generic parameter is a generic class the
	 * type of generic parameters of it would also be resolved. This is a recursive resolving process.
	 *
	 * @param baseClass a generic class/interface
	 * @param clazz a sub class of the {@code baseClass}
	 * @param pos the position of generic parameter in the {@code baseClass}
	 * @return the type of the {@code pos}-th generic parameter
	 */
	@PublicEvolving
	public static Type getParameterType(Class<?> baseClass, Class<?> clazz, int pos) {
		final List<ParameterizedType> typeHierarchy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(clazz, baseClass);

		if (typeHierarchy.size() < 1) {
			throw new InvalidTypesException("The types of the interface " + baseClass.getName() + " could not be inferred. " +
				"Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point");
		}

		final Type baseClassType =
			resolveTypeFromTypeHierarchy(typeHierarchy.get(typeHierarchy.size() - 1), typeHierarchy, false);

		return ((ParameterizedType) baseClassType).getActualTypeArguments()[pos];
	}

	// --------------------------------------------------------------------------------------------
	//  Utility methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Recursively determine all declared fields
	 * This is required because class.getFields() is not returning fields defined
	 * in parent classes.
	 *
	 * @param clazz class to be analyzed
	 * @param ignoreDuplicates if true, in case of duplicate field names only the lowest one
	 *                            in a hierarchy will be returned; throws an exception otherwise
	 * @return list of fields
	 */
	@PublicEvolving
	public static List<Field> getAllDeclaredFields(Class<?> clazz, boolean ignoreDuplicates) {
		List<Field> result = new ArrayList<>();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
					continue; // we have no use for transient or static fields
				}
				if (hasFieldWithSameName(field.getName(), result)) {
					if (ignoreDuplicates) {
						continue;
					} else {
						throw new InvalidTypesException("The field " + field + " is already contained in the hierarchy of the " +
							clazz + ".Please use unique field names through your classes hierarchy");
					}
				}
				result.add(field);
			}
			clazz = clazz.getSuperclass();
		}
		return result;
	}

	@PublicEvolving
	public static Field getDeclaredField(Class<?> clazz, String name) {
		for (Field field : getAllDeclaredFields(clazz, true)) {
			if (field.getName().equals(name)) {
				return field;
			}
		}
		return null;
	}

	private static boolean hasFieldWithSameName(String name, List<Field> fields) {
		for (Field field : fields) {
			if (name.equals(field.getName())) {
				return true;
			}
		}
		return false;
	}

	// ----------------------------------- private methods ----------------------------------------

	// for (Rich)Functions
	@SuppressWarnings("unchecked")
	private static <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(
		final Class<?> baseClass,
		final Class<?> clazz,
		final int returnParamPos,
		final TypeInformation<IN1> in1TypeInfo,
		final TypeInformation<IN2> in2TypeInfo) {

		final List<ParameterizedType> functionTypeHierarchy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(clazz, baseClass);

		if (functionTypeHierarchy.size() < 1) {
			throw new InvalidTypesException("The types of the interface " + baseClass.getName() + " could not be inferred. " +
				"Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point");
		}

		final ParameterizedType baseType = functionTypeHierarchy.get(functionTypeHierarchy.size() - 1);

		final Type returnType = resolveTypeFromTypeHierarchy(baseType.getActualTypeArguments()[returnParamPos], functionTypeHierarchy, false);

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();

		if (in1TypeInfo != null) {
			final Type in1Type = baseType.getActualTypeArguments()[0];
			final Type resolvedIn1Type = resolveTypeFromTypeHierarchy(in1Type, functionTypeHierarchy, false);
			typeVariableBindings.putAll(TypeVariableBinder.bindTypeVariables(resolvedIn1Type, in1TypeInfo));
		}

		if (in2TypeInfo != null) {
			final Type in2Type = baseType.getActualTypeArguments()[1];
			final Type resolvedIn2Type = resolveTypeFromTypeHierarchy(in2Type, functionTypeHierarchy, false);
			typeVariableBindings.putAll(TypeVariableBinder.bindTypeVariables(resolvedIn2Type, in2TypeInfo));
		}

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfo(returnType, typeVariableBindings, Collections.emptyList());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <OUT> TypeInformation<OUT> createTypeInfo(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		final List<Class<?>> currentExtractingClasses;
		if (isClassType(type)) {
			currentExtractingClasses = new ArrayList(extractingClasses);
			currentExtractingClasses.add(typeToClass(type));
		} else {
			currentExtractingClasses = extractingClasses;
		}

		TypeInformation<OUT> typeInformation;

		if ((typeInformation = (TypeInformation<OUT>) AvroTypeExtractorChecker.extract(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = (TypeInformation<OUT>) HadoopWritableExtractorChecker.extract(type)) != null) {
			return typeInformation;
		}

		if ((typeInformation = (TypeInformation<OUT>) TupleTypeExtractor.extract(type, typeVariableBindings, currentExtractingClasses)) != null) {
			return typeInformation;
		}

		if ((typeInformation = (TypeInformation<OUT>) DefaultExtractor.extract(type, typeVariableBindings, currentExtractingClasses)) != null) {
			return typeInformation;
		}

		throw new InvalidTypesException("Type Information could not be created.");
	}

}
