///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.formats.hadoop.bulk;
//
//import net.sf.cglib.proxy.Callback;
//import net.sf.cglib.proxy.Enhancer;
//import net.sf.cglib.proxy.MethodInterceptor;
//import net.sf.cglib.proxy.MethodProxy;
//import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.OutputCommitter;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//
//import java.io.Serializable;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// *
// */
//public class OutputFormatUtilsV2 implements Serializable {
//
//	private static final String FILE_OUTPUT_COMMITTER_NAME =
//		"org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter";
//
//	private static final String S3_OUTPUT_COMMITTER_NAME =
//		"org.apache.hadoop.fs.s3a.commit.AbstractS3ACommitter";
//
//	private static final String WORKER_PATH_METHOD_NAME = "getWorkPath";
//
//	private final Map<String, String> enhancedCommitterClassNames = new HashMap<>();
//
//	public OutputCommitter verifyAndEnhanceOutputCommitter(
//		OutputCommitter outputCommitter,
//		Path targetFilePath,
//		TaskAttemptContext taskContext) {
//
//		if (!TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), FILE_OUTPUT_COMMITTER_NAME) &&
//			!TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), S3_OUTPUT_COMMITTER_NAME)) {
//
//			throw new UnsupportedOperationException("");
//		}
//
//		String className = outputCommitter.getClass().getName();
//		String enhancedClassName = enhancedCommitterClassNames.get(className);
//		if (enhancedClassName == null) {
//			Enhancer enhancer = new Enhancer();
//			enhancer.setSuperclass(outputCommitter.getClass());
//			enhancer.setCallbackType(MethodInter.class);
//			Class<?> enhancedClass = enhancer.createClass();
//			Enhancer.registerStaticCallbacks(enhancedClass, new Callback[]{new MethodInter()});
//
//			enhancedClassName = enhancedClass.getName();
//			enhancedCommitterClassNames.put(className, enhancedClassName);
//		}
//
//		return createOutputCommitter(
//			enhancedClassName,
//			targetFilePath.getParent(),
//			taskContext);
//	}
//
//	public Path getTaskAttemptPath(OutputCommitter outputCommitter, Path targetFilePath) {
//		return addUniqueName(
//			targetFilePath.getParent(),
//			invokeMethod(outputCommitter, WORKER_PATH_METHOD_NAME),
//			"." + targetFilePath.getName());
//	}
//
//	private OutputCommitter createOutputCommitter(
//		String className,
//		Path targetFilePath,
//		TaskAttemptContext taskContext) {
//
//		try {
//			Class<?> clazz = Class.forName(className);
//			Constructor<?> constructor = clazz.getConstructor(Path.class, TaskAttemptContext.class);
//			return (OutputCommitter) constructor.newInstance(targetFilePath, taskContext);
//		} catch (Exception e) {
//			throw new RuntimeException("", e);
//		}
//	}
//
//	public static class MethodInter implements MethodInterceptor {
//
//		@Override
//		public Object intercept(Object object, Method method, Object[] parameters, MethodProxy methodProxy) throws Throwable {
//
//			if (method.getName().equals("getCommittedTaskPath") &&
//				parameters.length == 1 &&
//				parameters[0] instanceof TaskAttemptContext) {
//
//				Path result = (Path) methodProxy.invokeSuper(object, parameters);
//				TaskAttemptContext taskContext = (TaskAttemptContext) parameters[0];
//
//				Path targetFilePath = new Path(taskContext.getConfiguration().get("targetFilePath"));
//				return addUniqueName(targetFilePath.getParent(), result, "." + targetFilePath.getName());
//
//			} else if (method.getName().equals("getTaskAttemptPath") &&
//				parameters.length == 1 &&
//				parameters[0] instanceof TaskAttemptContext) {
//
//				if (!TypeExtractionUtils.hasSuperclass(object.getClass(), "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter")) {
//					Path result = (Path) methodProxy.invokeSuper(object, parameters);
//					TaskAttemptContext taskContext = (TaskAttemptContext) parameters[0];
//
//					Path targetFilePath = new Path(taskContext.getConfiguration().get("targetFilePath"));
//					return addUniqueName(targetFilePath.getParent(), result, "." + targetFilePath.getName());
//				} else {
//					Path result = (Path) methodProxy.invokeSuper(object, parameters);
//					TaskAttemptContext taskContext = (TaskAttemptContext) parameters[0];
//
//					Path targetFilePath = new Path(taskContext.getConfiguration().get("targetFilePath"));
//					return addUniqueName(targetFilePath.getParent(), result, "__magic");
//				}
//
//			} else if (method.getName().equals("getJobAttemptPath") &&
//				parameters.length == 1 &&
//				parameters[0] instanceof JobContext) {
//
//				Path result = (Path) methodProxy.invokeSuper(object, parameters);
//
//				JobContext jobContext = (JobContext) parameters[0];
//				Path targetFilePath = new Path(jobContext.getConfiguration().get("targetFilePath"));
//
//				return addUniqueName(targetFilePath.getParent(), result, "." + targetFilePath.getName());
//			} else if (method.getName().equals("abortPendingUploadsInCleanup")) {
//				// do not abort the required files
//				return null;
//			}
//
//			return methodProxy.invokeSuper(object, parameters);
//		}
//	}
//
//	private <T> T invokeMethod(Object object, String methodName) {
//		return invokeMethod(object, methodName, new Class<?>[0], new Object[0]);
//	}
//
//	@SuppressWarnings("unchecked")
//	private <T> T invokeMethod(Object object, String methodName, Class<?>[] paramClasses, Object[] parameters) {
//		try {
//			Method method = object.getClass().getMethod(methodName, paramClasses);
//			return (T) method.invoke(object, parameters);
//		} catch (Exception e) {
//			throw new RuntimeException("", e);
//		}
//	}
//
//	private static Path addUniqueName(Path targetDirPath, Path attemptPath, String uniqueName) {
//		List<String> targetDirElements = splitPath(targetDirPath);
//		List<String> attemptElements = splitPath(attemptPath);
//
//		attemptElements.add(targetDirElements.size(), uniqueName);
//		return new Path(
//			attemptPath.toUri().getScheme(),
//			attemptPath.toUri().getAuthority(),
//			mergePath(attemptElements));
//	}
//
//	private static List<String> splitPath(Path path) {
//		List<String> result = new ArrayList<>();
//
//		String rawPath = path.toUri().getRawPath();
//		for (String element : rawPath.split("/")) {
//			if (!element.trim().equals("")) {
//				result.add(element);
//			}
//		}
//
//		return result;
//	}
//
//	private static String mergePath(List<String> elements) {
//		StringBuilder result = new StringBuilder();
//		for (String element : elements) {
//			result.append("/").append(element);
//		}
//
//		return result.toString();
//	}
//}
