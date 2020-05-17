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

package org.apache.flink.formats.hadoop.bulk;

import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OutputFormatUtilsV3 implements Serializable {

	private static final String FILE_OUTPUT_COMMITTER_NAME =
		"org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter";

	private static final String S3_STAGING_OUTPUT_COMMITTER_NAME =
		"org.apache.hadoop.fs.s3a.commit.staging.StagingCommitter";

	private static final String S3_MAGIC_OUTPUT_COMMITTER_NAME =
		"org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter";

	private static final String WORKER_PATH_METHOD_NAME = "getWorkPath";

	private final Map<String, String> enhancedCommitterClassNames = new HashMap<>();

	public OutputCommitter verifyAndEnhanceOutputCommitter(
		OutputCommitter outputCommitter,
		Path targetFilePath,
		TaskAttemptContext taskContext) {

		if (!TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), FILE_OUTPUT_COMMITTER_NAME) &&
			!TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), S3_STAGING_OUTPUT_COMMITTER_NAME) &&
			!TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), S3_MAGIC_OUTPUT_COMMITTER_NAME)) {

			throw new UnsupportedOperationException("");
		}

		String className = outputCommitter.getClass().getName();
		String enhancedClassName = enhancedCommitterClassNames.get(className);
		if (enhancedClassName == null) {
			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(outputCommitter.getClass());
			enhancer.setCallbackType(MethodInter.class);
			Class<?> enhancedClass = enhancer.createClass();
			Enhancer.registerStaticCallbacks(enhancedClass, new Callback[]{new MethodInter()});

			enhancedClassName = enhancedClass.getName();
			enhancedCommitterClassNames.put(className, enhancedClassName);
		}

		return createOutputCommitter(
			enhancedClassName,
			targetFilePath.getParent(),
			taskContext);
	}

	public Path getTaskAttemptPath(OutputCommitter outputCommitter, Path targetFilePath) {
		return invokeMethod(outputCommitter, WORKER_PATH_METHOD_NAME);
	}

	private OutputCommitter createOutputCommitter(
		String className,
		Path targetFilePath,
		TaskAttemptContext taskContext) {

		try {
			Class<?> clazz = Class.forName(className);
			Constructor<?> constructor = clazz.getConstructor(Path.class, TaskAttemptContext.class);
			return (OutputCommitter) constructor.newInstance(targetFilePath, taskContext);
		} catch (Exception e) {
			throw new RuntimeException("", e);
		}
	}

	public static class MethodInter implements MethodInterceptor {

		@Override
		public Object intercept(Object object, Method method, Object[] parameters, MethodProxy methodProxy) throws Throwable {
			if (TypeExtractionUtils.hasSuperclass(object.getClass(), FILE_OUTPUT_COMMITTER_NAME)) {
				if (method.getName().equals("cleanupJob")) {
					JobContext jobContext = (JobContext) parameters[0];
					String attemptNumber = jobContext.getConfiguration().get(MRJobConfig.APPLICATION_ATTEMPT_ID);
					Path targetFilePath = new Path(jobContext.getConfiguration().get("targetFilePath"));

					FileSystem fileSystem = FileSystem.get(targetFilePath.toUri(), jobContext.getConfiguration());
					fileSystem.delete(new Path(targetFilePath.getParent(), "_temporary/" + attemptNumber), true);
					return null;
				}
			} else if (TypeExtractionUtils.hasSuperclass(object.getClass(), S3_STAGING_OUTPUT_COMMITTER_NAME)) {
				if (method.getName().equals("cleanup")) {
					JobContext jobContext = (JobContext) parameters[0];

					Path workPath = invokeMethod(object, "getWorkPath");
					System.out.println("staging work path: " + workPath);
					workPath.getFileSystem(jobContext.getConfiguration()).delete(workPath, true);

					OutputCommitter wrapperCommitter = getFieldFromParent(object, "wrappedCommitter");
					wrapperCommitter.cleanupJob(jobContext);
					return null;
				}
			} else if (TypeExtractionUtils.hasSuperclass(object.getClass(), S3_MAGIC_OUTPUT_COMMITTER_NAME)) {
				if (method.getName().equals("cleanup")) {
					JobContext jobContext = (JobContext) parameters[0];

					Path jobAttemptPath = invokeMethod(
						object,
						"getJobAttemptPath",
						new Class<?>[]{JobContext.class},
						new Object[]{jobContext});
					System.out.println("jobAttemptPath: " + jobAttemptPath);
					jobAttemptPath.getFileSystem(jobContext.getConfiguration()).delete(jobAttemptPath, true);
					return null;
				}
			}

			return methodProxy.invokeSuper(object, parameters);
		}
	}

	private static <T> T getFieldFromParent(Object object, String fileName) {
		try {
			Field field = object.getClass().getSuperclass().getDeclaredField(fileName);
			field.setAccessible(true);
			return (T) field.get(object);
		} catch (Exception e) {
			throw new RuntimeException("", e);
		}
	}

	private static <T> T invokeMethod(Object object, String methodName) {
		return invokeMethod(object, methodName, new Class<?>[0], new Object[0]);
	}

	@SuppressWarnings("unchecked")
	private static <T> T invokeMethod(Object object, String methodName, Class<?>[] paramClasses, Object[] parameters) {
		try {
			Method method = object.getClass().getMethod(methodName, paramClasses);
			method.setAccessible(true);
			return (T) method.invoke(object, parameters);
		} catch (Exception e) {
			throw new RuntimeException("", e);
		}
	}

	private static Path addUniqueName(Path targetDirPath, Path attemptPath, String uniqueName) {
		List<String> targetDirElements = splitPath(targetDirPath);
		List<String> attemptElements = splitPath(attemptPath);

		attemptElements.add(targetDirElements.size(), uniqueName);
		return new Path(
			attemptPath.toUri().getScheme(),
			attemptPath.toUri().getAuthority(),
			mergePath(attemptElements));
	}

	private static List<String> splitPath(Path path) {
		List<String> result = new ArrayList<>();

		String rawPath = path.toUri().getRawPath();
		for (String element : rawPath.split("/")) {
			if (!element.trim().equals("")) {
				result.add(element);
			}
		}

		return result;
	}

	private static String mergePath(List<String> elements) {
		StringBuilder result = new StringBuilder();
		for (String element : elements) {
			result.append("/").append(element);
		}

		return result.toString();
	}
}
