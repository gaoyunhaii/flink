package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.HashSet;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * This class is used to extract the {@link TypeInformation} of Hadoop writable.
 * TODO:: make this default and throw exception & move the actual extractor to the hadoop module
 */
class HadoopWritableExtractor {

	/** The name of the class representing Hadoop's writable. */
	private static final String HADOOP_WRITABLE_CLASS = "org.apache.hadoop.io.Writable";

	private static final String HADOOP_WRITABLE_TYPEINFO_CLASS = "org.apache.flink.api.java.typeutils.WritableTypeInfo";
	/**
	 * Extract {@link TypeInformation} for hadoop 'Writable'.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @return the {@link TypeInformation} of the type of {@code null} if the type is not the sub type of 'Writable'.
	 * @throws RuntimeException if error occurs when loading the 'Writable' through the reflection.
	 */
	static TypeInformation<?> extract(final Type type) {
		if (isClassType(type)) {
			final Class<?> clazz = typeToClass(type);
			// check for writable types
			if (isHadoopWritable(clazz)) {
				return createHadoopWritableTypeInfo(clazz);
			}
		}
		return null;
	}

	@VisibleForTesting
	static boolean isHadoopWritable(Class<?> typeClass) {
		// check if this is directly the writable interface
		if (typeClass.getName().equals(HADOOP_WRITABLE_CLASS)) {
			return false;
		}

		final HashSet<Class<?>> alreadySeen = new HashSet<>();
		alreadySeen.add(typeClass);
		return hasHadoopWritableInterface(typeClass, alreadySeen);
	}

	private static boolean hasHadoopWritableInterface(Class<?> clazz,  HashSet<Class<?>> alreadySeen) {
		Class<?>[] interfaces = clazz.getInterfaces();
		for (Class<?> c : interfaces) {
			if (c.getName().equals(HADOOP_WRITABLE_CLASS)) {
				return true;
			}
			else if (alreadySeen.add(c) && hasHadoopWritableInterface(c, alreadySeen)) {
				return true;
			}
		}

		Class<?> superclass = clazz.getSuperclass();
		return superclass != null && alreadySeen.add(superclass) && hasHadoopWritableInterface(superclass, alreadySeen);
	}

	@VisibleForTesting
	public static <T> TypeInformation<T> createHadoopWritableTypeInfo(Class<T> clazz) {
		checkNotNull(clazz);

		Class<?> typeInfoClass;
		try {
			typeInfoClass =
				Class.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, Thread.currentThread().getContextClassLoader());
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load the TypeInformation for the class '"
				+ HADOOP_WRITABLE_CLASS + "'. You may be missing the 'flink-hadoop-compatibility' dependency.");
		}

		try {
			Constructor<?> constr = typeInfoClass.getConstructor(Class.class);

			@SuppressWarnings("unchecked")
			TypeInformation<T> typeInfo = (TypeInformation<T>) constr.newInstance(clazz);
			return typeInfo;
		}
		catch (NoSuchMethodException | IllegalAccessException | InstantiationException e) {
			throw new RuntimeException("Incompatible versions of the Hadoop Compatibility classes found.");
		}
		catch (InvocationTargetException e) {
			throw new RuntimeException("Cannot create Hadoop WritableTypeInfo.", e.getTargetException());
		}
	}
}
