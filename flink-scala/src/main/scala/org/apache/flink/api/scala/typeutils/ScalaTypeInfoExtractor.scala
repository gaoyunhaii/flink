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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, NothingTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import java.lang.reflect.{Field, GenericArrayType, Modifier, ParameterizedType, Type, TypeVariable}
import java.util

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, ObjectArrayTypeInfo, PojoField, PojoTypeInfo, TypeExtractionUtils, TypeExtractor}
import org.apache.flink.api.java.typeutils.TypeResolver.ResolvedParameterizedType
import org.apache.flink.api.scala.codegen.{EnumParameterizedType, ScalaResolvedGenericArray, ScalaTypeVariable, TraversalOnceParameterizedType}
import org.apache.flink.types.Nothing
import java.util.Map

import org.apache.flink.api.java.typeutils.TypeExtractor.CustomizedHieraBuilder

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

/**
 * Extractors for scala types
 */
@Internal
class ScalaTypeInfoExtractor {

  def createTypeInfo(
    outputType: java.lang.reflect.Type,
    bindings : Map[TypeVariable[_], TypeInformation[_]],
    builder : CustomizedHieraBuilder): TypeInformation[_] = {

    // check for array first
    if (outputType.isInstanceOf[ScalaResolvedGenericArray]) {
      println("will be changed later... now handle array first...")
      val arrayType = outputType.asInstanceOf[ScalaResolvedGenericArray]
      val componentType = arrayType.getGenericComponentType

      if (componentType.isInstanceOf[ScalaTypeVariable[_]]) {
        val elementType = callExtract(componentType, bindings, builder)
        return elementType match {
          case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
            PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.BYTE_TYPE_INFO =>
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.CHAR_TYPE_INFO =>
            PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.DOUBLE_TYPE_INFO =>
            PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.FLOAT_TYPE_INFO =>
            PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.INT_TYPE_INFO =>
            PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.LONG_TYPE_INFO =>
            PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.SHORT_TYPE_INFO =>
            PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO

          case BasicTypeInfo.STRING_TYPE_INFO =>
            BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO

          case _ =>
            ObjectArrayTypeInfo.getInfoFor(elementType)
        }
      }

      return null
    }

    val clazz = getTrueClass(outputType)
    if (clazz == null) {
      return null
    }

    println()
    println("in create info, type is " + outputType + ", ", outputType.getClass, clazz.getName)
    new RuntimeException("check path" + ("in create info, type is " + outputType + ", ", outputType.getClass, clazz.getName).toString()).printStackTrace()

    if (!outputType.isInstanceOf[ScalaTypeVariable[_]] &&
      !outputType.isInstanceOf[EnumParameterizedType] &&
      !outputType.isInstanceOf[TraversalOnceParameterizedType]) {

      val mirror = ru.runtimeMirror(getClass.getClassLoader)
      val classSymbol = mirror.classSymbol(clazz)

      println(classSymbol.baseClasses.exists(b => {
        println("\t, base", b)
        b.eq(ru.typeOf[Any].typeSymbol)
      }))

      if (classSymbol.isJava) {
        println("is java, skip")
        return null
      }
    }

    // Base line: Only scala.Any is accept
    TypeExtractionUtils.hasSuperclass(clazz, "")

    if (isNothing(clazz)) {
      new NothingTypeInfo().asInstanceOf[TypeInformation[_]]
    } else if (isUnit(clazz)) {
      new UnitTypeInfo().asInstanceOf[TypeInformation[_]]
    } else if (isEither(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val leftTypeInfo = callExtract(pt.getActualTypeArguments()(0), bindings, builder)
      val rightTypeInfo = callExtract(pt.getActualTypeArguments()(1), bindings, builder)

      new EitherTypeInfo(pt.getRawType.asInstanceOf[Class[_ <: Either[Any, Any]]], leftTypeInfo.asInstanceOf[TypeInformation[Any]], rightTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isEnum(outputType)) {
      val moduleObj = parseEnumModule(outputType)
      new EnumValueTypeInfo[Enumeration](moduleObj, clazz.asInstanceOf[Class[Enumeration#Value]])
    } else if (isTry(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val subTypeInfo = callExtract(pt.getActualTypeArguments()(0), bindings, builder)
      new TryTypeInfo[Any, Try[Any]](subTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isOption(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val subTypeInfo = callExtract(pt.getActualTypeArguments()(0), bindings, builder)
      new OptionTypeInfo[Any, Option[Any]](subTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isCaseClass(clazz)) {
      //      val pt = outputType.asInstanceOf[ParameterizedType]
      //      pt.getActualTypeArguments map { subType => callExtract(subType) }
      // Now we need to try to get the actual class of the field
      var fields = parseCaseField(clazz)
      println("directly fields: " + fields.mkString(", "))

      if (outputType.isInstanceOf[ParameterizedType]) {
        // For each fields, we need to resolve the actual type from outputType. all the parameters should be there.
        fields = resolveFields(fields, outputType.asInstanceOf[ParameterizedType])
        println("Resolved fields: " + fields.mkString(", "))
      }

      val genericInfo = outputType match {
        case pt : ParameterizedType => pt.getActualTypeArguments.map(ag => callExtract(ag, bindings, builder))
        case _ => Array[TypeInformation[_]]()
      }

      val fieldNames = fields.map(_._1)
      val fieldTypes = fields.map(f => callExtract(f._2, bindings, builder))

      // Now we need to create the case class info for it...
      new CaseClassTypeInfo[Product](clazz.asInstanceOf[Class[Product]], genericInfo, fieldTypes, fieldNames) {
        /**
         * Creates a serializer for the type. The serializer may use the ExecutionConfig
         * for parameterization.
         *
         * @param config The config used to parameterize the serializer.
         * @return A serializer for this type.
         */
        override def createSerializer(config: ExecutionConfig): TypeSerializer[Product] = {
          val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
          for (i <- 0 until getArity) {
            fieldSerializers(i) = types(i).createSerializer(config)
          }

          new ScalaCaseClassSerializer[Product](getTypeClass, fieldSerializers)
        }
      }
    } else if (isTraversalOnce(clazz)) {
//      if (clazz.isInstance(BitSet)) {
      ////        return TypeExtractor.callExtract(outputType)
      ////      }

//      if (clazz.isInstance(SortedMap[Any, Any]) || clazz.isInstance(SortedSet[Any])) {
//        return TypeExtractor.callExtract(outputType)
//      }

      // Get the cbf ?
      if (!outputType.isInstanceOf[TraversalOnceParameterizedType]) {
        throw new RuntimeException("Why it is not TraversalOnceParameterizedType but " + outputType.getClass + "...?")
      }

      val mirror = ru.runtimeMirror(getClass.getClassLoader)
      val tpe = mirror.classSymbol(clazz).asType.toType

      val traversable = tpe.baseType(ru.typeOf[TraversableOnce[_]].typeSymbol)
      println("traversal once element type: " + traversable.typeArgs.head)

      //def

      val elementType = callExtract(outputType.asInstanceOf[ParameterizedType].getActualTypeArguments.head, bindings, builder)

      new TraversableTypeInfo[TraversableOnce[Any], Any](clazz.asInstanceOf[Class[TraversableOnce[Any]]], elementType.asInstanceOf[TypeInformation[Any]]) {
        override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[TraversableOnce[Any]] = {
          new TraversableSerializer[TraversableOnce[Any], Any](
            elementType.createSerializer(executionConfig).asInstanceOf[TypeSerializer[Any]],
            outputType.asInstanceOf[TraversalOnceParameterizedType].getCbfString)
        }
      }
    } else {
      val finalTry = analyzePojo(clazz, bindings, builder)
      finalTry
    }
  }

  def callExtract(t : Type, bindings : util.Map[TypeVariable[_], TypeInformation[_]], builder : CustomizedHieraBuilder): TypeInformation[_] = {
    TypeExtractor.extractWithBuilder(
      t,
      bindings,
      new util.ArrayList[Class[_]](),
      builder)
  }

  def isNothing(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType =:= ru.typeOf[Nothing]
  }

  def isUnit(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType =:= ru.typeOf[Unit]
  }

  def isEither(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Either[_, _]]
  }

  def isEnum(outputType : Type): Boolean = {
//    val mirror = ru.runtimeMirror(getClass.getClassLoader)
//    val classSymbol = mirror.classSymbol(clazz)
//
//    println("name", clazz.getName)
//
//    val owner = classSymbol.owner
//    owner.isModule &&
//      owner.typeSignature.baseClasses.contains(ru.typeOf[scala.Enumeration].typeSymbol)

    outputType.isInstanceOf[EnumParameterizedType]
  }

  def isTry(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Try[_]]
  }

  def isOption(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Option[_]]
  }

  def isCaseClass(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.asClass.isCaseClass
  }

  def isBitSet(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[BitSet]
  }

  def isTraversalOnce(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[TraversableOnce[_]] &&
      !(classSymbol.toType <:< ru.typeOf[SortedMap[_, _]]) &&
      !(classSymbol.toType <:< ru.typeOf[SortedSet[_]])
  }

  def analyzePojo(clazz: Class[_], bindings : Map[TypeVariable[_], TypeInformation[_]], builder : CustomizedHieraBuilder): TypeInformation[_] = {
    println("Analyze is pojo? ", clazz)
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    val tpe = classSymbol.toType

//    println(tpe, "decls", tpe.decls.mkString("\n\t"))
//    println(tpe, "members", tpe.members.mkString("\n\t"))

    val immutableFields = tpe.members filter { _.isTerm } map { _.asTerm } filter { _.isVal }
    if (immutableFields.nonEmpty) {
      // We don't support POJOs with immutable fields
      return null
    }

    val fields = tpe.members
      .filter { _.isTerm }
      .map { _.asTerm }
      .filter { _.isVar }
      .filter { !_.isStatic }
      .filterNot { _.annotations.exists( _.tpe <:< ru.typeOf[scala.transient]) }

    if (fields.isEmpty) {
      println("** no fields !!")
      return null
    }

    // check whether all fields are either: 1. public, 2. have getter/setter
    val invalidFields = fields filterNot {
      f =>
        f.isPublic ||
          (f.getter != ru.NoSymbol && f.getter.isPublic && f.setter != ru.NoSymbol && f.setter.isPublic)
    }

    if (invalidFields.nonEmpty) {
      return null
    }

    // check whether we have a zero-parameter ctor
    val hasZeroCtor = tpe.declarations exists  {
      case m: ru.MethodSymbol
        if m.isConstructor && m.paramss.length == 1 && m.paramss(0).length == 0 => true
      case _ => false
    }
    println("hasZeroCtor", hasZeroCtor)
    println(tpe.decls.mkString(",\n\t"))


    if (!hasZeroCtor) {
      // We don't support POJOs without zero-parameter ctor
      return null
    }

    // Here we will start extracting the information
    var traversalClazz: Class[_] = clazz
    val clazzFields = mutable.Map[String, Field]()

    var error = false
    while (traversalClazz != null) {
      for (field <- traversalClazz.getDeclaredFields) {
        if (clazzFields.contains(field.getName) && !Modifier.isStatic(field.getModifiers)) {
          println(s"The field $field is already contained in the " +
            s"hierarchy of the class $clazz. Please use unique field names throughout " +
            "your class hierarchy")
          error = true
        }
        clazzFields += (field.getName -> field)
      }
      traversalClazz = traversalClazz.getSuperclass
    }

    println("fields: ", fields.mkString(", "))

    if (error) {
      new GenericTypeInfo(clazz)
    } else {
//      val pojoFields = fields flatMap {
//        case (fName, fTpe) =>
//          val field = clazzFields(fName)
//          if (Modifier.isTransient(field.getModifiers) || Modifier.isStatic(field.getModifiers)) {
//            // ignore transient and static fields
//            // the TypeAnalyzer for some reason does not always detect transient fields
//            None
//          } else {
//            Some(new PojoField(clazzFields(fName), fTpe))
//          }
//      }

      val arrayList = new util.ArrayList[PojoField]()

      println("clazzFields", clazzFields.mkString(", "))

      fields.foreach(field => {
        val fName = field.name.decodedName.toString.trim
        val clazzField = clazzFields(fName)

        println("1", clazzField)
        val ti = callExtract(clazzField.getGenericType, bindings, builder)
        println("2", ti)
        arrayList.add(new PojoField(clazzField, ti))
      })

      return new PojoTypeInfo(clazz, arrayList)
    }
    null
  }

  def parseEnumModule(output : Type): Enumeration = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val moduleName = output.asInstanceOf[EnumParameterizedType].getEnumModule
    val owner = mirror.staticModule(moduleName)
    mirror.reflectModule(owner).instance.asInstanceOf[Enumeration]
  }

  def parseCaseField(clazz: Class[_]): Array[Tuple2[String, Type]] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    val caseClassType = classSymbol.toType
    val result = ArrayBuffer[Tuple2[String, Type]]()

    println("me", caseClassType, caseClassType.baseClasses)

    caseClassType.baseClasses exists {
      bc => {
        //        println("bc, ", bc, bc.getClass, caseClassType.getClass)
        !(bc.asClass.toType == caseClassType) && bc.asClass.isCaseClass
      }
    } match {
      case true =>
        throw new UnsupportedOperationException("Case-to-case inheritance is not supported.")

      case false =>
        val ctors = caseClassType.decls collect {
          case decl if decl.isConstructor && decl.asMethod.isPrimaryConstructor => decl.asMethod
        }

        ctors match {
          case c1 :: c2 :: _ =>
            throw new UnsupportedOperationException("Multiple constructors found, this is not supported.")
          case ctor :: Nil =>

            val caseFields = ctor.paramLists.flatten.map {
              sym => {
                val methodSym = caseClassType.member(sym.name).asMethod
                val getter = methodSym.getter
                val setter = methodSym.setter
                val returnType = methodSym.returnType.asSeenFrom(caseClassType, classSymbol)
                Tuple3(getter, setter, returnType)
              }
            }

            caseFields.foreach { field => {
              val (getter, setter, returnType) = field
              val name = getter.name.toString

              println("name", name)
              println(clazz.getMethod(name).getGenericReturnType)
              result.append((name, clazz.getMethod(name).getGenericReturnType))
            }
            }
        }
    }

    result.toArray
  }

  def resolveFields(tuples: Array[(String, Type)], outputType: ParameterizedType): Array[Tuple2[String, Type]] = {
    tuples.map(p => (p._1, _help_resolve(p._2, outputType)))
  }

  def _help_resolve(t: Type, outputType: ParameterizedType): Type = {
    t match {
      case pt : ParameterizedType => {
        val resolved = resolveFields(pt.getActualTypeArguments.map(t => (t.getTypeName, t)), outputType)
        val changed = (resolved zip pt.getActualTypeArguments).exists(p => {
          val ((_, rt), ot) = p
          !rt.equals(ot)
        })

        if (!changed) {
          pt
        } else {
          new ResolvedParameterizedType(pt.getRawType, pt.getOwnerType, resolved.map(p => p._2), pt.getTypeName)
        }
      }
      case c : Class[_] => c
      case gv : GenericArrayType => gv
      case tv : TypeVariable[_] => {
        val typeParams = outputType.getRawType.asInstanceOf[Class[_]].getTypeParameters
        val actualArguments = outputType.getActualTypeArguments

        for ((tp, ar) <- (typeParams zip actualArguments)) {
          println("\t searching", tp, tv, ar)
          if (tp.equals(tv)) {
            return ar
          }
        }

        throw new RuntimeException("Type variable should be resolved here!! " + tv)
      }
      case _ => throw new RuntimeException("Unknown type " + t)
    }
  }

  def getTrueClass(outputType: java.lang.reflect.Type): Class[_] = {
    outputType match {
      case pt: ParameterizedType => pt.getRawType.asInstanceOf[Class[_]]
      case c: Class[_] => c
      case _: GenericArrayType => null
      case _: TypeVariable[_] => null
      case _ => throw new RuntimeException("Bad type")
    }
  }


  //    def parseTraversalOnce(clazz: Class[_]): TypeInformation[_] = {
  //      val mirror = ru.runtimeMirror(getClass.getClassLoader)
  //      val classSymbol = mirror.classSymbol(clazz)
  //
  //      val traversable = classSymbol.toType.baseType(ru.typeOf[TraversableOnce[_]].typeSymbol)
  //      null
  //
  ////      traversable match {
  ////        case ru.TypeRef(_, _, elemTpe :: Nil) =>
  ////
  ////          import compat._ // this is needed in order to compile in Scala 2.11
  ////
  ////          // determine whether we can find an implicit for the CanBuildFrom because
  ////          // TypeInformationGen requires this. This catches the case where a user
  ////          // has a custom class that implements Iterable[], for example.
  ////          val cbfTpe = ru.TypeRef(
  ////            ru.typeOf[CanBuildFrom[_, _, _]],
  ////            ru.typeOf[CanBuildFrom[_, _, _]].typeSymbol,
  ////            classSymbol.toType :: elemTpe :: classSymbol.toType :: Nil)
  ////
  ////          val cbf = c.inferImplicitValue(cbfTpe, silent = true)
  ////
  ////          if (cbf == EmptyTree) {
  ////            None
  ////          } else {
  ////            Some(elemTpe.asSeenFrom(tpe, tpe.typeSymbol))
  ////          }
  ////        case _ => None
  ////      }
  //    }
}
