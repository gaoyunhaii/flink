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
package org.apache.flink.api.scala.codegen

import java.lang.reflect
import java.lang.reflect.{Field, Modifier, TypeVariable}
import java.{lang, util}
import java.util.HashMap

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.TypeResolver.{ResolvedGenericArrayType, ResolvedParameterizedType}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala.typeutils._
import org.apache.flink.types.Value

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.collection.{TraversableOnce, mutable}
import scala.language.postfixOps
import scala.reflect.macros.Context

@Internal
private[flink] trait TypeInformationGen[C <: Context] {
  this: MacroContextHolder[C]
    with TypeDescriptors[C]
    with TypeAnalyzer[C]
    with TreeGen[C] =>

  import c.universe._

  def extractTypeParameter[T: c.WeakTypeTag, T2: c.WeakTypeTag](tpe: Type, baseTpe: Type): c.Expr[java.lang.reflect.Type] = {
    println("extracting type parameter 1", tpe, tpe.typeSymbol.isParameter)
    println("extracting type parameter 2", tpe.typeArgs.mkString(","))

    println("Parameters...", c.weakTypeOf[TypeInformation[T]])
    val result = c.inferImplicitValue(
      c.weakTypeOf[TypeInformation[T]],
      silent = true,
      withMacrosDisabled = true,
      pos = c.enclosingPosition)
    println("Type parameters", result, result.getClass, "name is ", tpe.typeSymbol.name.toString)

    val baseTpeClass = c.Expr[Class[T2]](Literal(Constant(baseTpe)))
    val nameTree = c.Expr[String](Literal(Constant(tpe.typeSymbol.name.toString)))

    reify {
      new ScalaTypeVariable[Class[T2]](baseTpeClass.splice, nameTree.splice, Array(), c.Expr[TypeInformation[T]](result).splice)
    }
  }

  def extractType[T : c.WeakTypeTag](tpe: Type): c.Expr[java.lang.reflect.Type] = {
    // val tpe = weakTypeTag[T].tpe
    println("extracting 1", tpe, tpe.typeSymbol.isParameter)
    println("extracting 3, alias: ", tpe.typeSymbol.asType)
    tpe.typeArgs.foreach(ta => {
      println(ta, ta.typeSymbol)
    })

    if (tpe.typeSymbol.isParameter) {
      println("Root is type parameter.....")
      return extractTypeParameter(tpe, tpe)(c.WeakTypeTag(tpe), c.WeakTypeTag(tpe))
    }

    val tpeClazz = c.Expr[Class[T]](Literal(Constant(tpe)))
    val genericTypes = tpe match {
      case TypeRef(_, _, typeParams) =>
        val typeInfos = typeParams map { param => {
          if (param.typeSymbol.isParameter) {
            extractTypeParameter(param, tpe)(c.WeakTypeTag(param), c.WeakTypeTag(tpe)).tree
          } else {
            extractType(param)(c.WeakTypeTag(param)).tree
          }
        }}
        c.Expr[List[java.lang.reflect.Type]](mkList(typeInfos))
      case _ =>
        reify {
          List[java.lang.reflect.Type]()
        }
    }
    // Check the type parameters for this class
    println("extracting 4", tpeClazz)
    // println("extracting 5", genericTypes)

    // First check if it is array. Array will be returned as ResolvedArrayType
    if (tpe <:< typeOf[Array[_]]) {
      return reify {
        new ScalaResolvedGenericArray("", genericTypes.splice.head)
      }
    }

    // Check if it is enum... If it is, will use a special type for the module name
    try {
      val m = c.universe.rootMirror
      val fqn = tpe.dealias.toString.split('.')
      val moduleName = fqn.slice(0, fqn.size - 1).mkString(".")
      println("new enum owner: ", moduleName)

      val owner = m.staticModule(moduleName)
      val enumerationSymbol = typeOf[scala.Enumeration].typeSymbol
      val ownerExpr = c.Expr[String](Literal(Constant(moduleName)))
      if (owner.typeSignature.baseClasses.contains(enumerationSymbol)) {
        return reify {
          val tpe = tpeClazz.splice
          val owner = ownerExpr.splice

          new EnumParameterizedType(tpe, owner)
        }
      }
    } catch {
      // Do nothing
      case e: Throwable => println("some error occurs", e)
      case _ =>
    }

    // Check if it is traversalOnce & it requires the to check if it has canBuildFrom declaration
    // If not, it need to be deducted to GenericTypeInfo

    val cbfString = tpe match {
      case _ if tpe <:< typeOf[TraversableOnce[_]] => checkTraversalOnceHashCanBuildFrom(tpe)
      case _ => None
    }

    println("new ts 1 cbf string", cbfString)

    // now we need to extract the element type...
    val traversable = tpe.baseType(typeOf[TraversableOnce[_]].typeSymbol)
    val elementType = traversable match {
      case TypeRef(_, _, elemTpe :: Nil) => elemTpe.asSeenFrom(tpe, tpe.typeSymbol)
      case _ => null
    }

    if (cbfString.isDefined) {
      // This should be specialized traversalonce type
      reify {
        val cbfValue = cbfString.get.splice
        println("Cbf is ", cbfValue)

        new TraversalOnceParameterizedType(
          tpeClazz.splice,
          null,
          genericTypes.splice.toArray,
          tpeClazz.splice.getTypeName,
          null,
          cbfValue)
      }
    } else {
      reify {
        val list = genericTypes.splice
        if (list.isEmpty) {
          tpeClazz.splice
        } else {
          new ResolvedParameterizedType(
            tpeClazz.splice,
            null,
            genericTypes.splice.toArray,
            tpeClazz.splice.getTypeName)
        }
      }
    }
  }

  def mkTypeInfo2[T: c.WeakTypeTag]: c.Expr[TypeInformation[T]] = {
    // iterate over all the parameters
    // println("1. hahahaa")
    // println("2. " + weakTypeTag[T].tpe)
    val tpe = weakTypeTag[T].tpe
    val tpeJavaType = extractType(tpe)
    print("extract result", tpeJavaType)
    // val tpeJavaTypeExpr = c.Expr[java.lang.reflect.Type](Literal(Constant(tpeJavaType)))

    //
    //    val tpeClazz = c.Expr[Class[T]](Literal(Constant(tpe)))
    //    val genericTypeInfos = tpe match {
    //      case TypeRef(_, _, typeParams) =>
    //        val typeInfos = typeParams map { tpe => mkTypeInfo2(c.WeakTypeTag[T](tpe)).tree }
    //        c.Expr[List[TypeInformation[_]]](mkList(typeInfos))
    //      case _ =>
    //        reify {
    //          List[TypeInformation[_]]()
    //        }
    //    }
    //
    //    val constant = c.Expr[String](Literal(Constant("aaa")))

    reify {
      val javaType = tpeJavaType.splice

      // Compute the map required
      val bindings = new util.HashMap[TypeVariable[_], TypeInformation[_]]
      def checkTypeNeedBinding(javaTpe : java.lang.reflect.Type): Unit = {
        javaTpe match {
          case pt: ResolvedParameterizedType =>
            pt.getActualTypeArguments.foreach {
              checkTypeNeedBinding
            }
          case genericArray: ResolvedGenericArrayType =>
            checkTypeNeedBinding(genericArray.getGenericComponentType)
          case stv: ScalaTypeVariable[_] =>
            bindings.put(stv, stv.getTypeInformation)
          case _ =>
        }
      }

      checkTypeNeedBinding(javaType)
      println("In extract 2 reify before extract", javaType, "bindings is", bindings)
      // val extractorFactory = new ScalaTypeInfoExtractor()
      // extractorFactory.createTypeInfo(javaType).asInstanceOf[TypeInformation[T]]
      TypeExtractor.extractWithBuilder(
        javaType,
        bindings,
        new util.ArrayList[Class[_]](),
        new ScalaTypeHieraBuilder).asInstanceOf[TypeInformation[T]]
    }
  }

  def checkTraversalOnceHashCanBuildFrom(tpe: Type): Option[c.Expr[String]] = {
    val traversable = tpe.baseType(typeOf[TraversableOnce[_]].typeSymbol)

    traversable match {
      case TypeRef(_, _, elemTpe :: Nil) =>
        import compat._

        val cbfTpe = TypeRef(
          typeOf[CanBuildFrom[_, _, _]],
          typeOf[CanBuildFrom[_, _, _]].typeSymbol,
          tpe :: elemTpe :: tpe :: Nil)

        val cbf = c.inferImplicitValue(cbfTpe, silent = true)
        if (cbf == EmptyTree) {
          None
        } else {
          val finalElemTpe = elemTpe.asSeenFrom(tpe, tpe.typeSymbol)

          def replaceGenericTypeParameter(innerTpe: c.Type): c.Type = {
            println("\treplaceGenericTypeParameter", innerTpe, innerTpe.typeSymbol.isParameter)
            if (innerTpe.typeSymbol.isParameter) {
              innerTpe.erasure
            } else {
              innerTpe
            }
          }

          println("before tpe is ", tpe, "elem Type is ", finalElemTpe)
          val traversableTpe = tpe.map(replaceGenericTypeParameter)
          val replacedElemType = finalElemTpe.map(replaceGenericTypeParameter)
          println("after tpe is ", traversableTpe, "elem Type is ", replacedElemType)

          val cbf = q"scala.collection.generic.CanBuildFrom[$traversableTpe, $replacedElemType, $traversableTpe]"
          val cbfString = s"implicitly[$cbf]"
          Some(c.Expr[String](Literal(Constant(cbfString))))
        }
      case _ => None
    }
  }

  def mine[T <: Product : c.WeakTypeTag](
                                          desc: CaseClassDescriptor) : c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(weakTypeTag[T].tpe)))

    val genericTypeInfos = desc.tpe match {
      case TypeRef(_, _, typeParams) =>
        val typeInfos = typeParams map { tpe => mkTypeInfo(c.WeakTypeTag[T](tpe)).tree }
        c.Expr[List[TypeInformation[_]]](mkList(typeInfos))
      case _ =>
        reify {
          List[TypeInformation[_]]()
        }
    }

    val fields = desc.getters.toList map { field =>
      mkTypeInfo(field.desc)(c.WeakTypeTag(field.tpe)).tree
    }
    val fieldsExpr = c.Expr[Seq[TypeInformation[_]]](mkList(fields))
    val instance = mkCreateTupleInstance[T](desc)(c.WeakTypeTag(desc.tpe))

    val fieldNames = desc.getters map { f => Literal(Constant(f.getter.name.toString)) } toList
    val fieldNamesExpr = c.Expr[Seq[String]](mkSeq(fieldNames))

    reify {
      new CaseClassTypeInfo[T](tpeClazz.splice,
        genericTypeInfos.splice.toArray,
        fieldsExpr.splice,
        fieldNamesExpr.splice) {
        /**
         * Creates a serializer for the type. The serializer may use the ExecutionConfig
         * for parameterization.
         *
         * @param config The config used to parameterize the serializer.
         * @return A serializer for this type.
         */
        override def createSerializer(config: ExecutionConfig): TypeSerializer[T] = null
      }
    }
  }

  // This is for external calling by TypeUtils.createTypeInfo
  def mkTypeInfo[T: c.WeakTypeTag]: c.Expr[TypeInformation[T]] = {
    println("Start checking : ", weakTypeTag[T].tpe)
    val desc = getUDTDescriptor(weakTypeTag[T].tpe)
//    val result: c.Expr[TypeInformation[T]] = mkTypeInfo(desc)(c.WeakTypeTag(desc.tpe))

//    val unused = new Predef.Function[String, String] {
//      override def apply(v1: String): String = null
//    }

//    result
    val first = mine(desc.asInstanceOf[CaseClassDescriptor])(c.WeakTypeTag(weakTypeTag[T].tpe).asInstanceOf[c.WeakTypeTag[Product]]).asInstanceOf[c.Expr[TypeInformation[T]]]
    first
//
////     val result2: c.Expr[TypeInformation[T]] = mkTypeInfo2(c.weakTypeTag(weakTypeTag[T]))
////     println("result 2: ", result2)
////     println("result: ", result)
//    val second = result
//
//    if (desc.id < 10) {
//      first
//    } else {
//      second
//    }

//    result2
  }

  // We have this for internal use so that we can use it to recursively generate a tree of
  // TypeInformation from a tree of UDTDescriptor
  def mkTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = desc match {
    case f: FactoryTypeDescriptor => mkTypeInfoFromFactory(f)

    case cc@CaseClassDescriptor(_, tpe, _, _, _) =>
      mkCaseClassTypeInfo(cc)(c.WeakTypeTag(tpe).asInstanceOf[c.WeakTypeTag[Product]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]

    case tp: TypeParameterDescriptor => mkTypeParameter(tp)

    case p: PrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)
    case p: BoxedPrimitiveDescriptor => mkPrimitiveTypeInfo(p.tpe)

    case n: NothingDescriptor =>
      reify {
        new ScalaNothingTypeInfo().asInstanceOf[TypeInformation[T]]
      }

    case u: UnitDescriptor => reify {
      new UnitTypeInfo().asInstanceOf[TypeInformation[T]]
    }

    case e: EitherDescriptor => mkEitherTypeInfo(e)

    case e: EnumValueDescriptor => mkEnumValueTypeInfo(e)

    case tr: TryDescriptor => mkTryTypeInfo(tr)

    case o: OptionDescriptor => mkOptionTypeInfo(o)

    case a: ArrayDescriptor => mkArrayTypeInfo(a)

    case l: TraversableDescriptor => mkTraversableTypeInfo(l)

    case v: ValueDescriptor =>
      mkValueTypeInfo(v)(c.WeakTypeTag(v.tpe).asInstanceOf[c.WeakTypeTag[Value]])
        .asInstanceOf[c.Expr[TypeInformation[T]]]

    case pojo: PojoDescriptor => mkPojo(pojo)

    case javaTuple: JavaTupleDescriptor => mkJavaTuple(javaTuple)

    case d => mkGenericTypeInfo(d)
  }

  def mkTypeInfoFromFactory[T: c.WeakTypeTag](desc: FactoryTypeDescriptor)
  : c.Expr[TypeInformation[T]] = {

    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val baseClazz = c.Expr[Class[T]](Literal(Constant(desc.baseType)))

    val typeInfos = desc.params map { p => mkTypeInfo(p)(c.WeakTypeTag(p.tpe)).tree }
    val typeInfosList = c.Expr[List[TypeInformation[_]]](mkList(typeInfos.toList))

    reify {
      val factory = TypeInfoFactoryExtractor.getTypeInfoFactory[T](baseClazz.splice)
      val genericParameters = typeInfosList.splice
        .zip(baseClazz.splice.getTypeParameters).map { case (typeInfo, typeParam) =>
        typeParam.getName -> typeInfo
      }.toMap[String, TypeInformation[_]]
      factory.createTypeInfo(tpeClazz.splice, genericParameters.asJava)
    }
  }

  def mkCaseClassTypeInfo[T <: Product : c.WeakTypeTag](
                                                         desc: CaseClassDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))

    val genericTypeInfos = desc.tpe match {
      case TypeRef(_, _, typeParams) =>
        val typeInfos = typeParams map { tpe => mkTypeInfo(c.WeakTypeTag[T](tpe)).tree }
        c.Expr[List[TypeInformation[_]]](mkList(typeInfos))
      case _ =>
        reify {
          List[TypeInformation[_]]()
        }
    }

    val fields = desc.getters.toList map { field =>
      mkTypeInfo(field.desc)(c.WeakTypeTag(field.tpe)).tree
    }
    val fieldsExpr = c.Expr[Seq[TypeInformation[_]]](mkList(fields))
    val instance = mkCreateTupleInstance[T](desc)(c.WeakTypeTag(desc.tpe))

    val fieldNames = desc.getters map { f => Literal(Constant(f.getter.name.toString)) } toList
    val fieldNamesExpr = c.Expr[Seq[String]](mkSeq(fieldNames))
    reify {
      new CaseClassTypeInfo[T](
          tpeClazz.splice,
        genericTypeInfos.splice.toArray,
        fieldsExpr.splice,
        fieldNamesExpr.splice) {

        override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[T] = {
          val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
          for (i <- 0 until getArity) {
            fieldSerializers(i) = types(i).createSerializer(executionConfig)
          }
          // -------------------------------------------------------------------------------------
          // NOTE:
          // the following anonymous class is needed here, and should not be removed
          // (although appears to be unused) since it is required for backwards compatibility
          // with Flink versions pre 1.8, that were using Java deserialization.
          // -------------------------------------------------------------------------------------
          val unused = new ScalaCaseClassSerializer[T](getTypeClass(), fieldSerializers) {

            override def createInstance(fields: Array[AnyRef]): T = {
              instance.splice
            }
          }

          new ScalaCaseClassSerializer[T](getTypeClass, fieldSerializers)
        }
      }
    }
  }

  def mkEitherTypeInfo[T: c.WeakTypeTag](desc: EitherDescriptor): c.Expr[TypeInformation[T]] = {

    val eitherClass = c.Expr[Class[T]](Literal(Constant(weakTypeOf[T])))
    val leftTypeInfo = mkTypeInfo(desc.left)(c.WeakTypeTag(desc.left.tpe))
    val rightTypeInfo = mkTypeInfo(desc.right)(c.WeakTypeTag(desc.right.tpe))

    val result =
      q"""
      import org.apache.flink.api.scala.typeutils.EitherTypeInfo

      new EitherTypeInfo[${desc.left.tpe}, ${desc.right.tpe}, ${desc.tpe}](
        $eitherClass,
        $leftTypeInfo,
        $rightTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkEnumValueTypeInfo[T: c.WeakTypeTag](d: EnumValueDescriptor): c.Expr[TypeInformation[T]] = {

    val enumValueClass = c.Expr[Class[T]](Literal(Constant(weakTypeOf[T])))

    val result =
      q"""
      import org.apache.flink.api.scala.typeutils.EnumValueTypeInfo

      new EnumValueTypeInfo[${d.enum.typeSignature}](${d.enum}, $enumValueClass)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkTryTypeInfo[T: c.WeakTypeTag](desc: TryDescriptor): c.Expr[TypeInformation[T]] = {

    val elemTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    val result =
      q"""
      import org.apache.flink.api.scala.typeutils.TryTypeInfo

      new TryTypeInfo[${desc.elem.tpe}, ${desc.tpe}]($elemTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkOptionTypeInfo[T: c.WeakTypeTag](desc: OptionDescriptor): c.Expr[TypeInformation[T]] = {

    val elemTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    val result =
      q"""
      import org.apache.flink.api.scala.typeutils.OptionTypeInfo

      new OptionTypeInfo[${desc.elem.tpe}, ${desc.tpe}]($elemTypeInfo)
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkTraversableTypeInfo[T: c.WeakTypeTag](
                                               desc: TraversableDescriptor): c.Expr[TypeInformation[T]] = {
    val collectionClass = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val elementClazz = c.Expr[Class[T]](Literal(Constant(desc.elem.tpe)))
    val elementTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))
    println("ts 4", collectionClass, elementClazz, elementTypeInfo)

    // We substitute both type parameters in the Traversable as well as in the element type.
    //
    // This is probably best understood using some examples.
    //
    // No replacement:
    // CanBuildFrom[Seq[Any], Any, Seq[Any]]
    //   -> CanBuildFrom[Seq[Any], Any, Seq[Any]]
    //
    // No replacement:
    // CanBuildFrom[Seq[(Int, String)], (Int, String), Seq[(Int, String)]]
    //   -> CanBuildFrom[Seq[(Int, String)], (Int, String), Seq[(Int, String)]]
    //
    // Replacing type parameters of the element type, i.e. the Tuple type:
    // CanBuildFrom[Seq[(T, U)], (T, U), Seq[(T, U)]]
    //   -> CanBuildFrom[Seq[(Object, Object)], (Object, Object), Seq[(Object, Object)]]
    //
    // Replacing the element type itself because it is a type parameter:
    // CanBuildFrom[Seq[T], T, Seq[T]]
    //   -> CanBuildFrom[Seq[Object], Object, Seq[Object]]

    def replaceGenericTypeParameter(innerTpe: c.Type): c.Type = {
      println("\treplaceGenericTypeParameter", innerTpe, innerTpe.typeSymbol.isParameter)
      if (innerTpe.typeSymbol.isParameter) {
        innerTpe.erasure
      } else {
        innerTpe
      }
    }

    println("before tpe is ", desc.tpe, "elem Type is ", desc.elem.tpe)

    val traversableTpe = desc.tpe.map(replaceGenericTypeParameter)
    val elemTpe = desc.elem.tpe.map(replaceGenericTypeParameter)

    println("after tpe is ", traversableTpe, "elem Type is ", elemTpe)

    val cbf = q"scala.collection.generic.CanBuildFrom[$traversableTpe, $elemTpe, $traversableTpe]"
    val cbfString = s"implicitly[$cbf]"

    val cbfStringLiteral = c.Expr[Class[T]](Literal(Constant(cbfString)))

    val result =
      q"""
      import scala.collection.generic.CanBuildFrom
      import org.apache.flink.api.scala.typeutils.TraversableTypeInfo
      import org.apache.flink.api.scala.typeutils.TraversableSerializer
      import org.apache.flink.api.common.ExecutionConfig

      val elementTpe = $elementTypeInfo
      new TraversableTypeInfo($collectionClass, elementTpe) {
        def createSerializer(executionConfig: ExecutionConfig) = {

          // -------------------------------------------------------------------------------------
          // NOTE:
          // the following anonymous class is needed here, and should not be removed
          // (although appears to be unused) since it is required for backwards compatibility
          // with Flink versions pre 1.8, that were using Java deserialization.
          // -------------------------------------------------------------------------------------
          val unused = new TraversableSerializer[${desc.tpe}, ${desc.elem.tpe}](
              elementTpe.createSerializer(executionConfig),
              $cbfStringLiteral) {

                  override def legacyCbfCode = $cbfStringLiteral
              }

          new TraversableSerializer[${desc.tpe}, ${desc.elem.tpe}](
                                  elementTpe.createSerializer(executionConfig),
                                  $cbfStringLiteral)
        }
      }
    """

    c.Expr[TypeInformation[T]](result)
  }

  def mkArrayTypeInfo[T: c.WeakTypeTag](desc: ArrayDescriptor): c.Expr[TypeInformation[T]] = {
    val arrayClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val elementClazz = c.Expr[Class[T]](Literal(Constant(desc.elem.tpe)))
    val elementTypeInfo = mkTypeInfo(desc.elem)(c.WeakTypeTag(desc.elem.tpe))

    println("ArrayType", arrayClazz, elementClazz, elementTypeInfo)

    desc.elem match {
      // special case for string, which in scala is a primitive, but not in java
      case p: PrimitiveDescriptor if p.tpe <:< typeOf[String] =>
        reify {
          BasicArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case p: PrimitiveDescriptor =>
        reify {
          PrimitiveArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case bp: BoxedPrimitiveDescriptor =>
        reify {
          BasicArrayTypeInfo.getInfoFor(arrayClazz.splice)
        }
      case _ =>
        reify {
          val elementType = elementTypeInfo.splice.asInstanceOf[TypeInformation[_]]
          val result = elementType match {
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
          result.asInstanceOf[TypeInformation[T]]
        }
    }
  }

  def mkValueTypeInfo[T <: Value : c.WeakTypeTag](
                                                   desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      new ValueTypeInfo[T](tpeClazz.splice)
    }
  }

  def mkJavaTuple[T: c.WeakTypeTag](desc: JavaTupleDescriptor): c.Expr[TypeInformation[T]] = {

    val fieldsTrees = desc.fields map { f => mkTypeInfo(f)(c.WeakTypeTag(f.tpe)).tree }

    val fieldsList = c.Expr[List[TypeInformation[_]]](mkList(fieldsTrees.toList))

    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))

    reify {
      val fields = fieldsList.splice
      val clazz = tpeClazz.splice.asInstanceOf[Class[org.apache.flink.api.java.tuple.Tuple]]
      new TupleTypeInfo[org.apache.flink.api.java.tuple.Tuple](clazz, fields: _*)
        .asInstanceOf[TypeInformation[T]]
    }
  }


  def mkPojo[T: c.WeakTypeTag](desc: PojoDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    val fieldsTrees = desc.getters map {
      f =>
        val name = c.Expr(Literal(Constant(f.name)))
        val fieldType = mkTypeInfo(f.desc)(c.WeakTypeTag(f.tpe))
        reify {
          (name.splice, fieldType.splice)
        }.tree
    }

    val fieldsList = c.Expr[List[(String, TypeInformation[_])]](mkList(fieldsTrees.toList))

    reify {
      val fields = fieldsList.splice
      val clazz: Class[T] = tpeClazz.splice

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

      if (error) {
        new GenericTypeInfo(clazz)
      } else {
        val pojoFields = fields flatMap {
          case (fName, fTpe) =>
            val field = clazzFields(fName)
            if (Modifier.isTransient(field.getModifiers) || Modifier.isStatic(field.getModifiers)) {
              // ignore transient and static fields
              // the TypeAnalyzer for some reason does not always detect transient fields
              None
            } else {
              Some(new PojoField(clazzFields(fName), fTpe))
            }
        }

        new PojoTypeInfo(clazz, pojoFields.asJava)
      }
    }
  }

  def mkGenericTypeInfo[T: c.WeakTypeTag](desc: UDTDescriptor): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(desc.tpe)))
    reify {
      TypeExtractor.createTypeInfo(tpeClazz.splice)
    }
  }

  def mkTypeParameter[T: c.WeakTypeTag](
                                         typeParameter: TypeParameterDescriptor): c.Expr[TypeInformation[T]] = {

    val result = c.inferImplicitValue(
      c.weakTypeOf[TypeInformation[T]],
      silent = true,
      withMacrosDisabled = true,
      pos = c.enclosingPosition)

    println("Type parameters", c.weakTypeOf[TypeInformation[T]])

    if (result.isEmpty) {
      c.error(
        c.enclosingPosition,
        s"could not find implicit value of type TypeInformation[${typeParameter.tpe}].")
    }

    c.Expr[TypeInformation[T]](result)
  }

  def mkPrimitiveTypeInfo[T: c.WeakTypeTag](tpe: Type): c.Expr[TypeInformation[T]] = {
    val tpeClazz = c.Expr[Class[T]](Literal(Constant(tpe)))
    println("primitive 1 ", tpeClazz, tpe)
    reify {
      BasicTypeInfo.getInfoFor(tpeClazz.splice)
    }
  }

  def mkCreateTupleInstance[T: c.WeakTypeTag](desc: CaseClassDescriptor): c.Expr[T] = {
    val fields = desc.getters.zipWithIndex.map { case (field, i) =>
      val call = mkCall(Ident(newTermName("fields")), "apply")(List(Literal(Constant(i))))
      mkAsInstanceOf(call)(c.WeakTypeTag(field.tpe))
    }
    val result = Apply(Select(New(TypeTree(desc.tpe)), nme.CONSTRUCTOR), fields.toList)
    c.Expr[T](result)
  }
}
