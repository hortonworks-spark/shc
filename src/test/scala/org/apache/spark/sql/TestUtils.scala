package org.apache.spark.sql

import java.nio.ByteBuffer
import java.io.{IOException, File}
import java.nio.ByteBuffer
import java.util

import org.apache.avro.generic.GenericData

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.google.common.io.Files

import scala.util.Random

object TestUtils {

  def generateRandomByteBuffer(rand: Random, size: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    val arrayOfBytes = new Array[Byte](size)
    rand.nextBytes(arrayOfBytes)
    bb.put(arrayOfBytes)
  }

  def generateRandomMap(rand: Random, size: Int): java.util.Map[String, Int] = {
    val jMap = new util.HashMap[String, Int]()
    for (i <- 0 until size) {
      jMap.put(rand.nextString(5), i)
    }
    jMap
  }

  def generateRandomArray(rand: Random, size: Int): util.ArrayList[Boolean] = {
    val vec = new util.ArrayList[Boolean]()
    for (i <- 0 until size) {
      vec.add(rand.nextBoolean())
    }
    vec
  }
}
