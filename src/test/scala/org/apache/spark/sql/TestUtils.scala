package org.apache.spark.sql

import java.nio.ByteBuffer
import java.util.{ArrayList, HashMap}

import scala.util.Random

object TestUtils {

  def generateRandomByteBuffer(rand: Random, size: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    val arrayOfBytes = new Array[Byte](size)
    rand.nextBytes(arrayOfBytes)
    bb.put(arrayOfBytes)
  }

  def generateRandomMap(rand: Random, size: Int): java.util.Map[String, Int] = {
    val jMap = new HashMap[String, Int]()
    for (i <- 0 until size) {
      jMap.put(rand.nextString(5), i)
    }
    jMap
  }

  def generateRandomArray(rand: Random, size: Int): ArrayList[Boolean] = {
    val vec = new ArrayList[Boolean]()
    for (i <- 0 until size) {
      vec.add(rand.nextBoolean())
    }
    vec
  }
}
