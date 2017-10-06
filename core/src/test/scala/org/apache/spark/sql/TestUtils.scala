/*
 * (C) 2017 Hortonworks, Inc. All rights reserved. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. This file is licensed to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
