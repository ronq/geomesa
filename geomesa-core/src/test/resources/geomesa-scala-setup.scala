/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
val cl = getClass.getClassLoader
def mywrap[T](action: => T) = { Thread.currentThread.setContextClassLoader(cl); action }
def time[A](a: => A) = { val now = System.nanoTime
val result = a
println("%f seconds".format( (System.nanoTime - now) / 1000000000.0 ))
result }
*/

