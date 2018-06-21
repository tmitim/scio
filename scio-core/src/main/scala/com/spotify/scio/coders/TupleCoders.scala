/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// generated with tuplecoders.py

// scalastyle:off cyclomatic.complexity
// scalastyle:off file.size.limit
// scalastyle:off line.size.limit
// scalastyle:off method.length
// scalastyle:off number.of.methods
// scalastyle:off parameter.number

package com.spotify.scio.coders

trait TupleCoders {
  import Implicits.gen

  implicit def tuple2Coder[A: Coder, B: Coder]: Coder[(A, B)] = gen[(A, B)]
  implicit def tuple3Coder[A: Coder, B: Coder, C: Coder]: Coder[(A, B, C)] = gen[(A, B, C)]
  implicit def tuple4Coder[A: Coder, B: Coder, C: Coder, D: Coder]: Coder[(A, B, C, D)] = gen[(A, B, C, D)]
  implicit def tuple5Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder]: Coder[(A, B, C, D, E)] = gen[(A, B, C, D, E)]
  implicit def tuple6Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder]: Coder[(A, B, C, D, E, F)] = gen[(A, B, C, D, E, F)]
  implicit def tuple7Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder]: Coder[(A, B, C, D, E, F, G)] = gen[(A, B, C, D, E, F, G)]
  implicit def tuple8Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder]: Coder[(A, B, C, D, E, F, G, H)] = gen[(A, B, C, D, E, F, G, H)]
  implicit def tuple9Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder]: Coder[(A, B, C, D, E, F, G, H, I)] = gen[(A, B, C, D, E, F, G, H, I)]
  implicit def tuple10Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J)] = gen[(A, B, C, D, E, F, G, H, I, J)]
  implicit def tuple11Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K)] = gen[(A, B, C, D, E, F, G, H, I, J, K)]
  implicit def tuple12Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L)]
  implicit def tuple13Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M)]
  implicit def tuple14Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)]
  implicit def tuple15Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)]
  implicit def tuple16Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)]
  implicit def tuple17Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)]
  implicit def tuple18Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)]
  implicit def tuple19Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)]
  implicit def tuple20Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)]
  implicit def tuple21Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)]
  implicit def tuple22Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder]: Coder[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = gen[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)]
}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on method.length
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
