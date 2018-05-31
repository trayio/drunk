/*
 * Copyright 2018 Facundo Viale
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jarlakxen.drunk

import io.circe.DecodingFailure

final case class GraphQLResponseError[T](errors: Seq[T], networkStatus: Int)

final case class GraphQLResponseData[T](data: T)

final case class ResponseDecodingException(message: String, cause: DecodingFailure) extends RuntimeException(message, cause)