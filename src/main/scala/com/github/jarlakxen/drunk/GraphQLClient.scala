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

import scala.concurrent.{ExecutionContext, Future}
import scala.util._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import backend.{AkkaHttpBackend, GraphQLBackend}
import extensions.{GraphQLExtensions, NoExtensions}
import io.circe.Decoder.Result
import io.circe._
import io.circe.parser._
import sangria._
import sangria.ast.Document
import sangria.introspection._
import sangria.marshalling.circe._
import sangria.parser.{QueryParser, SyntaxError}

import scala.collection.immutable

class GraphQLClient private[GraphQLClient] (uri: Uri, options: ClientOptions, backend: GraphQLBackend) {
  import GraphQLClient._

  private[drunk] def execute[Res, Vars](doc: Document, variables: Option[Vars], name: Option[String])(
    implicit
    variablesEncoder: Encoder[Vars],
    ec: ExecutionContext): Future[(Int, Json)] =
    execute(GraphQLOperation(doc, variables, name))

  private[drunk] def execute[Res, Vars](op: GraphQLOperation[Res, Vars])(
    implicit
    ec: ExecutionContext): Future[(Int, Json)] = {

    val fields =
      List("query" -> op.docToJson) ++
        op.encodeVariables.map("variables" -> _) ++
        op.name.map("operationName" -> Json.fromString(_))

    val body = Json.obj(fields: _*).noSpaces

    for {
      (statusCode, rawBody) <- backend.send(uri, body)
      jsonBody <- Future.fromTry(parse(rawBody) match {
        case Right(b) => Success(b)
        case Left(a)  => Failure(a)
      })
    } yield (statusCode, jsonBody)
  }

  def query[Res, Err](doc: String)(implicit dec: Decoder[Res], decErr: Decoder[Err], ec: ExecutionContext): Try[GraphQLCursor[Res, Err, Nothing]] =
    query(doc, None, None)(dec, decErr, null, ec)

  def query[Res, Err](
    doc: String,
    operationName: String)(implicit dec: Decoder[Res], decErr: Decoder[Err], ec: ExecutionContext): Try[GraphQLCursor[Res, Err, Nothing]] =
    query(doc, None, Some(operationName))(dec, decErr, null, ec)

  def query[Res, Err, Vars](doc: String, variables: Vars)(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): Try[GraphQLCursor[Res, Err, Vars]] =
    query(doc, Some(variables), None)

  def query[Res, Err, Vars](doc: String, variables: Option[Vars], operationName: Option[String])(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): Try[GraphQLCursor[Res, Err, Vars]] =
    QueryParser.parse(doc).map(query(_, variables, operationName))

  def query[Res, Err](doc: Document)(implicit dec: Decoder[Res], decErr: Decoder[Err], ec: ExecutionContext): GraphQLCursor[Res, Err, Nothing] =
    query(doc, None, None)(dec, decErr, null, ec)

  def query[Res, Err](
    doc: Document,
    operationName: String)(implicit dec: Decoder[Res], decErr: Decoder[Err], ec: ExecutionContext): GraphQLCursor[Res, Err, Nothing] =
    query(doc, None, Some(operationName))(dec, decErr, null, ec)

  def query[Res, Err, Vars](doc: Document, variables: Vars)(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): GraphQLCursor[Res, Err, Vars] =
    query(doc, Some(variables), None)

  def query[Res, Err, Vars](doc: Document, variables: Option[Vars], operationName: Option[String])(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): GraphQLCursor[Res, Err, Vars] = {
    var fullDoc = doc
    if (options.addTypename) {
      fullDoc = ast.addTypename(doc)
    }

    val operation: GraphQLOperation[Res, Vars] = GraphQLOperation(doc, variables, operationName)
    val result = execute(operation)
    val data: Future[GraphQLClient.GraphQLResponse[Res, Err]] = result.flatMap { case (status, body) => Future.fromTry(extractErrorOrData[Res, Err](body, status)) }
    val extensions = result.map { case (_, body) => extractExtensions(body) }
    new GraphQLCursor(this, data, extensions, operation)
  }

  def mutate[Res, Err, Vars](doc: Document, variables: Vars)(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): Future[GraphQLResponse[Res, Err]] =
    mutate(doc, Some(variables), None)

  def mutate[Res, Err, Vars](doc: Document, variables: Vars, operationName: Option[String])(
    implicit
    dec: Decoder[Res],
    decErr: Decoder[Err],
    en: Encoder[Vars],
    ec: ExecutionContext): Future[GraphQLResponse[Res, Err]] = {

    val result = execute(doc, Some(variables), operationName)
    result.flatMap { case (status, body) => Future.fromTry(extractErrorOrData[Res, Err](body, status)) }
  }

  def schema(implicit ec: ExecutionContext): Future[IntrospectionSchema] =
    execute[Json, Nothing](introspectionQuery, None, None)(null, ec)
      .flatMap {
        case (_, json) => Future.fromTry(IntrospectionParser.parse(json))
      }

}

object GraphQLClient {

  type GraphQLResponse[Res, Err] = Either[GraphQLResponseError[Err], GraphQLResponseData[Res]]

  def apply(uri: String, backend: GraphQLBackend, clientOptions: ClientOptions): GraphQLClient =
    new GraphQLClient(Uri(uri), clientOptions, backend)

  def apply(uri: String, options: ConnectionOptions = ConnectionOptions.Default, clientOptions: ClientOptions = ClientOptions.Default): GraphQLClient =
    new GraphQLClient(Uri(uri), clientOptions, AkkaHttpBackend(options))

  def apply(actorSystem: ActorSystem, uri: String, connectionOptions: ConnectionOptions, clientOptions: ClientOptions): GraphQLClient =
    new GraphQLClient(Uri(uri), clientOptions, AkkaHttpBackend.usingActorSystem(actorSystem, connectionOptions))

  private[GraphQLClient] def extractErrors[Err](body: Json, statusCode: Int)(implicit dec: Decoder[Err]): Option[Try[GraphQLResponseError[Err]]] = {
    val cursor: HCursor = body.hcursor

    for {
      errorsNode <- cursor.downField("errors").focus
      errors <- errorsNode.asArray
    } yield {
      val errorsDecoded: immutable.Seq[Result[Err]] = errors.map(_.as[Err])
      val firstFailed: Option[DecodingFailure] = errorsDecoded.find(_.isLeft).map(_.left.get)
      firstFailed match {
        case Some(failed) => Failure(ResponseDecodingException(s"Failed to decode at least one error object in: ${errorsNode.noSpaces}", failed))
        case None => Success(GraphQLResponseError(errorsDecoded.map(_.right.get), statusCode))
      }
    }
  }

  private[GraphQLClient] def extractData[Res](jsonBody: Json)(implicit dec: Decoder[Res]): Try[GraphQLResponseData[Res]] = {
    val cursor = jsonBody.hcursor.downField("data")
    (cursor.as[Res] match {
      case Right(data) => Success(data)
      case Left(failure)  => Failure(ResponseDecodingException(s"Failed to decode data: ${cursor.focus.map(_.noSpaces)}", failure))
    }).map(GraphQLResponseData(_))
  }

  private[GraphQLClient] def extractErrorOrData[Res, Err](jsonBody: Json, statusCode: Int)(implicit dec: Decoder[Res], decErr: Decoder[Err]): Try[GraphQLResponse[Res, Err]] = {
    def errors: Option[Try[GraphQLResponse[Res, Err]]] =
      extractErrors[Err](jsonBody, statusCode).map(errors => errors.map(Left(_)))
    def data: Try[GraphQLResponse[Res, Err]] =
      extractData[Res](jsonBody).map(Right(_))

    errors.getOrElse(data)
  }

  private[GraphQLClient] def extractExtensions(jsonBody: Json): GraphQLExtensions =
    (jsonBody
      .hcursor
      .downField("extensions")
      .as[GraphQLExtensions] match {
      case Right(b) => Some(b)
      case _        => None
    })
      .getOrElse(NoExtensions)

}
