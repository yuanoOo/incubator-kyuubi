/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package org.apache.kyuubi.server.trino.api

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Context, MediaType, Response}
import javax.ws.rs.ext.{ExceptionMapper, Provider}

import org.eclipse.jetty.server.handler.ContextHandler

import org.apache.kyuubi.server.KyuubiTrinoFrontendService

private[api] trait ApiRequestContext {

  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  final protected def fe: KyuubiTrinoFrontendService = FrontendServiceContext.get(servletContext)
}

@Provider
class RestExceptionMapper extends ExceptionMapper[Exception] {
  override def toResponse(exception: Exception): Response = {
    exception match {
      case e: WebApplicationException =>
        Response.status(e.getResponse.getStatus)
          .`type`(e.getResponse.getMediaType)
          .entity(e.getMessage)
          .build()
      case e =>
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .`type`(MediaType.APPLICATION_JSON)
          .entity(e.getMessage)
          .build()
    }
  }
}

private[api] object FrontendServiceContext {

  private val attribute = getClass.getCanonicalName

  def set(contextHandler: ContextHandler, fe: KyuubiTrinoFrontendService): Unit = {
    contextHandler.setAttribute(attribute, fe)
  }

  def get(context: ServletContext): KyuubiTrinoFrontendService = {
    context.getAttribute(attribute).asInstanceOf[KyuubiTrinoFrontendService]
  }
}
