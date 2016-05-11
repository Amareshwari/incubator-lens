/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.error;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.lens.api.error.ErrorCollection;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.commons.lang.exception.ExceptionUtils;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {
  private final LogSegregationContext logContext;
  private final ErrorCollection errorCollection;

  @Context
  UriInfo uriInfo;

  public GenericExceptionMapper() {
    logContext = LensServices.get().getLogSegregationContext();
    errorCollection = LensServices.get().getErrorCollection();
  }

  @Override
  public Response toResponse(Exception exception) {
    LensErrorTO errorTO;
    Response.Status status;
    if (exception instanceof WebApplicationException) {
      status = Response.Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
    } else {
      status = Response.Status.INTERNAL_SERVER_ERROR;
    }
    String requestId = logContext.getLogSegragationId();
    // as only /queries and /preparedqueries are returning LensAPIResult this explicit check is done
    // once other api moves to return LensAPIResult, check needs to be updated accordingly
    // All api may not move to LensAPIResult, for example httpresultset result cannot be wrapped in LensAPIResult.
    if (uriInfo.getPath().endsWith("/queries") || uriInfo.getPath().endsWith("/preparedqueries")) {
      if (exception instanceof LensException) {
        LensException e = ((LensException)exception);
        e.buildLensErrorResponse(errorCollection, null, requestId);

        final LensAPIResult lensAPIResult = ((LensException)exception).getLensAPIResult();
        return Response.status(lensAPIResult.getHttpStatusCode()).entity(lensAPIResult).build();
      }
      if (exception instanceof WebApplicationException) {
        errorTO = LensErrorTO.composedOf(status.getStatusCode(), exception.getMessage(),
          ExceptionUtils.getStackTrace(exception));
      } else {
        errorTO = LensErrorTO.composedOf(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          "Internal server error" + (exception.getMessage() != null ? ":" + exception.getMessage() : ""),
          ExceptionUtils.getStackTrace(exception));
      }
      LensAPIResult lensAPIResult = LensAPIResult.composedOf(null, logContext.getLogSegragationId(), errorTO,
        status);
      return Response.status(lensAPIResult.getHttpStatusCode()).entity(lensAPIResult).build();
    } else {
      if (exception instanceof LensException) {
        LensException e = ((LensException)exception);
        e.buildLensErrorResponse(errorCollection, null, requestId);
        return Response.status(e.getLensAPIResult().getHttpStatusCode()).entity(exception.getMessage()).build();
      }
      return Response.status(status).entity(exception.getMessage()).build();
    }
  }
}
