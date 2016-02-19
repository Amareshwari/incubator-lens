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
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.api.result.LensErrorTO;
import org.apache.lens.server.LensServices;
import org.apache.lens.server.model.LogSegregationContext;

import org.apache.commons.lang.exception.ExceptionUtils;

@Provider
public class GenericErrorMapper implements ExceptionMapper<Throwable> {
  private final LogSegregationContext logContext = LensServices.get().getLogSegregationContext();
  @Override
  public Response toResponse(Throwable exception) {
    LensErrorTO errorTO;
    Response.Status status;
    if (exception instanceof WebApplicationException) {
      status = Response.Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
      errorTO = LensErrorTO.composedOf(status.getStatusCode(), exception.getMessage(),
        ExceptionUtils.getStackTrace(exception));
    } else {
      status = Response.Status.INTERNAL_SERVER_ERROR;
      errorTO = LensErrorTO.composedOf(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        "Internal server error" + (exception.getMessage() != null ? ":" + exception.getMessage() : ""),
        ExceptionUtils.getStackTrace(exception));
    }
    LensAPIResult lensAPIResult = LensAPIResult.composedOf(null, logContext.getLogSegragationId(), errorTO,
      status);
    return Response.status(lensAPIResult.getHttpStatusCode()).entity(lensAPIResult).build();

  }
}
