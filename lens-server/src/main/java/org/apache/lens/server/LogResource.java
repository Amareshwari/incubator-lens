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
package org.apache.lens.server;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.error.LensServerErrorCode;
import org.apache.lens.server.util.UtilityMethods;

import lombok.extern.slf4j.Slf4j;

/**
 * The logs resource
 */
@Path("/logs")
@Slf4j
public class LogResource {

  /**
   * Tells whether log resource if up or not
   *
   * @return message
   */
  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getMessage() {
    return "Logs resource is up!";
  }

  /**
   * Get logs corresponding to logSegregrationStr
   *
   * @param logSegregrationStr log segregation string - can be request id for all requests; query handle for queries
   *
   * @return streaming log
   * @throws LensException 
   */
  @GET
  @Path("/{logSegregrationStr}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM})
  public Response getLogs(@PathParam("logSegregrationStr") String logSegregrationStr) throws LensException {
    String[] command = { "bash", "-c", "grep -e '" + logSegregrationStr + "' "
      + System.getProperty("lens.log.dir") + "/*.*" };
    log.info("Running command {}", Arrays.asList(command));
    final ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    final Process process;
    try {
      process = pb.start();
    } catch (IOException e) {
      throw new LensException(LensServerErrorCode.LOG_SERVING_ERROR.getValue(), e, e.getLocalizedMessage());
    }

    StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream os) throws IOException {
        UtilityMethods.pipe(process.getInputStream(), os);        
      }
    };
//    return Response.ok(stream).header("content-disposition", "attachment; filename = lenslog.log")
//      .type(MediaType.APPLICATION_OCTET_STREAM).build();
    return Response.ok(stream).type(MediaType.APPLICATION_OCTET_STREAM).build();
  }

}
