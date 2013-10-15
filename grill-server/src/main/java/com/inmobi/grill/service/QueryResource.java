package com.inmobi.grill.service;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("/queryapi")
public class QueryResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String getClichedMessage() {
      // Return some cliched textual content
      return "Hello World!";
  }
}
