package com.inmobi.grill.cli.commands;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.client.GrillClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

@Component
public class GrillCubeCommands implements CommandMarker {
  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }

  @CliCommand(value = "show cubes", help = "show list of cubes in database")
  public String showCubes() {
    List<String> cubes = client.getAllCubes();
    return Joiner.on("\n").join(cubes);
  }

  @CliCommand(value = "create cube", help = "Create a new Cube")
  public String createCube(@CliOption(key = {"", "table"},
      mandatory = true, help = "<cube-spec>") String cubeSpec) {
    File f = new File(cubeSpec);

    if (!f.exists()) {
      return "cube spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }
    APIResult result = client.createCube(cubeSpec);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create cube succeeded";
    } else {
      return "create cube failed";
    }
  }

  @CliCommand(value = "drop cube", help = "drop cube")
  public String dropCube(@CliOption(key = {"", "table"},
      mandatory = true, help = "cube name to be dropped") String cube) {
    APIResult result = client.dropCube(cube);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + cube + "!!!";
    } else {
      return "Dropping cube failed";
    }
  }


  @CliCommand(value = "update cube", help = "update cube")
  public String updateCube(@CliOption(key = {"", "cube"}, mandatory = true, help = "<cube-name> <cube-spec>") String specPair) {
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }

    APIResult result = client.updateCube(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  @CliCommand(value = "describe cube", help = "describe cube")
  public String describeCube(@CliOption(key = {"", "cube"},
      mandatory = true, help = "<cube-name>") String cubeName) {

    XCube cube = client.getCube(cubeName);
    StringBuilder builder = new StringBuilder();
    builder.append("Cube Name : ").append(cube.getName()).append("\n");
    builder.append("Description : ").append(cube.getDescription() != null ? cube.getDescription() : "");
    if (cube.getMeasures() != null) {
      builder.append("Measures :").append("\n");
      builder.append("\t").append("name").append("\t").append("type").append("\t")
          .append("cost").append("\t").append("format string").append("\t")
          .append("unit").append("\t").append("starttime(in miliseconds)")
          .append("\t").append("endtime(in miliseconds)").append("\n");
      for (XMeasure measure : cube.getMeasures().getMeasures()) {
        builder.append("\t").append(measure.getName()!= null ? measure.getName(): "").append("\t")
            .append(measure.getType()!= null ? measure.getType() : "").append("\t")
            .append(measure.getCost()).append("\t")
            .append(measure.getFormatString()!=null ? measure.getFormatString(): "").append("\t")
            .append(measure.getUnit()!=null ? measure.getUnit(): "").append("\t")
            .append(measure.getStartTime() != null ?measure.getStartTime().toGregorianCalendar().getTimeInMillis(): "").append("\t")
            .append(measure.getEndTime()!= null? measure.getEndTime().toGregorianCalendar().getTimeInMillis(): "").append("\t")
            .append("\n");
      }
    }
    if (cube.getDimensions() != null) {
      builder.append("Dimensions  :").append("\n");
      builder.append("\t").append("name").append("\t").append("type").append("\t")
          .append("cost").append("\t").append("Expression").append("\t")
          .append("table references").append("\t").append("starttime(in miliseconds)")
          .append("\t").append("endtime(in miliseconds)").append("\n");
      for (XDimension dim : cube.getDimensions().getDimensions()) {
        builder.append("\t")
            .append(dim.getName()!=null ? dim.getName() : "").append("\t")
            .append(dim.getType()!=null? dim.getType(): "").append("\t")
            .append(dim.getCost()!= null ? dim.getCost() : "").append("\t")
            .append(dim.getExpr()!= null ? dim.getExpr() : "").append("\t")
            .append(dim.getReferences()!= null? getXtableString(dim.getReferences().getTableReferences()) : "")
            .append(dim.getStartTime()!=null ? dim.getStartTime().toGregorianCalendar().getTimeInMillis(): "").append("\t")
            .append(dim.getEndTime()!=null?dim.getEndTime().toGregorianCalendar().getTimeInMillis():"").append("\t")
            .append("\n");
      }
    }
    builder.append(FormatUtils.formatProperties(cube.getProperties().getProperties()));
    return builder.toString();

  }

  private String getXtableString(List<XTablereference> tableReferences) {
    StringBuilder builder = new StringBuilder();
    for (XTablereference ref : tableReferences) {
      builder.append(ref.getDestTable())
          .append(".")
          .append(ref.getDestColumn())
          .append(",");
    }
    return builder.toString();
  }
}
