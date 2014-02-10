package com.inmobi.grill.metastore.model;


import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import lombok.Getter;

@XmlRootElement
public class Database {
  @XmlElement @Getter private String name;
  private boolean cascade;
  private boolean ignoreIfExisting;
  public Database() {

  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }


  public boolean getCascacde() {
    return cascade;
  }

  public void setCascade(boolean b){
    cascade = b;
  }

  public boolean getIgnoreIfExisting() {
    return ignoreIfExisting;
  }

  public void setIgnoreIfExisting(boolean b){
    ignoreIfExisting = b;
  }
}
