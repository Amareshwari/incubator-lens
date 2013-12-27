package com.inmobi.grill.quota.service;

import org.apache.hive.service.cli.CLIService;

import com.inmobi.grill.exception.GrillException;
import com.inmobi.grill.server.api.QuotaService;
import com.inmobi.grill.service.GrillService;

public class QuotaServiceImpl extends GrillService implements QuotaService {

  public QuotaServiceImpl(CLIService cliService) {
    super("quota", cliService);
  }

}
