package com.inmobi.yoda.cube.ddl;

import com.inmobi.grill.exception.GrillException;
import org.apache.hadoop.conf.Configuration;

public class CubeSelectorFactory {
  private static CubeSelectorService instance;

  /**
   * Return cube selector service instance
   * @param conf
   * @param createNew if true, a new instance is created, otherwise a cached instance is returned
   * @return
   */
  public synchronized static CubeSelectorService getSelectorSvcInstance(Configuration conf, boolean createNew)
    throws GrillException {
    if (createNew) {
      return new CubeSelectorServiceImpl(conf);
    }

    if (instance == null) {
      instance = new CubeSelectorServiceImpl(conf);
    }

    return instance;
  }

  /**
   * Returned cubs selector service instance. Instance will be cached statically, so that subsequent calls of this
   * method will return the same instance
   * @param conf
   * @return
   */
  public static CubeSelectorService getSelectorSvcInstance(Configuration conf) throws GrillException {
    return getSelectorSvcInstance(conf, false);
  }
}
