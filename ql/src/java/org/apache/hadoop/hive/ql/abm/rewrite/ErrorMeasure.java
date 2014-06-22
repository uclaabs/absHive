package org.apache.hadoop.hive.ql.abm.rewrite;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public enum ErrorMeasure {
  MEAN,
  VARIANCE,
  QUANTILE,
  CONF_INV;

  private static ErrorMeasure[] dict = new ErrorMeasure[] {MEAN, VARIANCE, QUANTILE, CONF_INV};

  public static ErrorMeasure get(int type) throws SemanticException {
    if (type < 0 || type >= dict.length) {
      AbmUtilities.report(ErrorMsg.INVALID_MEASURE);
    }
    return dict[type];
  }
}
