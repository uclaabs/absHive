package org.apache.hadoop.hive.ql.abm.datatypes;


public class ContinuousSrvReader extends SrvReader {

  public ContinuousSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen / (numCols * 2);
  }

  @Override
  public void locate(double[] srv, int condId) {
    this.srv = srv;
    offset = condId * numCols * 2;
  }

  @Override
  public void fillVar(boolean[] fake, double[][] dest, int pos) {
    for (int i = offset + numCols, to = i + numCols; i < to; ++i, ++pos) {
      double v = srv[i];
      if (v != 0) {
        dest[pos][pos] = v;
        fake[pos] = false;
      } else {
        dest[pos][pos] = FAKE_ZERO;
        fake[pos] = true;
      }
    }
  }

}
