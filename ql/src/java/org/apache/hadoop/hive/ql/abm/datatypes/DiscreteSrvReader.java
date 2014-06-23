package org.apache.hadoop.hive.ql.abm.datatypes;

public class DiscreteSrvReader extends SrvReader {

  public DiscreteSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen/numCols;
  }

  @Override
  public void locate(double[] srv, int condId) {
    this.srv = srv;
    offset = condId * numCols;
  }

  @Override
  public void fillVar(boolean[] fake, double[][] dest, int pos) {
    for (int i = 0; i < numCols; ++i, ++pos) {
      dest[pos][pos] = FAKE_ZERO;
      fake[pos] = true;
    }
  }

}