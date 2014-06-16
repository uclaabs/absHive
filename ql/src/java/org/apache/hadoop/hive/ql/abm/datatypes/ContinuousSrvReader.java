package org.apache.hadoop.hive.ql.abm.datatypes;

public class ContinuousSrvReader extends SrvReader {

  private static final long serialVersionUID = 1L;

  public ContinuousSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen/(numCols * 2);
  }

  @Override
  public void locate(double[] srv, int condId) {
    this.srv = srv;
    offset = condId * numCols * 2;
  }

  @Override
  public boolean fillVar(double[][] dest, int pos) {
    if (srv[offset + numCols] != 0) {
      for (int i = offset + numCols, to = i + numCols; i < to; ++i, ++pos) {
        dest[pos][pos] = srv[i];
      }
      return true;
    } else {
      for (int i = 0; i < numCols; ++i, ++pos) {
        dest[pos][pos] = 1;
      }
      return false;
    }
  }

}