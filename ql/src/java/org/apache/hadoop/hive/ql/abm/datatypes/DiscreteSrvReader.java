package org.apache.hadoop.hive.ql.abm.datatypes;

public class DiscreteSrvReader extends SrvReader {

  private static final long serialVersionUID = 1L;

  public DiscreteSrvReader(int numCols) {
    super(numCols);
  }

  public int getNumCondition(int bufLen) {
    return bufLen/numCols;
  }

  @Override
  public void locate(double[] srv, int condId) {
    offset = condId * numCols;
  }

  @Override
  public boolean fillVar(double[][] dest, int pos) {
    if (srv[offset] != 0) {
      for (int i = offset, to = i + numCols; i < to; ++i, ++pos) {
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