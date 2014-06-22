package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;

public class SimulationResult {

  public ArrayList<IntArrayList> lastCondId = null; // filled by dispatch

  public final ArrayList<double[]> means = new ArrayList<double[]>();
  public final ArrayList<ArrayList<IntArrayList>> condIds = new ArrayList<ArrayList<IntArrayList>>();
  public ArrayList<double[][]> samples = new ArrayList<double[][]>();
  public Array2DRowRealMatrix sigma;

  @Override
  public SimulationResult clone() {
    SimulationResult ret = new SimulationResult();
    ret.means.addAll(means);
    ret.condIds.addAll(condIds);
    ret.sigma = sigma;
    return ret;
  }

  public double[] getSample(int idx) {
    double[] res = new double[sigma.getColumnDimension()];
    int pos = 0;
    for(double[] smpl : samples.get(idx)) {
      System.arraycopy(smpl, 0, res, pos, smpl.length);
      pos += smpl.length;
    }
    return res;
  }

  public double[] getMean() {
    double[] res = new double[sigma.getColumnDimension()];
    int pos = 0;
    for (double[] mean : means) {
      System.arraycopy(mean, 0, res, pos, mean.length);
      pos += mean.length;
    }
    return res;
  }

}
