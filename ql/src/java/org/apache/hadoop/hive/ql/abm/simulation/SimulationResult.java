package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;

public class SimulationResult {

  public int dimension = 0;
  public final ArrayList<double[]> means = new ArrayList<double[]>(); // TODO
  public final ArrayList<IntArrayList> condIds = new ArrayList<IntArrayList>();
  public final ArrayList<double[][]> samples = new ArrayList<double[][]>();
  public Array2DRowRealMatrix ivSigma;

  @Override
  public SimulationResult clone() {
    SimulationResult ret = new SimulationResult();
    ret.dimension = dimension;
    ret.condIds.addAll(condIds);
    ret.ivSigma = ivSigma;
    return ret;
  }

  public double[] getSampleArray(int indi, int indj) {
    double[] res;
    int resLen = 0;
    double[][] sampleMatrix = samples.get(indi);

    for(int i = 0; i < indj; ++ i) {
      resLen += sampleMatrix[i].length;
    }
    res = new double[resLen];

    int ind = 0;
    for(int i = 0; i < indj; ++ i) {
      double[] sampleArray = sampleMatrix[i];
      for(double sample:sampleArray) {
        res[ind ++] =sample;
      }
    }

    return res;
  }

}
