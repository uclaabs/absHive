package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.datatypes.CondGroup;
import org.apache.hadoop.hive.ql.abm.datatypes.ConditionRange;

public class ConditionComputation extends UDAFComputation {

  private CondGroup condGroup = null;
  private List<List<ConditionRange>> rangeMatrix = null;
  private double[] newCondRanges = null;
  private List<Boolean> flags = null;
  private int dimension = 0;


  public void setCondGroup(CondGroup cond, int dimension) {
    this.condGroup = cond;
    this.dimension = dimension;
    newCondRanges = new double[dimension * 2];
  }

  public void setFields(IntArrayList keyArray, List<List<ConditionRange>> rangeMatrix) {
    this.rangeMatrix = rangeMatrix;
    this.condGroup.addGroup(keyArray);
  }
  

  @Override
  public void iterate(int index) {
  }

  
  @Override
  public void partialUpdate(int level, int start, int end) {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public void partialTerminate(int level, int start, int end) {
    
    boolean flag = this.flags.get(level);
    if (flag) {
      newCondRanges[level * 2] = this.rangeMatrix.get(level).get(start).getValue(flag);
      newCondRanges[level * 2 + 1] = (end == this.rangeMatrix.get(level).size()) ? Double.POSITIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(flag);
    } else {
      newCondRanges[level * 2] = (end == this.rangeMatrix.get(level).size()) ? Double.NEGATIVE_INFINITY
          : this.rangeMatrix.get(level).get(end).getValue(flag);
      newCondRanges[level * 2 + 1] = this.rangeMatrix.get(level).get(start).getValue(flag);
    }
    System.out.println("Range Added: " + newCondRanges[level * 2] + "\t" + newCondRanges[level * 2 + 1]);
  }

  @Override
  public void terminate() {
    for (int i = 0; i < this.dimension; i++) {
      this.condGroup.getRangeMatrix().get(i)
          .add(new ConditionRange(newCondRanges[2 * i], newCondRanges[2 * i + 1]));
    }
  }

  @Override
  public void unfold() {
    // unfold the conditions
    
    List<Integer> unfoldKeys = new ArrayList<Integer>();
    for(List<Integer> currentKeys: this.condGroup.getKeys())
      unfoldKeys.addAll(currentKeys);
    
    List<List<ConditionRange>> unfoldRangeMatrix  = new ArrayList<List<ConditionRange>>();
    for(int i = 0; i < this.dimension * this.condGroup.getGroupNumber(); i ++)
      unfoldRangeMatrix.add(new ArrayList<ConditionRange>());
    
    ConditionRange[] rangeArray = new ConditionRange[this.dimension * this.condGroup.getGroupNumber()];
    unfoldRangeMatrix(0, rangeArray, unfoldRangeMatrix);
    this.condGroup.clear();
    this.condGroup.addKeys(unfoldKeys);
    this.condGroup.addRanges(unfoldRangeMatrix);
  }
  
  private void unfoldRangeMatrix(int level, ConditionRange[] rangeArray, List<List<ConditionRange>> rangeMatrix)
  {
    boolean leaf = (level == this.condGroup.getGroupNumber() - 1);
    System.out.println("unfoldRangeMatrix :" + level + "\t" + leaf);
    
    List<List<ConditionRange>> currentRangeMatrix = this.condGroup.getRangeMatrix(level);
    int rowNumber = currentRangeMatrix.get(0).size();
    
    for(int i = 0; i < rowNumber; i ++)
    {
      for(int j = 0; j < this.dimension; j ++)
      {
        System.out.println("Add to rangeArray: " + (level * this.dimension + j) + "\t" + currentRangeMatrix.get(j).get(i).toString());
        rangeArray[level * this.dimension + j] = currentRangeMatrix.get(j).get(i);
      }
      
      if(leaf)
      {
        for(int dim = 0; dim < rangeArray.length; dim ++)
          rangeMatrix.get(dim).add(rangeArray[dim]);
      }
      else
      {
        unfoldRangeMatrix(level + 1, rangeArray, rangeMatrix);
      }
    }
  }

  public void setFlags(List<Boolean> flags) {
    this.flags = flags;
    
  }





}
