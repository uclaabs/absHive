package org.apache.hadoop.hive.ql.abm.udaf;


public abstract class UDAFComputation {
  
  public abstract void iterate(int index);

  public abstract void partialTerminate(int level, int start, int end);
  
  public abstract void partialUpdate(int level, int start, int end);

  public abstract void terminate();
  
  public abstract void unfold();


}
