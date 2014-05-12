package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;

public class GroupByOutput {

  public final ArrayList<Integer> keys;
  public final ArrayList<Integer> vals;
  public final int id;

  public GroupByOutput(ArrayList<Integer> keyCols, ArrayList<Integer> valCols, int idCol) {
    keys = keyCols;
    vals = valCols;
    id = idCol;
  }

}
