package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;

public class GroupByResult {

  public final ArrayList<Integer> keys;
  public final ArrayList<Integer> vals;
  public final Integer id;

  public GroupByResult(ArrayList<Integer> keyCols, ArrayList<Integer> valCols, Integer idCol) {
    keys = keyCols;
    vals = valCols;
    assert idCol != null;
    id = idCol;
  }

}
