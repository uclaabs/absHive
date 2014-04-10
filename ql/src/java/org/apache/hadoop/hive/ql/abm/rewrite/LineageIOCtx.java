package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 *
 * LineageIOCtx:
 * 1. Only lineage readers which are GroupByOperator need to read tid
 * (actually other tuples need too, but since the correlation is so small, we omit it for now).
 * 2. Only GroupByOperator can broadcast lineage.
 *
 */
public class LineageIOCtx implements NodeProcessorCtx {

  private final LineageCtx lctx;

  private final HashMap<GroupByOperator, Long> broadcast =
      new HashMap<GroupByOperator, Long>();
  private final HashMap<GroupByOperator, ArrayList<Integer>> toWrite =
      new HashMap<GroupByOperator, ArrayList<Integer>>();

  private final HashSet<Operator<? extends OperatorDesc>> opsWithTid =
      new HashSet<Operator<? extends OperatorDesc>>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> tidCols =
      new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> lineageCols =
      new HashMap<Operator<? extends OperatorDesc>, Integer>();

  public LineageIOCtx(LineageCtx ctx, Set<LineageInfo> lineageToWrite,
      Set<Operator<? extends OperatorDesc>> lineageReaders) {
    lctx = ctx;
    populateLineageToWrite(lineageToWrite);
    findOperatorsWithTid(lineageReaders);
  }

  private void populateLineageToWrite(Set<LineageInfo> lineageToWrite) {
    for (LineageInfo lineageInfo : lineageToWrite) {
      GroupByOperator gby2 = lineageInfo.getGroupByOperator();
      // hack: get the first group by
      GroupByOperator gby1 = (GroupByOperator) gby2.getParentOperators().get(0)
          .getParentOperators().get(0);

      if (!broadcast.containsKey(gby2)) {
        broadcast.put(gby2, AbmUtilities.newBroadcastId());
      }
      ArrayList<Integer> valCols = toWrite.get(gby2);
      if (valCols == null) {
        valCols = new ArrayList<Integer>();
        toWrite.put(gby2, valCols);
        toWrite.put(gby1, valCols);
      }
      int index = lineageInfo.getIndex();
      if (index != -1 && valCols.indexOf(index) == -1) {
        valCols.add(index);
      }
    }
  }

  private void findOperatorsWithTid(Set<Operator<? extends OperatorDesc>> lineageReaders) {
    for (Operator<? extends OperatorDesc> reader : lineageReaders) {
      if (reader instanceof GroupByOperator) {
        GroupByOperator gby = (GroupByOperator) reader;
        backtrace(gby);
      }
    }
  }

  private boolean backtrace(Operator<? extends OperatorDesc> op) {
    boolean addTid = false;

    List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
    if (parents != null) {
      for (Operator<? extends OperatorDesc> parent : parents) {
        boolean branchAddTid = backtrace(parent);
        addTid = (branchAddTid || addTid);
      }
      if (op instanceof GroupByOperator) {
        addTid = false;
      }
    } else {
      if (lctx.isSampled(op)) {
        addTid = true;
      }
    }

    if (addTid) {
      opsWithTid.add(op);
    }
    return addTid;
  }

  public LineageCtx getLineageCtx() {
    return lctx;
  }

  public long getBroadcastId(GroupByOperator op) {
    return broadcast.get(op);
  }

  public boolean hasLineageToWrite(GroupByOperator gby) {
    return toWrite.containsKey(gby);
  }

  public ArrayList<Integer> getValColsToWrite(GroupByOperator op) {
    return toWrite.get(op);
  }

  public void putValColsToWrite(GroupByOperator op, ArrayList<Integer> valCols) {
    toWrite.put(op, valCols);
  }

  public boolean withTid(Operator<? extends OperatorDesc> op) {
    return opsWithTid.contains(op);
  }

  public Integer getTidColumn(Operator<? extends OperatorDesc> op) {
    return tidCols.get(op);
  }

  public void putTidColumn(Operator<? extends OperatorDesc> op, Integer index) {
    tidCols.put(op, index);
  }

  public Integer getLineageColumn(Operator<? extends OperatorDesc> op) {
    return lineageCols.get(op);
  }

  public void putLineageColumn(Operator<? extends OperatorDesc> op, Integer index) {
    lineageCols.put(op, index);
  }

  public ParseContext getParseContext() {
    return lctx.getParseContext();
  }

}
