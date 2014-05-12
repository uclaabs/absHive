package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.algebra.Transform;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ConditionAnnotation implements Comparator<GroupByOperator> {

  private final static HashMap<GroupByOperator, Operator<? extends OperatorDesc>> lastUsedBy =
      new HashMap<GroupByOperator, Operator<? extends OperatorDesc>>();

  private final HashMap<GroupByOperator, Integer> positions =
      new HashMap<GroupByOperator, Integer>();
  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final TreeSet<GroupByOperator> topLevel = new TreeSet<GroupByOperator>(this);
  private final ArrayList<Transform> transforms = new ArrayList<Transform>();
  private final HashMap<GroupByOperator, GroupByOperator[]> dependencies =
      new HashMap<GroupByOperator, GroupByOperator[]>();

  public void groupByAt(GroupByOperator gby) {
    if (!topLevel.isEmpty()) {
      dependencies.put(gby, topLevel.toArray(new GroupByOperator[topLevel.size()]));
      topLevel.clear();
    }
  }

  public void addTransform(Transform trans) {
    transforms.add(trans);
  }

  public void conditionOn(AggregateInfo aggr) {
    GroupByOperator gby = aggr.getGroupByOperator();
    TreeSet<AggregateInfo> buf = aggregates.get(gby);
    if (buf == null) {
      buf = new TreeSet<AggregateInfo>();
      aggregates.put(gby, buf);

      positions.put(gby, positions.size());
      topLevel.add(gby);
    }
    buf.add(aggr);
  }

  public void useAt(GroupByOperator gby, Operator<? extends OperatorDesc> op) {
    lastUsedBy.put(gby, op);
  }

  public static Operator<? extends OperatorDesc> lastUsedBy(GroupByOperator gby) {
    return lastUsedBy.get(gby);
  }

  @SuppressWarnings("unchecked")
  public void combine(ConditionAnnotation other) {
    for (GroupByOperator gby : other.positions.keySet()) {
      assert !positions.containsKey(gby);
      positions.put(gby, positions.size());
    }

    for (Map.Entry<GroupByOperator, TreeSet<AggregateInfo>> entry : other.aggregates.entrySet()) {
      aggregates.put(entry.getKey(), (TreeSet<AggregateInfo>) entry.getValue().clone());
    }

    topLevel.addAll(other.topLevel);

    transforms.addAll(other.transforms);

    for (Map.Entry<GroupByOperator, GroupByOperator[]> entry : other.dependencies.entrySet()) {
      dependencies.put(entry.getKey(), entry.getValue().clone());
    }
  }

  public int getInputSize(GroupByOperator gby) {
    int sz = gby.getConf().getKeys().size();
    for (AggregateInfo ai : aggregates.get(gby)) {
      if (!ai.getUdafType().equals(UdafType.COUNT)) {
        ++sz;
      }
    }
    // For tid
    ++sz;
    return sz;
  }

  public int getOutputSize(GroupByOperator gby) {
    int sz = gby.getConf().getKeys().size();
    for (AggregateInfo ai : aggregates.get(gby)) {
      ++sz;
    }
    // For the mandatory count(*)
    --sz;
    // For condition, lineage, group-by-id
    sz += 3;
    return sz;
  }

  public Set<GroupByOperator> getAllGroupByOps() {
    return positions.keySet();
  }

  public void check() {
    for (Map.Entry<GroupByOperator, TreeSet<AggregateInfo>> entry : aggregates.entrySet()) {
      GroupByOperator gby = entry.getKey();
      TreeSet<AggregateInfo> aggrs = entry.getValue();
      assert gby.getConf().getAggregators().size() + 1 == aggrs.size();
    }
  }

  @Override
  public int compare(GroupByOperator o1, GroupByOperator o2) {
    return positions.get(o1) - positions.get(o2);
  }

}
