package org.apache.hadoop.hive.ql.abm.rewrite;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.lib.TopologicalSort;
import org.apache.hadoop.hive.ql.abm.simulation.PredicateType;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ConditionAnnotation {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> aggregates =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<GroupByOperator, ComparisonTransform[]> dependencies =
      new HashMap<GroupByOperator, ComparisonTransform[]>();
  private final ArrayList<ComparisonTransform> transforms = new ArrayList<ComparisonTransform>();

  private final HashSet<GroupByOperator> continuous = new HashSet<GroupByOperator>();
  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  private final List<String> cachedOutputs = new ArrayList<String>();
  private final List<String> cachedInputs = new ArrayList<String>();

  private List<List<GroupByOperator>> sorted = null;
  private List<GroupByOperator> sortedContinuous = null;
  private List<GroupByOperator> sortedDiscrete = null;
  private Dictionary<GroupByOperator> gbyDict = null;

  // <-- Used by TraceProcCtx

  public void groupByAt(GroupByOperator gby, boolean isContinuous) {
    dependencies.put(gby, transforms.toArray(new ComparisonTransform[transforms.size()]));
    transforms.clear();
    if (isContinuous) {
      continuous.add(gby);
    }
  }

  public void conditionOn(ComparisonTransform trans) {
    transforms.add(trans);
    for (AggregateInfo ai : trans.getAggregatesInvolved()) {
      GroupByOperator gby = ai.getGroupByOperator();
      TreeSet<AggregateInfo> buf = aggregates.get(gby);
      if (buf == null) {
        buf = new TreeSet<AggregateInfo>();
        aggregates.put(gby, buf);
      }
      buf.add(ai);
    }
  }

  public void returnVal(AggregateInfo ai) {
    GroupByOperator gby = ai.getGroupByOperator();
    TreeSet<AggregateInfo> buf = aggregates.get(gby);
    if (buf == null) {
      buf = new TreeSet<AggregateInfo>();
      aggregates.put(gby, buf);
    }
    buf.add(ai);
  }

  public void combine(ConditionAnnotation other) {
    aggregates.putAll(other.aggregates);
    transforms.addAll(other.transforms);
    dependencies.putAll(other.dependencies);
    continuous.addAll(other.continuous);
  }

  // -->

  // <-- Used by RewriteProcCtx

  // A query is either a simple query, in which case we do not need to cache any input;
  // or is a complex query, in which case we have to cache the inputs of all continuous gbys.
  // This is because the dependency graph is a connected tree for complex queries, and thus
  // every continuous gby is connected with another.
  public boolean isSimpleQuery() {
    if (!AbmUtilities.isCovarianceNegligible()) {
      return false;
    }

    for (ComparisonTransform[] val : dependencies.values()) {
      if (val.length != 0) {
        return false;
      }
    }

    HashSet<AggregateInfo> hash = new HashSet<AggregateInfo>();
    for (ComparisonTransform ct : transforms) {
      hash.addAll(ct.getAggregatesInvolved());
    }
    if (hash.size() <= 2) {
      // It must contain COUNT(*) > 0
      return true;
    }

    return false;
  }

  public boolean isContinuous(GroupByOperator gby) {
    return continuous.contains(gby);
  }

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    String tableName = AbmUtilities.ABM_CACHE_INPUT_PREFIX + gby.toString();
    input.getConf().cache(tableName,
        AbmUtilities.fixSerDe(input.getSchema().getSignature()));
    inputs.put(gby, input);
    cachedInputs.add(tableName);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    String tableName = AbmUtilities.ABM_CACHE_OUTPUT_PREFIX + gby.toString();
    output.getConf().cache(tableName,
        AbmUtilities.fixSerDe(output.getSchema().getSignature()));
    outputs.put(gby, output);
    cachedOutputs.add(tableName);
  }

  public int getAggregateId(AggregateInfo ai) {
    if (sorted == null) {
      setupIds();
    }
    List<GroupByOperator> gbys = sorted.get(sorted.size() - 1);
    int idx = gbys.indexOf(ai.getGroupByOperator());

    int cum = 0;
    for (int i = 0; i < idx; ++i) {
      cum += aggregates.get(gbys.get(i)).size();
    }
    if (ai.getIndex() != -1) {
      cum += ai.getIndex();
    } else {
      cum += aggregates.get(gbys.get(idx)).size() - 1;
    }

    return cum;
  }

  private void setupIds() {
    Map<GroupByOperator, Set<GroupByOperator>> map = getDependencyGraph();
    sorted = TopologicalSort.getOrderByLevel(map);

    sortedContinuous = new ArrayList<GroupByOperator>();
    sortedDiscrete = new ArrayList<GroupByOperator>();
    for (List<GroupByOperator> level : sorted) {
      for (GroupByOperator gby : level) {
        if (continuous.contains(gby)) {
          sortedContinuous.add(gby);
        } else {
          sortedDiscrete.add(gby);
        }
      }
    }

    // Assign ids to GroupByOperators
    List<GroupByOperator> gbys = new ArrayList<GroupByOperator>();
    gbys.addAll(sortedContinuous);
    gbys.addAll(sortedDiscrete);
    gbyDict = new Dictionary<GroupByOperator>(gbys);
  }

  public void setupMCSim(SelectOperator select, int[] aggrColIdxs) {
    if (sorted == null) {
      setupIds();
    }

    // Rearrange the connections
    List<Operator<? extends OperatorDesc>> inputOps = new ArrayList<Operator<? extends OperatorDesc>>();
    boolean simpleQuery = inputs.isEmpty();
    if (!simpleQuery) {
      for (GroupByOperator gby : sortedContinuous) {
        inputOps.add(inputs.get(gby));
      }
    }
    List<Operator<? extends OperatorDesc>> outputContinuousOps = new ArrayList<Operator<? extends OperatorDesc>>();
    for (GroupByOperator gby : sortedContinuous) {
      outputContinuousOps.add(outputs.get(gby));
    }
    List<Operator<? extends OperatorDesc>> outputDiscreteOps = new ArrayList<Operator<? extends OperatorDesc>>();
    for (GroupByOperator gby : sortedDiscrete) {
      outputDiscreteOps.add(outputs.get(gby));
    }

    List<Operator<? extends OperatorDesc>> parents = select.getParentOperators();
    parents.addAll(inputOps);
    parents.addAll(outputContinuousOps);
    parents.addAll(outputDiscreteOps);
    for (Operator<? extends OperatorDesc> p : inputOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputContinuousOps) {
      p.getChildOperators().add(select);
    }
    for (Operator<? extends OperatorDesc> p : outputDiscreteOps) {
      p.getChildOperators().add(select);
    }

    // Continuous input (no input cached for discrete GBYs): keys, vals, tid
    // Continuous output:
    // 1. in simple queries: aggregates, condition, group-id
    // 2. in complex queries: keys, aggregates, lineage, condition, group-id
    List<Integer> numKeysContinuous = new ArrayList<Integer>();
    List<List<UdafType>> aggrTypes = new ArrayList<List<UdafType>>();
    for (GroupByOperator gby : sortedContinuous) {
      numKeysContinuous.add(gby.getConf().getKeys().size());
      List<UdafType> types = new ArrayList<UdafType>();
      for (AggregateInfo ai : aggregates.get(gby)) {
        types.add(ai.getUdafType());
      }
      aggrTypes.add(types);
    }

    // Discrete output: aggregates, condition, group-id
    List<Integer> numKeysDiscrete = new ArrayList<Integer>();
    for (GroupByOperator gby : sortedDiscrete) {
      numKeysDiscrete.add(gby.getConf().getKeys().size());
    }

    // GBYs' dependency structure
    int numLevels = sorted.size();

    int[][] gbyIds = new int[numLevels][];
    UdafType[][][] udafTypes = new UdafType[numLevels][][];

    for (int i = 0; i < numLevels; ++i) {
      List<GroupByOperator> level = sorted.get(i);

      int[] gbyIds2 = new int[level.size()];
      UdafType[][] udafTypes2 = new UdafType[level.size()][];

      int pos = 0;
      for (GroupByOperator gby : level) {
        gbyIds2[pos] = gbyDict.get(gby);
        List<UdafType> udafType1 = new ArrayList<UdafType>();
        for (AggregateInfo ai : aggregates.get(gby)) {
          udafType1.add(ai.getUdafType());
        }
        udafTypes2[pos++] = udafType1.toArray(new UdafType[udafType1.size()]);
      }

      gbyIds[i] = gbyIds2;
      udafTypes[i] = udafTypes2;
    }

    // Predicates
    int[][][] gbyIdsInPreds = new int[numLevels][][];
    int[][][] colsInPreds = new int[numLevels][][];
    PredicateType[][][] predTypes = new PredicateType[numLevels][][];

    for (int i = 0; i < numLevels; ++i) {
      List<GroupByOperator> level = sorted.get(i);
      List<GroupByOperator> parentLevel = null;
      if (i != 0) {
        parentLevel = sorted.get(i - 1);
      } else {
        parentLevel = new ArrayList<GroupByOperator>();
      }

      int[][] gbyIdsInPreds2 = new int[level.size()][];
      int[][] colsInPreds2 = new int[level.size()][];
      PredicateType[][] predTypes2 = new PredicateType[level.size()][];

      int j = 0;
      for (GroupByOperator gby : level) {
        IntArrayList gbyIdsInPreds1 = new IntArrayList();
        IntArrayList colsInPreds1 = new IntArrayList();
        List<PredicateType> predTypes1 = new ArrayList<PredicateType>();

        // unfold it to array
        for (ComparisonTransform transform : dependencies.get(gby)) {
          for (AggregateInfo ai : transform.getAggregatesInvolved()) {
            gbyIdsInPreds1.add(parentLevel.indexOf(ai.getGroupByOperator()));
            colsInPreds1.add(getValidIndex(ai));
          }
          predTypes1.add(transform.getPredicateType());
        }

        gbyIdsInPreds2[j] = gbyIdsInPreds1.toIntArray();
        colsInPreds2[j] = colsInPreds1.toIntArray();
        predTypes2[j] = predTypes1.toArray(new PredicateType[predTypes1.size()]);
        ++j;
      }

      gbyIdsInPreds[i] = gbyIdsInPreds2;
      colsInPreds[i] = colsInPreds2;
      predTypes[i] = predTypes2;
    }

    // Last condition
    IntArrayList gbyIdsInPorts1 = new IntArrayList();
    IntArrayList colsInPorts1 = new IntArrayList();
    ArrayList<PredicateType> predTypesInPorts1 = new ArrayList<PredicateType>();

    List<GroupByOperator> last = sorted.get(numLevels - 1);
    for (ComparisonTransform transform : transforms) {
      for (AggregateInfo ai : transform.getAggregatesInvolved()) {
        gbyIdsInPorts1.add(last.indexOf(ai.getGroupByOperator()));
        colsInPorts1.add(getValidIndex(ai));
      }
      predTypesInPorts1.add(transform.getPredicateType());
    }

    int[][] gbyIdsInPorts = new int[][] {gbyIdsInPorts1.toIntArray()};
    int[][] colsInPorts = new int[][] {colsInPorts1.toIntArray()};
    PredicateType[][] predTypesInPorts = new PredicateType[][] {
        predTypesInPorts1.toArray(new PredicateType[predTypesInPorts1.size()])};

    select.getConf().setMCSim(numKeysContinuous, aggrTypes, numKeysDiscrete, cachedOutputs,
        cachedInputs, simpleQuery, gbyIds, udafTypes, gbyIdsInPreds, colsInPreds, predTypes,
        gbyIdsInPorts, colsInPorts, predTypesInPorts, aggrColIdxs,
        AbmUtilities.getNumSimulationSamples());
  }

  private int getValidIndex(AggregateInfo ai) {
    int idx = ai.getIndex();
    if (idx == -1) {
      idx = aggregates.get(ai.getGroupByOperator()).size() - 1;
    }
    return idx;
  }

  private Map<GroupByOperator, Set<GroupByOperator>> getDependencyGraph() {
    Map<GroupByOperator, Set<GroupByOperator>> map =
        new HashMap<GroupByOperator, Set<GroupByOperator>>();
    for (Map.Entry<GroupByOperator, ComparisonTransform[]> entry : dependencies.entrySet()) {
      Set<GroupByOperator> parents = new HashSet<GroupByOperator>();
      for (ComparisonTransform trans : entry.getValue()) {
        for (AggregateInfo ai : trans.getAggregatesInvolved()) {
          parents.add(ai.getGroupByOperator());
        }
      }
      map.put(entry.getKey(), parents);
    }
    return map;
  }

  public List<Boolean> getCondFlags(GroupByOperator gby) {
    ComparisonTransform[] trans = dependencies.get(gby);
    List<Boolean> flags = new ArrayList<Boolean>(trans.length);
    for (ComparisonTransform tran : trans) {
      flags.add(tran.isAscending());
    }
    return flags;
  }

  // -->

}

class Dictionary<T> {

  private HashMap<T, Integer> elem2Id = null;
  private ArrayList<T> id2Elem = null;

  public Dictionary(List<T> elements) {
    elem2Id = new HashMap<T, Integer>(elements.size());
    id2Elem = new ArrayList<T>(elements.size());

    for (T elem : elements) {
      elem2Id.put(elem, elem2Id.size());
      id2Elem.add(elem);
    }
  }

  public T get(int index) {
    return id2Elem.get(index);
  }

  public int get(T element) {
    return elem2Id.get(element);
  }

  public int size() {
    return id2Elem.size();
  }

}
