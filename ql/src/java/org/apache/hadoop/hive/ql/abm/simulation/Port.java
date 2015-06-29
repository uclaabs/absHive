/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.abm.datatypes.SrvTuple;

public class Port {

  private final TupleMap[] reqDicts;
  private final PredicateSet[] predSets;

  private final TupleList[] reqTuples;
  private final IdxList[] reqIdxs;
  private final ArrayList<IntArrayList> reqCondIds;

  private final IntArrayList[] targets;
  private final int[] numAggrs;
  private final Offset[] offsets;
  private final Int2IntOpenHashMap[] uniqs;

  public Port(int[] reqGbys, TupleMap[] reqSrvs, PredicateSet[] predSets,
      IntArrayList[] targets, int[] numAggrs, Offset[] offsets) {
    reqDicts = new TupleMap[reqGbys.length];
    for (int i = 0; i < reqGbys.length; ++i) {
      reqDicts[i] = reqSrvs[reqGbys[i]];
    }
    this.predSets = predSets;

    // Initialize simulation-related fields
    reqTuples = new TupleList[reqGbys.length];
    reqIdxs = new IdxList[reqGbys.length];
    reqCondIds = new ArrayList<IntArrayList>(reqGbys.length);
    for (int i = 0; i < reqGbys.length; ++i) {
      reqTuples[i] = new TupleList();
      reqIdxs[i] = new IdxList();
      reqCondIds.add(new IntArrayList());
    }

    this.targets = targets;
    this.numAggrs = numAggrs;
    this.offsets = offsets;
    uniqs = new Int2IntOpenHashMap[numAggrs.length];
    for (int i = 0; i < numAggrs.length; ++i) {
      uniqs[i] = new Int2IntOpenHashMap();
    }
  }

  public int init(IntArrayList[] requests) {
    for (int i = 0; i < targets.length; ++i) {
      targets[i].clear();
      uniqs[i].clear();
    }

    for (int i = 0; i < requests.length; ++i) {
      IntArrayList target = requests[i];
      TupleMap map = reqDicts[i];
      TupleList list = reqTuples[i];
      PredicateSet predSet = predSets[i];
      list.clear();
      for (int j = 0; j < target.size(); ++j) {
        SrvTuple tuple = map.get(target.getInt(j));
        list.add(tuple);
        predSet.initDep(tuple.key, targets, uniqs);
      }
    }

    // preprocess offset
    int cum = 0;
    for (int i = 0; i < targets.length; ++i) {
      offsets[i].offset = cum;
      cum += uniqs[i].size() * numAggrs[i];
    }

    for (int i = 0; i < requests.length; ++i) {
      IntArrayList target = requests[i];
      TupleMap map = reqDicts[i];
      PredicateSet predSet = predSets[i];
      IdxList inds = reqIdxs[i];

      while (inds.size() < target.size()) {
        inds.add(new IntArrayList());
      }

      for (int j = 0; j < target.size(); ++j) {
        SrvTuple tuple = map.get(target.getInt(j));
        predSet.initIdx(tuple.key, uniqs, offsets, numAggrs, inds.get(j));
      }
    }

    return cum;
  }

  public ArrayList<IntArrayList> parse(double[] sample) {
    for (int i = 0; i < reqTuples.length; ++i) {
      TupleList list = reqTuples[i];
      IdxList inds = reqIdxs[i];
      PredicateSet predSet = predSets[i];

      IntArrayList res = reqCondIds.get(i);
      res.clear();
      for (int j = 0; j < list.size(); ++j) {
        res.add(predSet.parse(sample, inds.get(j), list.get(j).range));
      }
    }
    return reqCondIds;
  }

}

class TupleList extends ArrayList<SrvTuple> {

  private static final long serialVersionUID = 1L;

}

class IdxList extends ArrayList<IntArrayList> {

  private static final long serialVersionUID = 1L;

}
