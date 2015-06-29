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

package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class LineageCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>> lineage =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>>();

  private final HashMap<Operator<? extends OperatorDesc>, HashSet<Operator<? extends OperatorDesc>>> annoSrc =
      new HashMap<Operator<? extends OperatorDesc>, HashSet<Operator<? extends OperatorDesc>>>();
  private final HashMap<Operator<? extends OperatorDesc>, HashSet<Operator<? extends OperatorDesc>>> condSrc =
      new HashMap<Operator<? extends OperatorDesc>, HashSet<Operator<? extends OperatorDesc>>>();

  private final ParseContext ctx;

  public LineageCtx(ParseContext pctx) {
    ctx = pctx;
  }

  public ParseContext getParseContext() {
    return ctx;
  }

  public void putLineage(Operator<? extends OperatorDesc> op, String internalName, ExprInfo ctx) {
    HashMap<String, ExprInfo> map = lineage.get(op);
    if (map == null) {
      map = new HashMap<String, ExprInfo>();
      lineage.put(op, map);
    }
    map.put(internalName, ctx);
  }

  public ExprInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    HashMap<String, ExprInfo> map = lineage.get(op);
    if (map != null) {
      return map.get(internalName);
    }
    return null;
  }

  public HashMap<String, ExprInfo> getLineages(Operator<? extends OperatorDesc> op) {
    return lineage.get(op);
  }

  public boolean isUncertain(Operator<? extends OperatorDesc> op) {
    return annoSrc.containsKey(op) || condSrc.containsKey(op);
  }

  public boolean isAnnotatedWithSrv(Operator<? extends OperatorDesc> op) {
    return annoSrc.containsKey(op);
  }

  public void addAnnoSrc(Operator<? extends OperatorDesc> op, Operator<? extends OperatorDesc> src) {
    HashSet<Operator<? extends OperatorDesc>> srcSet = annoSrc.get(op);
    if (srcSet == null) {
      srcSet = new HashSet<Operator<? extends OperatorDesc>>();
      annoSrc.put(op, srcSet);
    }
    srcSet.add(src);
  }

  public void addAnnoSrcs(Operator<? extends OperatorDesc> op, Set<Operator<? extends OperatorDesc>> srcs) {
    HashSet<Operator<? extends OperatorDesc>> srcSet = annoSrc.get(op);
    if (srcSet == null) {
      srcSet = new HashSet<Operator<? extends OperatorDesc>>();
      annoSrc.put(op, srcSet);
    }
    srcSet.addAll(srcs);
  }

  public Set<Operator<? extends OperatorDesc>> getAnnoSrcs(Operator<? extends OperatorDesc> op) {
    return annoSrc.get(op);
  }

  public void addCondSrc(Operator<? extends OperatorDesc> op, Operator<? extends OperatorDesc> src) {
    HashSet<Operator<? extends OperatorDesc>> srcSet = condSrc.get(op);
    if (srcSet == null) {
      srcSet = new HashSet<Operator<? extends OperatorDesc>>();
      condSrc.put(op, srcSet);
    }
    srcSet.add(src);
  }

  public void addCondSrcs(Operator<? extends OperatorDesc> op, Set<Operator<? extends OperatorDesc>> srcs) {
    HashSet<Operator<? extends OperatorDesc>> srcSet = condSrc.get(op);
    if (srcSet == null) {
      srcSet = new HashSet<Operator<? extends OperatorDesc>>();
      condSrc.put(op, srcSet);
    }
    srcSet.addAll(srcs);
  }

  public Set<Operator<? extends OperatorDesc>> getCondSrcs(Operator<? extends OperatorDesc> op) {
    return condSrc.get(op);
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();

    for (Entry<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>> entry : lineage.entrySet()) {
        ret.append(entry.getKey().toString());
        ret.append(":\n");
        ret.append(entry.getValue());
        ret.append("\n\n");
    }

    return ret.toString();
  }

}
