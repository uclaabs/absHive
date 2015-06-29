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

package org.apache.hadoop.hive.ql.abm;

import org.apache.hadoop.hive.ql.abm.funcdep.FuncDepProcFactory;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.abm.lineage.LineageProcFactory;
import org.apache.hadoop.hive.ql.abm.rewrite.RewriteProcFactory;
import org.apache.hadoop.hive.ql.abm.rewrite.TraceProcCtx;
import org.apache.hadoop.hive.ql.abm.rewrite.TraceProcFactory;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AbmRewriter implements Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    LineageCtx lctx = LineageProcFactory.extractLineage(pctx);
    FuncDepProcFactory.checkFuncDep(lctx);

    TraceProcCtx tctx = TraceProcFactory.trace(lctx);
    return RewriteProcFactory.rewritePlan(tctx);
  }

}
