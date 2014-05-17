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
    if (!AbmUtilities.inAbmMode()) {
      return pctx;
    }

    LineageCtx lctx = LineageProcFactory.extractLineage(pctx);
    FuncDepProcFactory.checkFuncDep(lctx);

    TraceProcCtx tctx = TraceProcFactory.trace(lctx);
    return RewriteProcFactory.rewritePlan(tctx);
  }

}
