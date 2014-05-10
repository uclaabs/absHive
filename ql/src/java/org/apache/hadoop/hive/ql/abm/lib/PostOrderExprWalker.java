package org.apache.hadoop.hive.ql.abm.lib;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * PostOrderExprWalker does post order traversal.
 * Unlike DefaultGraphWalker, each node may be visited by PostOrder Walker multiple times.
 *
 */
public class PostOrderExprWalker extends DefaultGraphWalker {

  public PostOrderExprWalker(Dispatcher disp) {
    super(disp);
  }

  @Override
  public void walk(Node nd) throws SemanticException {
    opStack.push(nd);

    if (nd.getChildren() != null) {
      for (Node n : nd.getChildren()) {
        walk(n);
      }
    }

    dispatch(nd, opStack);
    opStack.pop();
  }

}
