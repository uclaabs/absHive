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
