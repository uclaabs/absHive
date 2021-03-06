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

package org.apache.hadoop.hive.ql.abm.udf;

public class SrvGreaterEqualSrv extends SrvCompareSrv {

  @Override
  protected void updateRet(int id1, int id2, double lower1, double lower2, double upper1,
      double upper2) {
    if (lower1 <= upper2) {
      ret.update(id1, id2, Double.NEGATIVE_INFINITY);
    } else {
      ret.update(id1, id2, 0);
    }
  }

  @Override
  protected String udfFuncName() {
    return "Srv_Greater_Equal_Srv";
  }

}
