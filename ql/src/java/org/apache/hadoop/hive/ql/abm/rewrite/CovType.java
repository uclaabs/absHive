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

package org.apache.hadoop.hive.ql.abm.rewrite;

public enum CovType {
  INNER_SUM_SUM,
  INNER_SUM_COUNT,
  INNER_SUM_AVG,
  INNER_COUNT_SUM,
  INNER_COUNT_COUNT,
  INNER_COUNT_AVG,
  INNER_AVG_SUM,
  INNER_AVG_COUNT,
  INNER_AVG_AVG,

  INTER_SUM_SUM,
  INTER_SUM_COUNT,
  INTER_SUM_AVG,
  INTER_COUNT_SUM,
  INTER_COUNT_COUNT,
  INTER_COUNT_AVG,
  INTER_AVG_SUM,
  INTER_AVG_COUNT,
  INTER_AVG_AVG;

  private static final CovType[][] inners = new CovType[][] {
      {INNER_SUM_SUM, INNER_SUM_COUNT, INNER_SUM_AVG},
      {INNER_COUNT_SUM, INNER_COUNT_COUNT, INNER_COUNT_AVG},
      {INNER_AVG_SUM, INNER_AVG_COUNT, INNER_AVG_AVG}
  };

  private static final CovType[][] inters = new CovType[][] {
      {INTER_SUM_SUM, INTER_SUM_COUNT, INTER_SUM_AVG},
      {INTER_COUNT_SUM, INTER_COUNT_COUNT, INTER_COUNT_AVG},
      {INTER_AVG_SUM, INTER_AVG_COUNT, INTER_AVG_AVG}
  };

  public static CovType getCovType(boolean inner, UdafType lhs, UdafType rhs) {
    if (inner) {
      return inners[lhs.index][rhs.index];
    } else {
      return inters[lhs.index][rhs.index];
    }
  }

}
