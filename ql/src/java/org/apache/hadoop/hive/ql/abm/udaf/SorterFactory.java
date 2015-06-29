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

package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import org.apache.hadoop.hive.ql.abm.datatypes.RangeList;

public class SorterFactory {

  public static Sorter getSorter(RangeList conditions, boolean flag) {
    assert !conditions.isEmpty();
    if (flag) {
      return new AscendSorter(conditions);
    } else {
      return new DescendSorter(conditions);
    }
  }

  public static abstract class Sorter implements IntComparator, Swapper {
    public abstract IntArrayList getIndexes();
  }

  private static class AscendSorter extends Sorter {

    private final IntArrayList indexes;
    private final RangeList conditions;

    public AscendSorter(RangeList conditions) {
      indexes = new IntArrayList(conditions.size());

      for (int i = 0; i < conditions.size(); ++i) {
        indexes.add(i);
      }

      this.conditions = conditions;
    }

    @Override
    public int compare(Integer arg0, Integer arg1) {
      return compare(arg0.intValue(), arg1.intValue());
    }

    @Override
    public int compare(int arg0, int arg1) {
      return Double.compare(
          conditions.get(indexes.getInt(arg0)),
          conditions.get(indexes.getInt(arg1)));
    }

    @Override
    public void swap(int arg0, int arg1) {
      int tmpId = indexes.get(arg0);
      indexes.set(arg0, indexes.get(arg1));
      indexes.set(arg1, tmpId);

    }

    @Override
    public IntArrayList getIndexes() {
      return indexes;
    }

  }

  private static class DescendSorter extends AscendSorter {

    public DescendSorter(RangeList conditions) {
      super(conditions);
    }

    @Override
    public int compare(int arg0, int arg1) {
      return -super.compare(arg0, arg1);
    }

  }

}
