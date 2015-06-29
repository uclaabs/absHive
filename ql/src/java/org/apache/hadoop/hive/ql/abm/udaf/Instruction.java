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

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class Instruction {

  private int tmp = -1;
  private final IntArrayList groupInstruction = new IntArrayList();
  private final List<Merge> mergeInstruction = new ArrayList<Merge>();

  public void addGroupInstruction(int instruction) {
    groupInstruction.add(instruction);
  }

  public IntArrayList getGroupInstruction() {
    return groupInstruction;
  }

  public void addMergeInstruction(Merge instruction) {
    mergeInstruction.add(instruction);
  }

  public List<Merge> getMergeInstruction() {
    return mergeInstruction;
  }

  public void resetGroupInstruction() {
    groupInstruction.clear();
  }

  public void resetMergeInstruction() {
    mergeInstruction.clear();
  }

  public void fakeIterate() {
    tmp ++;
    if(tmp == 3) {
      tmp = -1;
    }
    groupInstruction.add(tmp);
  }

}
