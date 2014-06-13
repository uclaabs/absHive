package org.apache.hadoop.hive.ql.abm.simulation;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class KeyReaderSearchTest {
  
  public static int lessThan(int left, int right, DoubleArrayList range, double value) {
    while(right > left) {
      int midPos = (left + right) / 2; 
      if(range.getDouble(midPos) < value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }
  
  public static int lessEqualThan(int left, int right, DoubleArrayList range, double value) {
    while(right > left) {
      int midPos = (left + right) / 2; 
      if(range.getDouble(midPos) <= value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }
  
  public static int greaterEqualThan(int left, int right, DoubleArrayList range, double value) {
    while(right > left) {
      int midPos = (left + right) / 2; 
      if(range.getDouble(midPos) >= value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }
  
  public static int greaterThan(int left, int right, DoubleArrayList range, double value) {
    while(right > left) {
      int midPos = (left + right) / 2; 
      if(range.getDouble(midPos) > value) {
        right = midPos;
      } else {
        left = midPos + 1;
      }
    }
    return left;
  }
  
  public static void singleGreaterEqualThan(int left, int right, DoubleArrayList range, double value) {
    
    int[] bound = new int[2];
    bound[0] = left;
    bound[1] = right;
    
    // find the first value greaterOrEquanlTo value
    int index = greaterThan(left,right,range,value);
    if(range.getDouble(index) <= value) {
      bound[1] = index;
      bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if(index == left) {
        bound[0] = range.size(); 
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
    
    System.out.print("(" + bound[0] + "," + bound[1] + ")");
  }
  
  public static void singleGreaterThan(int left, int right, DoubleArrayList range, double value) {
    
    int[] bound = new int[2];
    bound[0] = left;
    bound[1] = right;
    
    // find the first value greaterOrEquanlTo value
    int index = greaterEqualThan(left,right,range,value);
    if(range.getDouble(index) < value) {
      bound[1] = index;
      bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if(index == left) {
        bound[0] = range.size(); 
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = greaterEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
    
    System.out.print("(" + bound[0] + "," + bound[1] + ")");
  }
  
  public static void singleLessEqualThan(int left, int right, DoubleArrayList range, double value) {
    
    int[] bound = new int[2];
    bound[0] = left;
    bound[1] = right;
    
    // find the first value greaterOrEquanlTo value
    int index = lessThan(left,right,range,value);
    if(range.getDouble(index) >= value) {
      bound[1] = index;
      bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if(index == left) {
        bound[0] = range.size(); 
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
    
    System.out.print("(" + bound[0] + "," + bound[1] + ")");
  }
  
  public static void singleLessThan(int left, int right, DoubleArrayList range, double value) {
    
    int[] bound = new int[2];
    bound[0] = left;
    bound[1] = right;
    
    // find the first value greaterOrEquanlTo value
    int index = lessEqualThan(left,right,range,value);
    if(range.getDouble(index) > value) {
      bound[1] = index;
      bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
    } else {
      if(index == left) {
        bound[0] = range.size(); 
        bound[1] = range.size();
      } else {
        bound[1] = index - 1;
        bound[0] = lessEqualThan(left, bound[1], range, range.getDouble(bound[1]));
      }
    }
    
    System.out.print("(" + bound[0] + "," + bound[1] + ")");
  }
  
  public static void testGreater(int left, int right, DoubleArrayList range, double value) {
    singleGreaterThan(left, right, range, value);
    System.out.print("\t");
    singleGreaterEqualThan(left, right, range, value);
    System.out.println();
  }
  
  public static void testLess(int left, int right, DoubleArrayList range, double value) {
    singleLessThan(left, right, range, value);
    System.out.print("\t");
    singleLessEqualThan(left, right, range, value);
    System.out.println();
  }

  public static void main(String[] args) {
    

    /*
     * Test Cases for Greater Than/Greater EqualThan 
     * Range: (2 2 3 3 4 5)
     * (left, right)-Target-Greater-GreaterEqual
     * (0 0)-1.5-(6,6)
     * (0 0)-2.5-(0,0) 
     * (1 2)-1.5-(6,6)
     * (1 2)-2.5-(1,1)
     * (1 2)-3.5-(2,2)
     * (0 3)-1.5-(6,6)
     * (0 3)-2.5-(0,1)
     * (0 3)-3.5-(2,3)
     * (0 5)-1.5-(6,6)
     * (0 5)-3-(0,1)-(2,3)
     * (0 5)-4-(2,3)-(4,4)
     * (0 5)-5-(4,4)-(5,5)
     */
    DoubleArrayList range = new DoubleArrayList();
    range.add(2);
    range.add(2);
    range.add(3);
    range.add(3);
    range.add(4);
    range.add(5);
    testGreater(0,0,range,1.5);
    testGreater(0,0,range,2.5);
    testGreater(1,2,range,1.5);
    testGreater(1,2,range,2.5);
    testGreater(1,2,range,3.5);
    testGreater(0,3,range,1.5);
    testGreater(0,3,range,2.5);
    testGreater(0,3,range,3.5);
    testGreater(0,5,range,1.5);
    testGreater(0,5,range,3);
    testGreater(0,5,range,4);
    testGreater(0,5,range,5);
    
   
    
    
    /*
     * Test Cases for Less Than/Less EqualThan 
     * Range: (5 5 4 4 3 2)
     * (left, right)-Target-Greater-GreaterEqual
     * (0 0)-5.5-(6,6)
     * (0 0)-4.5-(0,0) 
     * (1 2)-5.5-(6,6)
     * (1 2)-4.5-(1,1)
     * (1 2)-3.5-(2,2)
     * (0 3)-5.5-(6,6)
     * (0 3)-4.5-(0,1)
     * (0 3)-3.5-(2,3)
     * (0 5)-6.5-(6,6)
     * (0 5)-4-(0,1)-(2,3)
     * (0 5)-3-(2,3)-(4,4)
     * (0 5)-2-(4,4)-(5,5)
     */
//    DoubleArrayList range = new DoubleArrayList();
//    range.add(5);
//    range.add(5);
//    range.add(4);
//    range.add(4);
//    range.add(3);
//    range.add(2);
//    
//    testLess(0,0,range,5.5);
//    testLess(0,0,range,4.5);
//    testLess(1,2,range,5.5);
//    testLess(1,2,range,4.5);
//    testLess(1,2,range,3.5);
//    testLess(0,3,range,5.5);
//    testLess(0,3,range,4.5);
//    testLess(0,3,range,3.5);
//    testLess(0,5,range,6.5);
//    testLess(0,5,range,4);
//    testLess(0,5,range,3);
//    testLess(0,5,range,2);
 
  }
}
