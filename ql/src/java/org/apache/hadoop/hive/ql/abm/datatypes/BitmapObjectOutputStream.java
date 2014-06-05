package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.IOException;
import java.io.ObjectOutput;

public class BitmapObjectOutputStream implements ObjectOutput {
  byte[] buffer = null;
  int cursor = 0;
  
  public BitmapObjectOutputStream(int size) {
    buffer = new byte[size];
    cursor = 0;
  }
  
  public byte[] getBuffer() {
    return buffer;
  }
  
  @Override
  public void writeInt(int arg0) throws IOException {
    
    buffer[cursor] = (byte) (arg0 >> 24);
    buffer[cursor + 1] = (byte) (arg0 >> 16);
    buffer[cursor + 2] = (byte) (arg0 >> 8);
    buffer[cursor + 3] = (byte) (arg0);
    cursor += 4;
  }

  @Override
  public void writeLong(long arg0) throws IOException {
    
    for(int i = 0; i < 8; i ++) {
      buffer[cursor + i] = (byte) (arg0 >> (8 * (7 - i)));
    }
    
    cursor += 8;
  }
  
  @Override
  public void write(int arg0) throws IOException {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void write(byte[] arg0) throws IOException {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void write(byte[] arg0, int arg1, int arg2) throws IOException {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void writeObject(Object arg0) throws IOException {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void writeByte(int arg0) throws IOException {
    throw new UnsupportedOperationException();
    
  }

  @Override
  public void writeBytes(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }


  
  @Override
  public void writeDouble(double arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeFloat(float arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeShort(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeUTF(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChar(int arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeChars(String arg0) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void writeBoolean(boolean arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

}
