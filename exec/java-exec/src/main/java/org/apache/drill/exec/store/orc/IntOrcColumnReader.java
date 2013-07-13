package org.apache.drill.exec.store.orc;

import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class IntOrcColumnReader extends OrcColumnReader<LongColumnVector> {

  public IntOrcColumnReader(VectorHolder holder) {
    super(holder);
  }

  @Override
  public void parseNextBatch(long count, LongColumnVector vector) {
    NullableIntVector vv = (NullableIntVector) holder.getValueVector();
    NullableIntVector.Mutator mutator = vv.getMutator();
    long[] values = vector.vector;
    if(vector.noNulls) {
      for(int i = 0; i < count; i++) {
        mutator.set(i, (int)values[i]);
      }
    } else {
      boolean[] isNulls = vector.isNull;
      for(int i = 0; i < count; i++) {
        if(!isNulls[i]) {
          mutator.set(i, (int)values[i]);
        }
      }
    }
  }
}