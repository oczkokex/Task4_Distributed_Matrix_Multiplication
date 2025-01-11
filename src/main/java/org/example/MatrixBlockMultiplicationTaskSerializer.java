package org.example;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import java.io.IOException;

public class MatrixBlockMultiplicationTaskSerializer implements StreamSerializer<MatrixBlockMultiplicationTask> {

    private static final long serialVersionUID = 4575958307612750595L;

    @Override
    public void write(ObjectDataOutput out, MatrixBlockMultiplicationTask object) throws IOException {
        out.writeInt(object.blockA.length);
        out.writeInt(object.blockA[0].length);
        for (int i = 0; i < object.blockA.length; i++) {
            for (int j = 0; j < object.blockA[i].length; j++) {
                out.writeInt(object.blockA[i][j]);
            }
        }

        out.writeInt(object.blockB.length);
        out.writeInt(object.blockB[0].length);
        for (int i = 0; i < object.blockB.length; i++) {
            for (int j = 0; j < object.blockB[i].length; j++) {
                out.writeInt(object.blockB[i][j]);
            }
        }

        out.writeInt(object.subBlockSize);
    }

    @Override
    public MatrixBlockMultiplicationTask read(ObjectDataInput in) throws IOException {
        // matrix A
        int rowsA = in.readInt();
        int colsA = in.readInt();
        int[][] blockA = new int[rowsA][colsA];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsA; j++) {
                blockA[i][j] = in.readInt();
            }
        }

        int rowsB = in.readInt();
        int colsB = in.readInt();
        int[][] blockB = new int[rowsB][colsB];
        for (int i = 0; i < rowsB; i++) {
            for (int j = 0; j < colsB; j++) {
                blockB[i][j] = in.readInt();
            }
        }

        int subBlockSize = in.readInt();


        return new MatrixBlockMultiplicationTask(blockA, blockB, subBlockSize);
    }

    @Override
    public int getTypeId() {
        return 1;
    }

    @Override
    public void destroy() {
    }
}