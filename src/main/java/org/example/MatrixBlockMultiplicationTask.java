package org.example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class MatrixBlockMultiplicationTask implements Callable<int[][]>, Serializable { // Implementa Serializable
    final int[][] blockA;
    final int[][] blockB;
    final int subBlockSize;

    public MatrixBlockMultiplicationTask(int[][] blockA, int[][] blockB, int subBlockSize) {
        this.blockA = blockA;
        this.blockB = blockB;
        this.subBlockSize = subBlockSize;
    }

    @Override
    public int[][] call() throws Exception {
        return multiplyBlocks(blockA, blockB);
    }

    private int[][] multiplyBlocks(int[][] blockA, int[][] blockB) {
        int size = blockA.length;
        int[][] result = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < size; k++) {
                    result[i][j] += blockA[i][k] * blockB[k][j];
                }
            }
        }

        return result;
    }
}