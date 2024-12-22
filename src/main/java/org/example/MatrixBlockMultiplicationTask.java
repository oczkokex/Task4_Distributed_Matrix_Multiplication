package org.example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class MatrixBlockMultiplicationTask implements Callable<int[][]>, Serializable {
    private final int[][] matrixA;
    private final int[][] matrixB;
    private final int blockSize;

    // Constructor for initializing the task
    public MatrixBlockMultiplicationTask(int[][] matrixA, int[][] matrixB, int blockSize) {
        this.matrixA = matrixA;
        this.matrixB = matrixB;
        this.blockSize = blockSize;
    }

    // Perform the matrix block multiplication
    @Override
    public int[][] call() throws Exception {
        int[][] result = new int[blockSize][blockSize];
        for (int i = 0; i < blockSize; i++) {
            for (int j = 0; j < blockSize; j++) {
                for (int k = 0; k < blockSize; k++) {
                    result[i][j] += matrixA[i][k] * matrixB[k][j];
                }
            }
        }
        return result;
    }

    // Override toString for better debugging
    @Override
    public String toString() {
        return "MatrixBlockMultiplicationTask{blockSize=" + blockSize + ", matrixA=" + Arrays.toString(matrixA) + ", matrixB=" + Arrays.toString(matrixB) + "}";
    }
}
