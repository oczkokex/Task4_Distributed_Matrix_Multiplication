package org.example;

import java.util.Random;

public class MatrixMultiplication {

    // Method to generate a matrix with random integers between 1 and 100
    public static int[][] generateMatrix(int rows, int cols) {
        Random random = new Random();
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(100) + 1; // Random numbers between 1 and 100
            }
        }
        return matrix;
    }

}
