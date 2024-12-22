package org.example;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.*;
import com.hazelcast.map.IMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;

public class MatrixMultiplicationDistributor {

    private static HazelcastInstance hazelcastInstance;
    private static IMap<String, int[][]> resultMap;
    private static CountDownLatch latch;

    public static void main(String[] args) throws Exception {
        // Start Hazelcast instance
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        resultMap = hazelcastInstance.getMap("results");

        // Initialize latch to wait for 2 members (adjust as needed)
        latch = new CountDownLatch(2); // Wait for 2 members, change as necessary

        // Register a MembershipListener to track when new members join
        hazelcastInstance.getCluster().addMembershipListener(new MembershipListener() {

            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                latch.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                // Optionally handle member removal if needed
            }
        });

        // Wait until there are at least 2 members in the cluster
        latch.await();
        System.out.println("At least 2 members connected, starting tasks...");

        // Matrix size (5000x5000)
        int matrixSize = 5000;
        int blockSize = 500;
        int subBlockSize = 50;

        // Generate matrices
        int[][] matrixA = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);
        int[][] matrixB = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);

        // Divide matrices into blocks and distribute tasks
        distributeBlocks(matrixA, matrixB, blockSize, subBlockSize);
    }

    public static void distributeBlocks(int[][] matrixA, int[][] matrixB, int blockSize, int subBlockSize) throws Exception {
        int blocks = matrixA.length / blockSize;

        // Divide matrices into blocks of 500x500
        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {
                int[][] blockA = extractBlock(matrixA, i * blockSize, j * blockSize, blockSize);
                int[][] blockB = extractBlock(matrixB, i * blockSize, j * blockSize, blockSize);

                // Submit task to Hazelcast for each block
                Callable<int[][]> task = new MatrixBlockMultiplicationTask(blockA, blockB, subBlockSize);
                Future<int[][]> future = hazelcastInstance.getExecutorService("blockExecutor").submit(task);

                // Store result temporarily
                resultMap.put(i + "_" + j, future.get());
            }
        }

        // After all tasks complete, gather the final result
        System.out.println("Matrix multiplication distributed and tasks executed!");
    }

    private static int[][] extractBlock(int[][] matrix, int startRow, int startCol, int size) {
        int[][] block = new int[size][size];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                block[i][j] = matrix[startRow + i][startCol + j];
            }
        }
        return block;
    }
}