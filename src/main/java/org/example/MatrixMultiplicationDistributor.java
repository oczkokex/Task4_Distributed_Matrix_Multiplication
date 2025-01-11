package org.example;

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.map.IMap;
import com.hazelcast.core.Hazelcast;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.CountDownLatch;

public class MatrixMultiplicationDistributor {

    private static HazelcastInstance hazelcastInstance;
    private static IMap<String, int[][]> resultMap;
    private static CountDownLatch latch;

    public static void main(String[] args) throws Exception {
        Config config = new Config();


        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.addSerializerConfig(new SerializerConfig()
                .setTypeClass(MatrixBlockMultiplicationTask.class)
                .setImplementation(new MatrixBlockMultiplicationTaskSerializer()));


        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("192.168.111.4");
        tcpIpConfig.addMember("192.168.111.83");

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        System.out.println("Hazelcast instance started.");


        waitForClusterToHaveTwoMembers();

        int matrixSize = 10000;
        int blockSize = 1000;
        int subBlockSize = 500;

        int[][] matrixA = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);
        int[][] matrixB = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);

        distributeBlocks(matrixA, matrixB, blockSize, subBlockSize);
    }

    private static void waitForClusterToHaveTwoMembers() throws InterruptedException {

        while (hazelcastInstance.getCluster().getMembers().size() != 2) {
            System.out.println("Waiting for exactly 2 members in cluster...");
            Thread.sleep(1000);
        }
        System.out.println("Cluster have 2 members. Procceding initializing the cluster.");
    }

    public static void distributeBlocks(int[][] matrixA, int[][] matrixB, int blockSize, int subBlockSize) throws Exception {
        resultMap = hazelcastInstance.getMap("resultMap");

        int blocks = matrixA.length / blockSize;

        latch = new CountDownLatch(blocks * blocks);

        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {
                int[][] blockA = extractBlock(matrixA, i * blockSize, j * blockSize, blockSize);
                int[][] blockB = extractBlock(matrixB, i * blockSize, j * blockSize, blockSize);

                Callable<int[][]> task = new MatrixBlockMultiplicationTask(blockA, blockB, subBlockSize);
                System.out.println("Submitting task for block (" + i + ", " + j + ")");
                Future<int[][]> future = hazelcastInstance.getExecutorService("blockExecutor").submit(task);

                resultMap.put(i + "_" + j, future.get());

                latch.countDown();
            }
        }

        latch.await();
        assembleMatrix(matrixA.length, blockSize);
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

    private static void assembleMatrix(int matrixSize, int blockSize) throws Exception {
        int[][] resultMatrix = new int[matrixSize][matrixSize];


        int blocks = matrixSize / blockSize;

        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {
                int[][] block = resultMap.get(i + "_" + j);
                for (int k = 0; k < block.length; k++) {
                    for (int l = 0; l < block[k].length; l++) {
                        int row = i * blockSize + k;
                        int col = j * blockSize + l;

                        if (row < matrixSize && col < matrixSize) {
                            resultMatrix[row][col] = block[k][l];
                        }
                    }
                }
            }
        }

        System.out.println("Matrix Mutliplication Complete!");
    }



}