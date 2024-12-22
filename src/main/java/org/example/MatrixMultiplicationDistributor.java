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

        // Configuración de serialización personalizada
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.addSerializerConfig(new SerializerConfig()
                .setTypeClass(MatrixBlockMultiplicationTask.class)
                .setImplementation(new MatrixBlockMultiplicationTaskSerializer()));

        // Configuración de red
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false); // Desactivar multicast

        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("192.168.111.4"); // IP del primer equipo
        tcpIpConfig.addMember("192.168.111.83"); // IP del segundo equipo

        // Crear instancia de Hazelcast
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        System.out.println("Hazelcast instance started.");

        // Esperar a que el clúster tenga exactamente 2 miembros
        waitForClusterToHaveTwoMembers();

        int matrixSize = 4;
        int blockSize = 2;
        int subBlockSize = 2;

        // Generar matrices
        int[][] matrixA = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);
        int[][] matrixB = MatrixMultiplication.generateMatrix(matrixSize, matrixSize);

        // Dividir matrices en bloques y distribuir tareas
        distributeBlocks(matrixA, matrixB, blockSize, subBlockSize);
    }

    private static void waitForClusterToHaveTwoMembers() throws InterruptedException {
        // Esperar hasta que el clúster tenga exactamente 2 miembros
        while (hazelcastInstance.getCluster().getMembers().size() != 2) {
            System.out.println("Esperando a que haya exactamente 2 miembros en el clúster...");
            Thread.sleep(1000); // Esperar un segundo antes de verificar nuevamente
        }
        System.out.println("El clúster tiene 2 miembros. Procediendo con la distribución de tareas.");
    }

    public static void distributeBlocks(int[][] matrixA, int[][] matrixB, int blockSize, int subBlockSize) throws Exception {
        resultMap = hazelcastInstance.getMap("resultMap");

        int blocks = matrixA.length / blockSize;

        // Inicializar el CountDownLatch con el número correcto de tareas
        latch = new CountDownLatch(blocks * blocks);

        // Dividir las matrices en bloques
        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {
                int[][] blockA = extractBlock(matrixA, i * blockSize, j * blockSize, blockSize);
                int[][] blockB = extractBlock(matrixB, i * blockSize, j * blockSize, blockSize);

                // Enviar tarea a Hazelcast para cada bloque
                Callable<int[][]> task = new MatrixBlockMultiplicationTask(blockA, blockB, subBlockSize);
                System.out.println("Submitting task for block (" + i + ", " + j + ")");
                Future<int[][]> future = hazelcastInstance.getExecutorService("blockExecutor").submit(task);

                // Almacenar el resultado temporalmente
                resultMap.put(i + "_" + j, future.get());

                // Disminuir el contador del latch cuando la tarea termine
                latch.countDown();
            }
        }

        // Esperar a que todas las tareas terminen
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
        int[][] resultMatrix = new int[matrixSize][matrixSize]; // Tamaño final de la matriz

        // Ensamblar la matriz a partir de los bloques
        int blocks = matrixSize / blockSize;

        for (int i = 0; i < blocks; i++) {
            for (int j = 0; j < blocks; j++) {
                int[][] block = resultMap.get(i + "_" + j);
                for (int k = 0; k < block.length; k++) {
                    for (int l = 0; l < block[k].length; l++) {
                        // Asegúrate de que los índices no excedan matrixSize
                        int row = i * blockSize + k;
                        int col = j * blockSize + l;

                        if (row < matrixSize && col < matrixSize) { // Comprobación para evitar desbordamiento
                            resultMatrix[row][col] = block[k][l];
                        }
                    }
                }
            }
        }

        // Mostrar el resultado de la multiplicación de matrices
        System.out.println("Matrix Mutliplication Complete!");
    }



}