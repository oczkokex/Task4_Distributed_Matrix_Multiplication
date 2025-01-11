import hazelcast
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import logging
import concurrent.futures

# Connect to Hazelcast cluster with specific name and IP
client = hazelcast.HazelcastClient(
    cluster_name='my-cluster',
    cluster_members=['192.168.1.55:5701']  # Replace with your Hazelcast cluster IP and port
)

print("Connected to Hazelcast cluster.")

# Function to generate random matrix
def generate_matrix(rows, cols, value_range=(1, 100)):
    return np.random.randint(value_range[0], value_range[1] + 1, size=(rows, cols))

# Function to multiply two matrix blocks
def multiply_blocks(block_a, block_b):
    return np.dot(block_a, block_b)

# Function to divide a matrix into smaller blocks
def divide_matrix(matrix, block_size):
    blocks = []
    n = matrix.shape[0]
    for i in range(0, n, block_size):
        for j in range(0, n, block_size):
            block = matrix[i:i + block_size, j:j + block_size]
            blocks.append(block)
    return blocks

# Function to assemble the matrix from blocks
def assemble_matrix(blocks, matrix_size, block_size):
    n = matrix_size
    result = np.zeros((n, n), dtype=int)
    block_idx = 0
    for i in range(0, n, block_size):
        for j in range(0, n, block_size):
            block = blocks[block_idx]
            block_idx += 1
            result[i:i + block.shape[0], j:j + block.shape[1]] = block
    return result

# Main distributed matrix multiplication function
def distributed_matrix_multiplication(client, matrix_a, matrix_b, block_size):
    n = matrix_a.shape[0]
    block_count = n // block_size

    # Divide matrices into blocks
    blocks_a = divide_matrix(matrix_a, block_size)
    blocks_b = divide_matrix(matrix_b.T, block_size)  # Transpose matrix B for easier block access

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for i in range(block_count):
            for j in range(block_count):
                block_a = blocks_a[i * block_count + j]
                block_b = blocks_b[j * block_count + i]  # Access corresponding transposed block
                future = executor.submit(multiply_blocks, block_a, block_b)
                futures.append(future)

        # Collect results
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    # Assemble the result matrix
    result_matrix = assemble_matrix(results, n, block_size)
    return result_matrix

# Initialize Hazelcast client and perform distributed matrix multiplication
def main():
    # Define matrix size and block size
    matrix_size = 5000
    block_size = 500

    # Generate random matrices
    matrix_a = generate_matrix(matrix_size, matrix_size)
    matrix_b = generate_matrix(matrix_size, matrix_size)

    # Perform distributed matrix multiplication
    result = distributed_matrix_multiplication(client, matrix_a, matrix_b, block_size)
    print("Distributed matrix multiplication complete!")

    # Shutdown Hazelcast client
    client.shutdown()

if __name__ == "__main__":
    main()
