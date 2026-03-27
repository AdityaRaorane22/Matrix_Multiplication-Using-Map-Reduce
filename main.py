from collections import defaultdict

# Mapper function
def mapper(A, B):
    mapped = []

    rows_A = len(A)
    cols_A = len(A[0])
    cols_B = len(B[0])

    # Emit values from A
    for i in range(rows_A):
        for j in range(cols_A):
            for k in range(cols_B):
                # Key = (i, k)
                mapped.append(((i, k), ('A', j, A[i][j])))

    # Emit values from B
    for j in range(len(B)):
        for k in range(cols_B):
            for i in range(rows_A):
                # Key = (i, k)
                mapped.append(((i, k), ('B', j, B[j][k])))

    return mapped


# Reducer function
def reducer(mapped):
    grouped = defaultdict(list)

    # Group by key
    for key, value in mapped:
        grouped[key].append(value)

    result = {}

    # Compute multiplication
    for key in grouped:
        values = grouped[key]

        A_dict = {}
        B_dict = {}

        for val in values:
            matrix_name, j, v = val
            if matrix_name == 'A':
                A_dict[j] = v
            else:
                B_dict[j] = v

        total = 0
        for j in A_dict:
            if j in B_dict:
                total += A_dict[j] * B_dict[j]

        result[key] = total

    return result


# Convert result to matrix
def format_result(result, rows, cols):
    matrix = [[0 for _ in range(cols)] for _ in range(rows)]
    for (i, k), val in result.items():
        matrix[i][k] = val
    return matrix


# Example matrices (any size)
A = [
    [1, 2, 3],
    [4, 5, 6]
]

B = [
    [7, 8],
    [9, 10],
    [11, 12]
]

# Run MapReduce
mapped = mapper(A, B)
result = reducer(mapped)

final_matrix = format_result(result, len(A), len(B[0]))

# Output
print("Result Matrix:")
for row in final_matrix:
    print(row)
