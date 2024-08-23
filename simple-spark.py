import dask
from dask import delayed
import re
from functools import reduce

class RDD:
    def __init__(self, data):
        self.data = data
        self.transformations = []

    def map(self, func):
        self.transformations.append(('map', func))
        return self

    def filter(self, func):
        self.transformations.append(('filter', func))
        return self

    def flatMap(self, func):
        self.transformations.append(('flatMap', func))
        return self

    def collect(self):
        result = self.data
        for name, func in self.transformations:
            if name == 'map':
                result = [delayed(func)(x) for x in result]
            elif name == 'filter':
                result = [x for x in result if delayed(func)(x).compute()]
            elif name == 'flatMap':
                result = [delayed(func)(x) for x in result]
                result = [item for sublist in result for item in sublist]
        return dask.compute(*result)

    def count(self):
        return len(self.collect())

    def reduce(self, func):
        return reduce(func, self.collect())


class SparkContextWithDask:
    def __init__(self):
        pass

    def parallelize(self, data):
        return RDD([delayed(x) for x in data])


def read_file_in_chunks(file_path, chunk_size=1024):
    with open(file_path, 'r') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def word_count(file_path):
    sc = SparkContextWithDask()

    # Read the file in chunks and parallelize the data
    chunks = list(read_file_in_chunks(file_path))
    rdd = sc.parallelize(chunks)

    # Perform the word count
    word_counts = rdd.flatMap(lambda chunk: re.findall(r'\w+', chunk.lower())) \
                     .map(lambda word: (word, 1)) \
                     .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # Collect and print the results
    result = word_counts
    print(f"Total words counted: {result[1]}")


if __name__ == "__main__":
    file_path = "sample.txt"
    word_count(file_path)
