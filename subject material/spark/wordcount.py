import sys
# sys provides varaibles for better control over inout and output
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

# Command-line arguments are those which are passed during the calling of the program along with the calling statement. To achieve this using the sys module, the sys module provides a variable called sys.argv. It’s main purpose are:
# It is a list of command-line arguments.
# len(sys.argv) provides the number of command-line arguments.
# sys.argv[0] is the name of the current Python script.
    
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()