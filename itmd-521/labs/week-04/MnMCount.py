#Vinutha_Bengaluru_Prabhudev
import sys
from pyspark.sql import SparkSession

if __name__ =="_main_":
    if len(sys/argv) != 2:
        print("Usage mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession.builder.appName.("PythonMnMcount").getOrCreate())
    mnm_file = sys.argv[1]
    mnm_df=(spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(mnm_file))

    count_mnm_df = (mnm_df. select("State","Color","Count")
    .groupBy("State","Color")
    .sum("Count")
    .orderBy("sum(Count)", ascending=False))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d " %(count_mnm_df.count()))

    ca_count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .where(mnm_df.state=="CA")
        .groupBy("State","Color")
        .sum("Count")
        .orderBy("sum(Count)", ascending = False))


    ca_count-mnm_df.show(n=10, truncate=Falsespark)
    spark.stop()







