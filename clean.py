from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("Spark S3 Integration")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .getOrCreate()
    )
    tag = "python-polars"

    questions = spark.read.json(
        f"s3a://seasonal-schools-bedrock-data-us/input/{tag}/questions.json"
    )

    df = questions.withColumn("question", f.explode("items"))
    df = df.select("question.*")
    df = df.select("question_id", "body", "title", "link").withColumnRenamed(
        "body", "question"
    )
    df.show()

    answers = spark.read.json(
        f"s3a://seasonal-schools-bedrock-data-us/input/{tag}/answers.json"
    )

    dfa = answers.withColumn("answer", f.explode("items"))
    dfa = dfa.select("answer.*")
    dfa = dfa.select("question_id", "answer_id", "body").withColumnRenamed(
        "body", "answer"
    )
    dfa.show()

    joined = df.join(dfa, "question_id")
    joined.show()
    joined = joined.withColumn("question_id", f.col("question_id").cast("string"))
    joined = joined.withColumn("answer_id", f.col("answer_id").cast("string"))

    concatenated = joined.select(f.concat(*joined.columns).alias("data"))
    count = concatenated.count()
    concatenated.repartition(count).write.mode("overwrite").text(
        f"s3a://seasonal-schools-bedrock-data-us/cleaned/{tag}/"
    )
