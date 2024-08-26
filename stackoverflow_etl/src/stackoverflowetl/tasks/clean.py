import argparse

from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from stackoverflowetl.common.spark import ClosableSparkSession


def clean(spark: SparkSession, environment: str, tag: str):
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

    joined = df.join(dfa, "question_id")
    joined = joined.withColumn("question_id", f.col("question_id").cast("string"))
    joined = joined.withColumn("answer_id", f.col("answer_id").cast("string"))

    concatenated = joined.select(f.concat(*joined.columns).alias("data"))
    count = concatenated.count()
    concatenated.repartition(count).write.mode("overwrite").text(
        f"s3a://seasonal-schools-bedrock-data-us/cleaned/{tag}/"
    )


def main():
    parser = argparse.ArgumentParser(description="stackoverflow_etl")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    args = parser.parse_args()

    with ClosableSparkSession("stackoverflow_etl") as session:
        clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
