import os
import argparse
import logging
import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)


def clean(spark: SparkSession, environment: str, tag: str):
    bucket = "dataminded-academy-capstone-llm-data-us"
    input_folder = os.path.join(bucket, "input")

    tags = ["airflow", "apache-spark", "dbt", "docker", "pyspark", "python-polars", "sql"]
    
    for tag in tags:
        questions_raw = spark.read.json(os.path.join(f"s3a://{input_folder}", tag, "questions.json"))
        questions_expl = questions_raw.withColumn("item", psf.explode(psf.col("items")))
        questions = (
            questions_expl.select(
                psf.col("item.accepted_answer_id"),
                psf.col("item.body").alias("question_body"),
                psf.col("item.question_id"),
                psf.col("item.title").alias("question_title"),
            )
        )

        answers_raw = spark.read.json(os.path.join(f"s3a://{input_folder}", tag, "answers.json"))
        answers_expl = answers_raw.withColumn("item", psf.explode(psf.col("items")))
        answers = answers_expl.select(
            psf.col("item.answer_id").alias("accepted_answer_id"),
            psf.col("item.body").alias("answer_body")
        )

        content = questions.join(
            answers,
            on="accepted_answer_id"
        ).select(
            "question_id",
            psf.concat_ws(os.linesep, "question_title", "question_body", "answer_body").alias("content")
        )
         
        s3_client = boto3.client('s3')
        for row in content.collect():
            s3_client.put_object(Bucket=bucket, Key=f"cleaned/{tag}/{row['question_id']}.txt", Body=row['content'])




        
    

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
