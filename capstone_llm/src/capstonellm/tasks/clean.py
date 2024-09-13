import argparse
import logging
from pyspark.sql import SparkSession
import  pyspark.sql.functions as psf
import boto3

from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    tags = ["airflow", "apache-spark", "dbt", "docker", "pyspark", "python-polars", "sql"]
    
    for tag in tags:
        questions = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/questions.json")
        answers = spark.read.json(f"s3a://dataminded-academy-capstone-llm-data-us/input/{tag}/answers.json")
        cleaned_data = clean_data(questions, answers)
        write_data(cleaned_data,tag)
       
def write_data(data, tag):
    bucket = "dataminded-academy-capstone-llm-data-us"
    s3_client = boto3.client('s3')
    for row in data.collect():
       
        s3_client.put_object(Bucket=bucket, Key=f"cleaned/{tag}/{row['question_id']}.txt", Body=row['content']
            )


def clean_data(questions, answers):
    cleaned_questions = clean_questions(questions)
    cleaned_answers = clean_answers(answers)
    cleaned_data = cleaned_questions.join(
        cleaned_answers,
        on="question_id",
        how="inner"
    )
    return cleaned_data.select(
        psf.col("question_id"),
        psf.concat("title","body","answer", "link")
            .alias("content")
    )
def clean_questions(questions):
    cleaned_questions = questions.select(psf.explode(psf.col("items")).alias("items"))
    cleaned_questions = cleaned_questions.select(
        psf.col("items.title"),
        psf.col("items.body"),
        psf.col("items.link").alias("link"),
        psf.col("items.question_id").alias("question_id"),
    )
    return cleaned_questions
 
def clean_answers(answers):
    cleaned_answers = answers.select(psf.explode(psf.col("items")).alias("items"))
    cleaned_answers = cleaned_answers.select(
        psf.col("items.body").alias("answer"),
        psf.col("items.question_id").alias("question_id"),
        psf.col("items.is_accepted").alias("is_accepted")
    ).filter(
        psf.col("is_accepted") == "True"
    )
    return cleaned_answers
 

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
