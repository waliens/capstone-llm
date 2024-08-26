# Capstone Exercise: building a StackOverflow Chatbot

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/datamindedacademy/capstone-llm)

1. We ingest popular StackOverflow questions per summer school course topic (dbt, SQL, Airflow, Polars, Spark, Docker) and answers from Stack Exchange API to an S3 bucket.
2. The job of the participants of the summer school is to read in the data, join the questions and answers, clean the data, and write individual questions with their answers to a location accessible by Bedrock.
3. We configure a lambda that uses Bedrock to reply to questions (copy-pasta Rushil's code).


TODO:

- [ ] Create a branch for instructors to prepare input data (ingest data from different SO tags) -> Niels
- [ ] Add scaffolding for the participants to work on writing their cleaning script and Dockerfile -> Niels
- [ ] Prepare Gitpod environment with Airflow Docker compose and DockerOperator (https://github.com/fclesio/airflow-docker-operator-with-compose for inspiration) -> Jan / Jonas
- [ ] (Optional) Bedrock set up via Terraform -> Jan (if bored and too much time)
- [ ] Set up lambda with ChatGPT and Bedrock (Rushil's code: https://github.com/datamindedbe/llm-hackathon) -> Jan
- [ ] Make sure that conveyor can read/write to the S3 buckets (rewrite instructor setup? https://github.com/datamindedacademy/instructor_setups/tree/main/capstone-project) -> Niels
- [ ] Use SSO credentials for Gitpod environment -> Jan