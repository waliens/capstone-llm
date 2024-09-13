
#TODO add your project code and dependencies to the image

FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.1-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

COPY ./requirements.txt /opt/spark/work-dir/requirements.txt

RUN pip install -r requirements.txt

COPY ./src /opt/spark/work-dir/src
COPY ./setup.py /opt/spark/work-dir/setup.py

RUN pip install -e .

ENTRYPOINT ["python3", "-m", "capstonellm.tasks.clean"]
CMD ["-e", "local"]