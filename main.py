import requests
import pprint
import json
import boto3


def main():

    s3 = boto3.client("s3", region_name="us-east-1")
    tag = "python-polars"

    url = f"http://api.stackexchange.com/2.3/questions?order=desc&sort=votes&site=stackoverflow&tagged={tag}&pagesize=5&filter=withbody"

    response = requests.request("GET", url)

    pprint.pprint(response.json())

    with open("questions.json", "w") as f:
        f.write(json.dumps(response.json()))

    s3.upload_file(
        "questions.json",
        "seasonal-schools-bedrock-data-us",
        f"input/{tag}/questions.json",
    )

    question_ids = [
        str(question["question_id"]) for question in response.json()["items"]
    ]

    url = f"http://api.stackexchange.com/2.3/questions/{';'.join(question_ids)}/answers?order=desc&sort=votes&site=stackoverflow&filter=withbody"
    response = requests.request("GET", url)
    pprint.pprint(response.json())

    with open("answers.json", "w") as f:
        f.write(json.dumps(response.json()))

    s3.upload_file(
        "answers.json",
        "seasonal-schools-bedrock-data-us",
        f"input/{tag}/answers.json",
    )


if __name__ == "__main__":
    main()
