from flask import Flask
from flask_restful import Resource, Api
import boto3
import datetime
import json


app = Flask(__name__)
api = Api(app)


class Questions(Resource):
    """ Class that realize API for precomputed reports"""

    def __init__(self):
        s3r = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY, aws_session_token=SESSION_TOKEN)

        self.bucket = s3r.Bucket('meetupprojectbucket')

    def get(self, question):
        """
        Return answer to question

        For first question: Return the statistics with the number of newly created events per each country for the last
        6 full hours, excluding the last hour. Example:
        {“time_start”: “15:00”,
        “time_end”: “21:00”,
        “statistics”: [{“US”: 1543}, {“France” : 899}, {“Germany” : 923}, ...]}.

        For second question: return the statistics containing the information about which groups posted the events at
        each US state in the last 3 full hours, excluding the last hour
        {“time_start”: “15:00”,
        “time_end”: “18:00”,
        “statistics”: [{“California” : [“Happy Programmers Group”, “Emmy’s Bookclub”]}, {“Nevada”: [“Las Vegas Warriors”
        , “Desert Bikers”]}, ...]}

        For third question: The most popular topic of the events for each country posted in the last 6 hours, excluding
        the last hour. The popularity is calculated based on the number of occurrences topic has amongst all the topics
        in all the events created in that country during the specified period. Example:
        {“time_start”: “15:00”,
        “time_end”: “21:00”,
        “statistics”: [{“France” : {“Baking croissants”: 88}}, {“Germany”: {“Brewing beer”: 71}, ...]}
        """
        if question not in ["1", "2", "3"]:
            return "Wrong number of question"
        hour = datetime.datetime.now().hour

        filename = f"q{question}_{hour}.json"
        self.bucket.download_file("api1/" + filename, filename)
        with open(filename) as file:
            result = json.load(file)
        return result


api.add_resource(Questions, '/<question>')

if __name__ == '__main__':
    app.run(port='5002', debug=True)
