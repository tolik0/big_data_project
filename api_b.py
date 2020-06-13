from flask import Flask
from flask_restful import Resource, Api
from flask.json import jsonify
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect("test3")

app = Flask(__name__)
api = Api(app)


class q1(Resource):
    def get(self):
        """Return the list of all the countries for which the events were created."""
        rows = session.execute('select DISTINCT group_country from cities')
        res = [getattr(row, "group_country") for row in rows]

        return jsonify(res)


class q2(Resource):
    def get(self, country):
        """Return the list of the cities for the specified country where at least one event was created."""
        rows = session.execute('select group_city from cities where group_country=%s', (country,))
        res = [getattr(row, "group_city") for row in rows]

        return jsonify(res)


class q3(Resource):
    def get(self, event_id):
        """
        Given the event id, return the following details:
        - event name
        - event time
        - the list of the topics
        - the group name
        - the city and the country of the event
        """
        rows = session.execute(
            'select event_id, event_name, time, group_topics, group_name, group_country, group_city from event_by_event_id where event_id=%s',
            (int(event_id),))

        columns = ["event_id", "event_name", "time", "group_topics", "group_name", "group_country", "group_city"]
        res = [{name: getattr(row, "group_city") for name in columns} for row in rows]

        return jsonify(res)


class q4(Resource):
    def get(self, group_city):
        """
        Return the list of the groups which have created events in the specified city. It should contain the following details:
        - City name
        - Group name
        - Group id
        """
        rows = session.execute(
            'select group_city, group_name, group_id from groups_by_city_and_group_id where group_city=%s',
            (group_city,))

        res = [{name: getattr(row, "group_city") for name in ["group_city", "group_name", "group_id"]} for row in rows]

        return jsonify(res)


class q5(Resource):
    def get(self, group_id):
        """
        Return all the events that were created by the specified group (group id will be the input parameter).
        With the following details:
        - event id
        - event name
        - event time
        - the list of the topics
        - the group name
        - the city and the country of the event
        """
        rows = session.execute(
            'select event_id, event_name, time, group_topics, group_name, group_country, group_city, group_id from event_by_group_id where group_id=%s',
            (int(group_id),))

        columns = ["event_id", "event_name", "time", "group_topics", "group_name", "group_country", "group_city",
                   "group_id"]
        res = [{name: getattr(row, "group_city") for name in columns} for row in rows]

        return jsonify(res)


api.add_resource(q1, '/q1/')
api.add_resource(q2, '/q2/<country>')
api.add_resource(q3, '/q3/<event_id>')
api.add_resource(q4, '/q4/<group_city>')
api.add_resource(q5, '/q5/<group_id>')

if __name__ == '__main__':
    app.run(port='5002', debug=True)
