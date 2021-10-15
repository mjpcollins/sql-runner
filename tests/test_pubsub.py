from unittest import TestCase
from utils.pubsub import pubsub_message_to_dict


class TestPubSub(TestCase):

    def test_pubsub_message_to_dict_ok(self):
        input_request = {"message": {"data": b'aGVsbG8gd29ybGQ='}}
        expected_message = {"message": {"data": "hello world"}}, 200
        actual_message = pubsub_message_to_dict(input_request)
        self.assertEqual(expected_message, actual_message)

    def test_pubsub_message_to_dict_no_envelop(self):
        input_request = None
        expected_message = "Bad Request: no Pub/Sub message received", 400
        actual_message = pubsub_message_to_dict(input_request)
        self.assertEqual(expected_message, actual_message)

    def test_pubsub_message_to_dict_no_message(self):
        input_request = {"no_message": None}
        expected_message = "Bad Request: invalid Pub/Sub message format", 400
        actual_message = pubsub_message_to_dict(input_request)
        self.assertEqual(expected_message, actual_message)

    def test_pubsub_message_to_dict_not_a_dict(self):
        input_request = {1, 2, 3}
        expected_message = "Bad Request: invalid Pub/Sub message format", 400
        actual_message = pubsub_message_to_dict(input_request)
        self.assertEqual(expected_message, actual_message)

    def test_pubsub_message_to_dict_no_data(self):
        input_request = {"message": None}
        expected_message = {"message": {"data": None}}, 200
        actual_message = pubsub_message_to_dict(input_request)
        self.assertEqual(expected_message, actual_message)
