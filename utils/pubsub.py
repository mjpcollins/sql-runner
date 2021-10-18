import base64


def pubsub_message_to_dict(envelope):
    """
    Convert a PubSub message and message contents into a dict.

    Stolen from: https://cloud.google.com/run/docs/tutorials/pubsub#run-clone-sample-repository-python

    :param envelope: Dict of data sent by PubSub
    :return: Parsed message or a note that the request failed
    """
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    data = None
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        data = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()

    full_decoded_message = {"message": {"data": data}}
    return full_decoded_message, 200
