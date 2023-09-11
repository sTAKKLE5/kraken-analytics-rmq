import json
from typing import Any, Dict

import pika


def handle_errors(func):
    def wrapper(self, ch, method, properties, body):
        try:
            # Check the 'x-death' headers, which are automatically assigned when a message is
            # transferred to the retry queue, expires, and is then returned to its original queue.
            if properties.headers:
                if properties.headers.get("x-death"):
                    if properties.headers.get("x-death")[0].get("count") >= int(
                        self.settings["retry_count"]
                    ):
                        # send message to dead letter queue
                        self.send_direct_message(
                            message=json.loads(body),
                            routing_key=self.settings["queue_dead_letter"],
                        )
                        return
            # Process the original callback
            func(self, ch, method, properties, body)

        except:
            self.send_message(
                message=json.loads(body),
                headers=properties.headers,
                routing_key=self.settings["queue_retry"],
            )

    return wrapper


class RMQ:
    def __init__(
        self,
        business_logic_function,
        settings: Dict,
        connection=None,
        channel=None,
    ):
        self.business_logic_function = business_logic_function
        self.settings: Dict = settings
        self.connection = connection
        self.channel = channel
        if self.connection is None or self.channel is None:
            self._setup_connection_and_channel()

    def _setup_connection_and_channel(self):
        credentials = pika.PlainCredentials(
            self.settings["user"], self.settings["password"]
        )
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.settings["host"],
                port=self.settings["port"],
                virtual_host=self.settings["vhost"],
                credentials=credentials,
            )
        )
        self.channel = self.connection.channel()

    def send_message(
        self,
        message: Dict[str, Any],
        exchange: str = None,
        routing_key: str = None,
        headers: Dict = None,
    ):
        """
        Send message via exchange to a queue
        """
        headers = headers or {}

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(headers=headers),
        )

    def consume_messages(self):
        self.channel.basic_consume(
            queue=self.settings["queue_consume"],
            on_message_callback=self.callback,
            auto_ack=True,
        )
        self.channel.start_consuming()

    def send_direct_message(self, message: Dict[str, Any], routing_key: str):
        self.send_message(message=message, exchange="", routing_key=routing_key)

    @handle_errors
    def callback(self, ch, method, properties, body):
        message_body = json.loads(body)

        response = self.business_logic_function(message_body=message_body)
        for item in response:
            message: Dict[str, Any] = item["message"]
            routing_key: str = item["routing_key"]
            self.send_message(
                message=message,
                exchange=self.settings["exchange"],
                routing_key=routing_key,
            )
