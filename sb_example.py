"""Synchronous send/receive of messages via Azure Service Bus.
"""
from timeit import default_timer

from settings import SB_CONN_STR

from azure.servicebus import ServiceBusClient, Message
from azure.servicebus.common.errors import ServiceBusResourceNotFound

from faker import Faker

def get_queue_client(queue_name, create=True):
    """Returns a service bus queue client object for the named queue.
    If create==True, creates the queue if not found.
    """
    start = default_timer()
    print("\nCreating Service Bus queue client ...")
    sb_client = ServiceBusClient.from_connection_string(SB_CONN_STR)
    try:
        client = sb_client.get_queue(queue_name)
    except ServiceBusResourceNotFound:
        if create:
            sb_client.create_queue(queue_name)
            client = sb_client.get_queue(queue_name)
        else:
            client = None
    print(f"    {default_timer()-start:.2f} seconds\n")
    return client

def main():
    """Simple example of synchronous access to a Service Bus queue
    """

    # create a list of random messages
    fake = Faker()
    test_messages = [f"{fake.name()}, {fake.state()}" for _ in range(10)]

    # create a queue client
    queue_client = get_queue_client("test_queue")
 
    # send the test messages
    message_send(queue_client, test_messages)
 
    # read the same number of messages
    message_receive(queue_client, len(test_messages))

def message_receive(queue_client, msg_count):
    """Read msg_count total messages from queue_client.
    """
    start = default_timer()
    messages = queue_client.get_receiver()
    for _ in range(msg_count):
        message = next(messages)
        print(f"Msg received <-- {message}")
        message.complete()
    print(f"    {default_timer()-start:.2f} seconds")

def message_send(queue_client, messages):
    """Send a list of messages to queue_client.
    """
    start = default_timer()
    with queue_client.get_sender() as sender:
        for msg_no in range(len(messages)):
            sender.send(Message(messages[msg_no]))
            print(f"Message sent --> {messages[msg_no]}")
    print(f"    {default_timer()-start:.2f} seconds\n")

if __name__ == "__main__":
    main()
