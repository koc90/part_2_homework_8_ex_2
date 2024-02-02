import contacts
import pika
from contacts import connect_to_mongo
from contact_model import Contact
from names import exchange_name, queue_name


def create_channel():
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_name, exchange_type="direct")
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    return (connection, channel)


def get_contacts_id() -> list:
    # mongo_client = connect_to_mongo()
    contacts_id = []

    for id in Contact.objects.fields(id=1).as_pymongo():
        contacts_id.append(str(id["_id"]))
    return contacts_id


def put_id_on_queue(channel, id: str):
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=queue_name,
        body=id.encode(),
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
        ),
    )
    print(f">>> Id {id} sent to queue")


def main():
    contacts.generate(15)
    contacts_id = get_contacts_id()
    (connection, channel) = create_channel()
    for id in contacts_id:
        put_id_on_queue(channel, id)

    connection.close()


if __name__ == "__main__":
    main()
