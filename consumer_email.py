import pika
from bson import ObjectId
from contact_model import Contact
from names import exchange_name, queue_name
from contacts import connect_to_mongo


def create_channel():
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
    )
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)
    print("<<< Waiting for messages. To exit press CTRL+C")

    return channel


def stub(email):
    return email


def do_task(id_obj):
    mongo_client = connect_to_mongo()
    try:
        contact = Contact.objects(id=id_obj).as_pymongo().first()
        email = contact["email"]
    except:
        print("<<< There is no such contact in database")
    else:

        stub(email)
        print(f"<<< Message to {email} has been sent.")
        Contact.objects(id=id_obj).update(is_sent=True)


def callback(ch, method, properties, body):
    id_str = body.decode()
    print(f" <<< Received {id_str}")
    id = ObjectId(id_str)
    do_task(id)

    print(f" <<< Done: {method.delivery_tag}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    channel = create_channel()
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    main()
