from mongoengine import connect
from contact_model import Contact
import faker


def connect_to_mongo():
    with open("uri_mongo.txt", "r") as f:
        uri = f.read()

    mongo_client = connect(host=uri)

    return mongo_client


def complete_data(n, mongo_client):
    Contact.objects().delete()
    fake_data = faker.Faker()
    for _ in range(n):
        contact = Contact(
            full_name=fake_data.name(), email=fake_data.email(), is_sent=False
        )
        contact.save()


def generate(contact_num):
    complete_data(n=contact_num, mongo_client=connect_to_mongo())
