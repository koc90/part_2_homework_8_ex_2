from contacts import connect_to_mongo
from contact_model import Contact


def verify():
    mongo_client = connect_to_mongo()
    contacts = Contact.objects().as_pymongo()

    for contact in contacts:
        print(contact)


if __name__ == "__main__":
    verify()
