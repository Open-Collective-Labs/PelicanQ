"""PelicanQ Python SDK example: publish and consume."""

from pelicanq import PelicanClient, ClientMessage, QueueOptions


def main():
    client = PelicanClient.connect("127.0.0.1:7072")

    created = client.declare_queue("example-queue", QueueOptions())
    print(f"Queue created: {created}")

    msg = ClientMessage(b"Hello, Python!").with_priority(5)
    result = client.publish("example-queue", msg)
    print(f"Published: id={result.id}")

    delivery = client.consume("example-queue")
    if delivery:
        print(f"Received: payload={delivery.message.payload!r} tag={delivery.delivery_tag}")
        client.ack("example-queue", delivery.delivery_tag)
        print("Done!")
    else:
        print("No message received")

    client.close()


if __name__ == "__main__":
    main()
