"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import threading
from threading import Thread
from time import sleep


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        threading.Thread.__init__(self, **kwargs)

        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time

    def run(self):
        producer_id = self.marketplace.register_producer()
        while True:
            # parcurg lista de products si incerc sa fac publish la fiecare produs
            for product in self.products:
                for _ in range(product[1]):
                    # adaug doar tipul produsului in lista
                    ret = self.marketplace.publish(producer_id, product[0])
                    # Daca s-a reusit sa fie pus in marketplace
                    if ret:
                        sleep(product[2])
                    # Daca coada producatorului e plina, astept
                    while not ret:
                        sleep(self.republish_wait_time)
                        ret = self.marketplace.publish(producer_id, product[0])
