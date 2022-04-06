"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import threading
from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        threading.Thread.__init__(self, **kwargs)

        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.kwargs = kwargs

    def run(self):
        for cart in self.carts:
            # primeste un id pentru cosul actual de cumparaturi
            cart_id = self.marketplace.new_cart()

            for product in cart:
                for _ in range(product["quantity"]):
                    if product["type"] == "add":
                        ret = self.marketplace.add_to_cart(cart_id, product["product"])
                        # daca produsul nu e disponibil
                        while not ret:
                            sleep(self.retry_wait_time)
                            ret = self.marketplace.add_to_cart(cart_id, product["product"])
                    elif product["type"] == "remove":
                        self.marketplace.remove_from_cart(cart_id, product["product"])
                    else:
                        return

            products = self.marketplace.place_order(cart_id)

            # Daca am ceva in cos
            if products:
                for product in products:
                    self.marketplace.print_lock.acquire()
                    print("%s bought %s" % (self.kwargs["name"], product[0]))
                    self.marketplace.print_lock.release()
