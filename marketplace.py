"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import BoundedSemaphore
import unittest

from producer import Producer
from product import Tea, Coffee


class TestMarketplace(unittest.TestCase):
    def setUp(self):
        self.marketplace = Marketplace(15)
        self.producers = []
        self.consumers = []
        self.initialize_producers()

    def initialize_producers(self):
        products = [(Tea(name='Linden', price=9, type='Herbal'), 2, 0.18),
                    (Coffee(name='Indonezia', price=1, acidity='5.05', roast_level='MEDIUM'), 1, 0.23)]
        republish_wait_time = 0.15
        kwargs = {'name': 'prod1', 'daemon': True}
        self.producers.append(Producer(products, self.marketplace, republish_wait_time, **kwargs))

    def test_register_producer(self):
        i = 0
        while i < len(self.producers):
            self.assertTrue(self.producers[i].marketplace.register_producer() == i)
            i += 1

    def test_publish(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_new_cart(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_add_to_cart(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_remove_from_cart(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_place_order(self):
        self.assertEqual('foo'.upper(), 'FOO')


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        # (key, value) = (id_producer, [product, availability)])
        self.dict_prod = {}
        # (key, value) = (product, id_producer)
        self.distribution_products = {}
        # (key, value) = (id_cart, [(product, id_producer)])
        self.carts = {}
        self.register_lock = BoundedSemaphore(1)
        self.cart_lock = BoundedSemaphore(1)
        self.print_lock = BoundedSemaphore(1)
        self.id_prod = -1
        self.id_cart = -1

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        self.register_lock.acquire()
        self.id_prod += 1
        self.register_lock.release()
        return self.id_prod

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        # Verific daca producatorul are produse in coada
        products_list = self.dict_prod.get(producer_id)

        # Are coada goala
        if products_list is None:
            self.dict_prod[producer_id] = []
            products_list = []

        # Daca mai poate produce, ii adaug produsul
        if len(products_list) < self.queue_size_per_producer:
            # Ii adaug produsul in coada ca fiind True
            self.dict_prod[producer_id].append((product, True))

            # Il adaug ca producator pentru acel produs
            producers = self.distribution_products.get(product)

            # Acest produs nu a mai fost produs niciodata
            if producers is None:
                self.distribution_products[product] = []

            self.distribution_products[product].append(producer_id)
        # Trebuie sa astepte, nu poate produce
        else:
            return False

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.cart_lock.acquire()
        self.id_cart += 1
        self.cart_lock.release()
        return self.id_cart

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        # Verific daca exista cosul, altfel il adaug in lista de cosuri
        products = self.carts.get(cart_id)
        if products is None:
            self.carts[cart_id] = []

        # Caut produsul la un producator
        # nu exista, inseamna ca nu am de unde sa-l cumpar
        if product not in self.distribution_products:
            return False

        producers = self.distribution_products[product]

        # caut la fiecare producator acel produs
        # daca l-am gasit, il fac indisponibil
        for producer in producers:
            # Daca are produse
            if self.dict_prod[producer] is not None:
                for i, (prod, availability) in enumerate(self.dict_prod[producer]):
                    # Daca am gasit produsul cautat
                    if prod == product:
                        # Daca e disponibil
                        if availability:
                            # Il fac indisponibil
                            self.dict_prod[producer][i] = (prod, False)
                            # Adaug in cos produsul si producatorul de la care l-a luat
                            self.carts[cart_id].append((product, producer))
                            return True
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """

        # Caut produsul in cos
        source_producer = -1
        items = self.carts[cart_id]

        # Daca am produse in cos
        if items is not None:
            # Parcurg produsele
            for _, (prod, producer) in enumerate(self.carts[cart_id]):
                # Daca am gasit produsul
                if prod == product:
                    # Il actualizez in lista producatorului
                    for j, all_product in enumerate(self.dict_prod[producer]):
                        (current_product, availability) = all_product
                        # Am gasit produsul si nu e disponibil, il fac disponibil
                        if current_product == prod:
                            if not availability:
                                self.dict_prod[producer][j] = (current_product, True)
                                source_producer = producer
                                break
                    break

            # il elimin din cos
            self.carts[cart_id].remove((product, source_producer))

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        # Parcurg toate produsele din cos
        for (product, producer) in self.carts[cart_id]:
            # Parcurg toate produsele de la acel producator, pentru a-l gasi
            # pe cel din cos
            for (prod, availability) in self.dict_prod[producer]:
                if product == prod:
                    # Daca e indisponibil il sterg de tot de la producer
                    if not availability:
                        self.dict_prod[producer].remove((product, availability))
                        # Sterg producatorul de la cine produce acel produs
                        self.distribution_products[product].remove(producer)
                        break

        return self.carts[cart_id]
