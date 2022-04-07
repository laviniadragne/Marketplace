"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import logging
import time
from logging.handlers import RotatingFileHandler
from threading import BoundedSemaphore
import unittest

from tema.consumer import Consumer
from tema.producer import Producer
from tema.product import Tea, Coffee


class TestMarketplace(unittest.TestCase):
    """
    Class that represents the TestMarketplace.
    Used to test the behavior of the Marketplace class.
    """

    def setUp(self):
        self.marketplace = Marketplace(15)
        self.producers = []
        self.consumers = []
        self.initialize_producers()
        self.initialize_consumers()

    def initialize_producers(self):
        """
        Creates a list of producers with 1 producer and 3 products
        """

        products = [(Tea(name='Linden', price=9, type='Herbal'), 2, 0.18),
                    (Coffee(name='Indonezia', price=1, acidity='5.05', roast_level='MEDIUM'),
                     1, 0.23)]
        republish_wait_time = 0.15
        kwargs = {'name': 'prod1', 'daemon': True}
        self.producers.append(Producer(products, self.marketplace,
                                       republish_wait_time, **kwargs))

    def initialize_consumers(self):
        """
        Creates a list of consumers with 1 consumer, 1 cart and 3 operations to be performed
        """

        carts = [[{'type': 'add', 'product': Tea(name='Linden', price=9, type='Herbal'),
                   'quantity': 1},
                  {'type': 'add', 'product': Coffee(name='Indonezia', price=1, acidity='5.05',
                                                    roast_level='MEDIUM'), 'quantity': 1},
                  {'type': 'remove', 'product': Coffee(name='Indonezia', price=1, acidity='5.05',
                                                       roast_level='MEDIUM'), 'quantity': 1}]]
        retry_wait_time = 0.31
        kwargs = {'name': 'cons1'}
        self.consumers.append(Consumer(carts, self.marketplace, retry_wait_time, **kwargs))

    def producers_action(self):
        """
        Iterate through the list of products and try to publish them
        """
        # adauga producatorii produse
        ret = True
        producer_id = self.marketplace.register_producer()
        # parcurg lista de products si incerc sa fac publish la fiecare produs
        for product in self.producers[0].products:
            for _ in range(product[1]):
                # coada are dimensiune destul de mare
                ret = self.marketplace.publish(producer_id, product[0])
                if not ret:
                    return ret
        return ret

    def consumers_action(self):
        """
        Iterate through the list of carts and try to add or remove
        products from marketplace
        """

        # Fac add si remove la produse
        for cart in self.consumers[0].carts:
            cart_id = self.marketplace.new_cart()

            for product in cart:
                for _ in range(product["quantity"]):
                    if product["type"] == "add":
                        # Toti consumatorii ar trebui sa poata obtine produsele
                        self.marketplace.add_to_cart(cart_id, product["product"])
                    elif product["type"] == "remove":
                        self.marketplace.remove_from_cart(cart_id, product["product"])

    def test_register_producer(self):
        """
        Test the assign of an id for a producer
        """
        i = 0
        self.assertTrue(self.producers[i].marketplace.register_producer() == i)

    def test_publish(self):
        """
        Verify the return value and the resulted dictionaries from publish function
        """

        # Verific publish cu o coada de 15 produse, 1 producator si
        # 3 produse de adaugat
        ret = self.producers_action()
        # Verific daca produsele au fost adaugate
        self.assertTrue(ret, True)
        dict_prod = {0: [(Tea(name='Linden', price=9, type='Herbal'), True),
                         (Tea(name='Linden', price=9, type='Herbal'), True),
                         (Coffee(name='Indonezia', price=1, acidity='5.05',
                                 roast_level='MEDIUM'), True)]}
        distribution_products = {Tea(name='Linden', price=9, type='Herbal'): [0, 0],
                                 Coffee(name='Indonezia', price=1, acidity='5.05',
                                        roast_level='MEDIUM'): [0]}
        # Verific valorile din dictionarele create
        self.assertEqual(self.marketplace.distribution_products, distribution_products)
        self.assertEqual(self.marketplace.dict_prod, dict_prod)

        # Alt caz: verific cu o limitare de 2 produse in coada
        self.marketplace = Marketplace(2)
        ret = self.producers_action()
        # Ar trebui sa nu pot publica 3 produse intr-o coada limitata de 2
        self.assertEqual(ret, False)

    def test_new_cart(self):
        """
        Test the assign of an id for a cart
        """
        # Ar trebui sa am id-uri consecutive
        i = 0
        while i < len(self.consumers[0].carts):
            self.assertEqual(self.marketplace.new_cart(), i)
            i += 1

    def test_add_to_cart(self):
        """
        Verify the return value and the resulted dictionaries from add_to_cart function
        """

        # Public produse
        self.producers_action()

        # Fac doar add la produse
        for cart in self.consumers[0].carts:
            cart_id = self.marketplace.new_cart()

            for product in cart:
                for _ in range(product["quantity"]):
                    if product["type"] == "add":
                        # Toti consumatorii ar trebui sa poata obtine produsele
                        self.assertEqual(self.marketplace.add_to_cart(cart_id,
                                                                      product["product"]), True)

            # Outputul pe care ar trebui sa-l am
            dict_prod = {0: [(Tea(name='Linden', price=9, type='Herbal'), False),
                             (Tea(name='Linden', price=9, type='Herbal'), True),
                             (Coffee(name='Indonezia', price=1, acidity='5.05',
                                     roast_level='MEDIUM'), False)]}

            carts = {0: [(Tea(name='Linden', price=9, type='Herbal'), 0),
                         (Coffee(name='Indonezia', price=1, acidity='5.05',
                                 roast_level='MEDIUM'), 0)]}

            # Verific continutul dictionarelor
            self.assertEqual(dict_prod, self.marketplace.dict_prod)
            self.assertEqual(carts, self.marketplace.carts)

    def test_remove_from_cart(self):
        """
        Verify the resulted dictionaries from remove_to_cart function
        """

        # Public produse
        self.producers_action()

        # Fac operatiile de add si remove
        self.consumers_action()

        # Outputul pe care ar trebui sa-l am
        dict_prod = {0: [(Tea(name='Linden', price=9, type='Herbal'), False),
                         (Tea(name='Linden', price=9, type='Herbal'), True),
                         (Coffee(name='Indonezia', price=1, acidity='5.05',
                                 roast_level='MEDIUM'), True)]}

        carts = {0: [(Tea(name='Linden', price=9, type='Herbal'), 0)]}

        # Verific continutul dictionarelor
        self.assertEqual(dict_prod, self.marketplace.dict_prod)
        self.assertEqual(carts, self.marketplace.carts)

    def test_place_order(self):
        """
        Verify the return list of products from place_order function
        """

        # Simulez publish, add si remove
        self.producers_action()
        self.consumers_action()
        # Outputul dorit
        products = [(Tea(name='Linden', price=9, type='Herbal'), 0)]
        # Verific outputul intors
        self.assertEqual(self.marketplace.place_order(0), products)


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

        logging.basicConfig(
            handlers=[RotatingFileHandler('marketplace.log', maxBytes=100000, backupCount=10)],
            level=logging.INFO,
            format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
            datefmt='%Y-%m-%dT%H:%M:%S')
        logging.Formatter.converter = time.gmtime

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
        logging.info("id_producer: %d", self.id_prod)
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
        logging.info("producer_id: %d", producer_id)
        logging.info("product: %s", product)

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
            logging.info("return value: False")
            return False

        logging.info("return value: True")
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.cart_lock.acquire()
        self.id_cart += 1
        logging.info("id_cart: %d", self.id_cart)
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
        logging.info("cart_id: %d", cart_id)
        logging.info("product: %s", product)

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
                            logging.info("return value: True")
                            return True
        logging.info("return value: False")
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        logging.info("cart_id: %d", cart_id)
        logging.info("product: %s", product)

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
        logging.info("cart_id: %d", cart_id)

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

        logging.info("return value: %s", self.carts[cart_id])
        return self.carts[cart_id]
