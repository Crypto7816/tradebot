import unittest
from entity import PositionDict, Position

class PositionDictTests(unittest.TestCase):
    def setUp(self):
        self.position_dict = PositionDict()

    def test_update_existing_position(self):
        symbol = 'BTC'
        order_amount = 1.5
        order_price = 50000.0

        # Add a position
        self.position_dict.update(symbol, order_amount, order_price)

        # Update the position
        new_order_amount = 2.0
        new_order_price = 55000.0
        self.position_dict.update(symbol, new_order_amount, new_order_price)

        # Check if the position is updated correctly
        position = self.position_dict[symbol]
        self.assertEqual(position.amount, order_amount + new_order_amount)
        self.assertEqual(position.last_price, new_order_price)
        self.assertEqual(position.avg_price, (order_amount * order_price + new_order_amount * new_order_price) / (order_amount + new_order_amount))
        self.assertEqual(position.total_cost, order_amount * order_price + new_order_amount * new_order_price)

    def test_update_new_position(self):
        symbol = 'ETH'
        order_amount = 2.5
        order_price = 3000.0

        # Update a new position
        self.position_dict.update(symbol, order_amount, order_price)

        # Check if the position is added correctly
        position = self.position_dict[symbol]
        self.assertEqual(position.amount, order_amount)
        self.assertEqual(position.last_price, order_price)
        self.assertEqual(position.avg_price, order_price)
        self.assertEqual(position.total_cost, order_amount * order_price)

    def test_update_zero_amount_position(self):
        symbol = 'BTC'
        order_amount = 1.5
        order_price = 50000.0

        # Add a position
        self.position_dict.update(symbol, order_amount, order_price)

        # Update the position with zero amount
        new_order_amount = -5
        new_order_price = 55000.0
        self.position_dict.update(symbol, new_order_amount, new_order_price)

        # Check if the position is removed
        self.assertNotIn(symbol, self.position_dict)

if __name__ == '__main__':
    unittest.main()