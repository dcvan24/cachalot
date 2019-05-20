import time
from app.base import Data
from unittest import TestCase
from tools.basic import sha1_encode, returns


class Sha1EncodeTest(TestCase):

    def test_int(self):
        i1, i2, i3 = 1, 1, 2
        self.assertEqual(sha1_encode(i1), sha1_encode(i2))
        self.assertNotEqual(sha1_encode(i1), sha1_encode(i3))
        self.assertNotEqual(sha1_encode(i1), sha1_encode(None))

    def test_float(self):
        f1, f2, f3 = 1., 1., 2.
        self.assertEqual(sha1_encode(f1), sha1_encode(f2))
        self.assertNotEqual(sha1_encode(f1), sha1_encode(f3))
        self.assertNotEqual(sha1_encode(f1), sha1_encode(None))

    def test_bool(self):
        b1, b2, b3 = True, True, False
        self.assertEqual(sha1_encode(b1), sha1_encode(b2))
        self.assertNotEqual(sha1_encode(b1), sha1_encode(b3))
        self.assertNotEqual(sha1_encode(b1), sha1_encode(None))

    def test_str(self):
        s1, s2, s3 = '1', '1', '2'
        self.assertEqual(sha1_encode(s1), sha1_encode(s2))
        self.assertNotEqual(sha1_encode(s1), sha1_encode(s3))
        self.assertNotEqual(sha1_encode(s1), sha1_encode(None))

    def test_none(self):
        self.assertEqual(sha1_encode(None), sha1_encode(None))

    def test_object(self):
        d1, d2 = Data('hello', 10), Data('hello', 10)
        d3, d4 = Data('world', 10), Data('hello', 20)
        d5 = Data('world', 20)

        self.assertEqual(sha1_encode(d1), sha1_encode(d2))
        self.assertNotEqual(sha1_encode(d1), sha1_encode(d3))
        self.assertNotEqual(sha1_encode(d1), sha1_encode(d4))
        self.assertNotEqual(sha1_encode(d1), sha1_encode(d5))

    def test_primitive_list(self):
        l1 = [i for i in range(5)]
        l2 = [i for i in range(5)]
        self.assertEqual(sha1_encode(l1), sha1_encode(l2))

        l3 = [i for i in range(3)]
        l4 = [i for i in range(10)]
        self.assertNotEqual(sha1_encode(l1), sha1_encode(l3))
        self.assertNotEqual(sha1_encode(l1), sha1_encode(l4))

        l5 = [i if i%2 == 0 else str(i) for i in range(5)]
        l6 = [i if i%2 == 0 else float(i) for i in range(5)]
        self.assertNotEqual(sha1_encode(l1), sha1_encode(l5))
        self.assertNotEqual(sha1_encode(l1), sha1_encode(l6))

    def test_object_list(self):
        l1 = [Data(str(i), i) for i in range(5)]
        l2 = [Data(str(i), i) for i in range(5)]
        self.assertEqual(sha1_encode(l1), sha1_encode(l2))


class ReturnDecoratorTest(TestCase):

    def test_dup_execution(self):
        @returns(str)
        def wait(duration):
            time.sleep(duration)
            return 'Sleep for {} seconds'.format(duration)
        duration = 1
        start = time.time()
        wait(duration)
        self.assertEqual(int(time.time() - start), duration)








