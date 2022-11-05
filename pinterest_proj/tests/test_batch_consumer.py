import unittest
from (pintrest-proj) import config as c

class AccessKey(object):
    def __init__(self, user, region):
        self.user = user
        self.param_regions = region
        self.param_access = c.S3_ACCESS_KEY
        self.param_secret = c.S3_SECRET_KEY

class AccesssKeyTest(unittest.TestCase):
    def test_init(self):
        a = AccessKey('abc', 'eu-west-1')
        self.assertEquals(a.user, 'abc')
        self.assertEquals(a.param_regions, 'eu-west-1')
        self.assertEquals(a.param_access, 'abc/')

if __name__ == '__main__':
    unittest.main()