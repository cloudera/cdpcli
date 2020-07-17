# Copyright (c) 2016 Cloudera, Inc. All rights reserved.

import os

from cdpcli.argparser import ArgTableArgParser
from cdpcli.argparser import ServiceArgParser
from cdpcli.clidriver import ServiceOperation
from cdpcli.model import ServiceModel
from tests import unittest
import yaml

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'argparser')
TEST_ARGS = ['submit-spark-job',
             '--jars',
             's3a://foobar/barfoo-1.0.jar',
             u'\\-s3a://foobar/barfoo-1.0.jar',
             '--main-class',
             'com.foobarcorp.big_data_maker.SimpleApp',
             '--cluster-name=Cluster9',
             '--arguments',
             '\\-o',
             's3a://foobar-demo/big.out/',
             u'\\-p',
             '100',
             '\\-rfoo\\-bar\\-',
             '1000000']


class TestServiceArgParser(unittest.TestCase):

    def setUp(self):
        self.parser = ServiceArgParser({'submit-spark-job': ''}, 'dataeng')
        self.args = TEST_ARGS

    def testOperationDetected(self):
        parsed, remaining = self.parser.parse_known_args(self.args)
        self.assertEqual('submit-spark-job', parsed.operation)
        self.assertEqual(self.args[1:], remaining)


class TestArgTableArgParser(unittest.TestCase):

    def setUp(self):
        self.args = TEST_ARGS[1:]
        model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        service_model = ServiceModel(model, 'servicename')
        service_operation = ServiceOperation(
            'submit-spark-job',
            'dataeng',
            '',
            service_model.operation_model('submitSparkJob'))
        self.parser = ArgTableArgParser(service_operation.arg_table)

    def testArsParsedCorrectly(self):
        parsed, remaining = self.parser.parse_known_args(self.args)
        self.assertEqual([], remaining)
        parsed_args = vars(parsed)
        # The second jar starts with a dash. We should decode it correctly.
        self.assertEquals(['s3a://foobar/barfoo-1.0.jar',
                           '-s3a://foobar/barfoo-1.0.jar'],
                          parsed_args['jars'])
        self.assertEquals('com.foobarcorp.big_data_maker.SimpleApp',
                          parsed_args['main_class'])
        self.assertEquals('Cluster9', parsed_args['cluster_name'])
        # The following tests that we decoded the starting dash
        self.assertEquals(
            ['-o',
             's3a://foobar-demo/big.out/',
             '-p',
             '100',
             '-rfoo\\-bar\\-',
             '1000000'],
            parsed_args['arguments'])
