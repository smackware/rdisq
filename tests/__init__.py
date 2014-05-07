__author__ = 'smackware'
from unittest.case import TestCase
from rdisq import Result, RESULT_ATTR, PROCESS_TIME_ATTR, EXCEPTION_ATTR


class RdisqTest(TestCase):
    def test__result__process_response(self):
        valid_response = {
            RESULT_ATTR: "RETURNED",
            PROCESS_TIME_ATTR: 2,
            EXCEPTION_ATTR: None,
        }
        result = Result(1, None, timeout=2)
        returned_value = result.process_response(valid_response)
        self.assertEqual(result.exception, None)
        self.assertEqual(returned_value, "RETURNED")

    def test__result__process_response_exception(self):
        class TestException(Exception):
            pass

        valid_response = {
            RESULT_ATTR: None,
            PROCESS_TIME_ATTR: 2,
            EXCEPTION_ATTR: TestException("BLAH"),
        }
        result = Result(1, None, timeout=2)
        response = result.process_response(valid_response)
        self.assertEqual(response, None)
        self.assertTrue(isinstance(result.exception, TestException))
