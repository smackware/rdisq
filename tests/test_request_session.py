from rdisq.request.receiver import RegisterMessage
from rdisq.request.rdisq_request import RdisqRequest
from tests._messages import SumMessage, sum_, AddMessage, SubtractMessage, Summer
from tests._other_module import MessageFromExternalModule
#
# def test_session_vars(rdisq_message_fixture):
#     receiver_service_1 = rdisq_message_fixture()
#     RdisqRequest(StartHandling(AddMessage, {})).send_async()
#     receiver_service_1.rdisq_process_one()