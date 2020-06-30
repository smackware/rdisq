from typing import NewType

RECEIVER_SERVICE_NAME = "ReceiverService"
QueueName = NewType("QueueName", str)  # names of redis keys that are used as message queues
ServiceUid = NewType("ServiceUid", str)  # uids of rdisq instances
