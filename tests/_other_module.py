"""To test how auto-detection of handler-class works with classes from other modules"""

from rdisq.request.message import RdisqMessage

class MessageFromExternalModule(RdisqMessage):
    pass