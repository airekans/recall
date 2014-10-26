from google.protobuf import message
import logging
import struct
from recall.proto import rpc_meta_pb2


def serialize_message(meta_info, msg):
    meta_info.msg_name = msg.DESCRIPTOR.full_name

    meta_buf = meta_info.SerializeToString()
    msg_buf = msg.SerializeToString()
    meta_buf_len = len(meta_buf)
    msg_buf_len = len(msg_buf)

    pb_buf_len = struct.pack('!III', meta_buf_len + msg_buf_len + 8,
                             meta_buf_len, msg_buf_len)
    msg_buf = 'PB' + pb_buf_len + meta_buf + msg_buf
    return msg_buf


def parse_message(buf, msg_cls):
    msg = msg_cls()
    try:
        msg.ParseFromString(buf)
        return msg
    except message.DecodeError as err:
        logging.warning('parsing msg failed: ' + str(err))
        return None


def parse_meta(buf, meta_info=None):
    if len(buf) < 8:
        return None

    (meta_len, pb_msg_len) = struct.unpack('!II', buf[:8])
    if len(buf) < 8 + meta_len + pb_msg_len:
        return None

    meta_msg_buf = buf[8:8 + meta_len]
    if meta_info is None:
        meta_info = rpc_meta_pb2.MetaInfo()

    try:
        meta_info.ParseFromString(meta_msg_buf)
        return meta_len, pb_msg_len, meta_info
    except message.DecodeError as err:
        logging.warning('parsing msg meta info failed: ' + str(err))
        return None

