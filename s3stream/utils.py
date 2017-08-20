import datetime
import logging

def dnow():
    return datetime.datetime.now().time()

def u_print(msg):
    _msg = "[{}]>{}".format(dnow(), msg)
    print(_msg)
    logging.info(_msg)
