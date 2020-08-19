import sys

import hpfeeds
import pymongo
import ast
import json
import logging


#####################################################################
#Broker Information
HOST = '127.0.0.1'
PORT = 20000
CHANNELS = ['hostage','conpot']
IDENT = 'xxxxx'
SECRET = 'xxxxxx'
######################################################################
# Required - MongoDB information
MONGOHOST = '127.0.0.1'
MONGOPORT = 27017
MONGODBNAME = 'channels'
# Optional
MONGOUSER = ''
MONGOPWD = ''
#########################################################################
#Init logger
logger = logging.getLogger('hpfeeds Subscriber')
hdlr = logging.FileHandler('C:\cti-logs\hpfeeds\subscriber\subscriber.log') #log file path
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO) #Log level
##########################################################################


def get_db(host, port, name, user='', passwd=''):
    dbconn = pymongo.Connection(host, port)
    db = pymongo.database.Database(dbconn, name)
    if user != '' or passwd != '':
        db.authenticate(user, passwd)
    return db


def main():
    hpc = hpfeeds.new(HOST, PORT, IDENT, SECRET)

    logger.info('Connected to:'+hpc.brokername)

    insertCon = pymongo.MongoClient(host="localhost", port=27017)
    #db = "channels"
    #collection = "hostage"

    def on_message(identifier, channel, payload):
        if channel == 'hostage':
            try:
                msg = ast.literal_eval(str(payload))
                #msg = json.dumps(payload)
            except:
                logger.error('exception processing hostage.connections event:'+repr(payload))

            else:
                logger.info("Received message on hostage channel! from:"+identifier)
                logger.info(msg)
                logger.info('inserting record:'+ str(msg))

                db = insertCon['channels']
                collection = db['hostage']
                collection.insert_many(json.loads(msg))
                logger.info("Added to DB")


        elif channel == 'conpot':
            try:
                payload_python = str(payload)
                msg = ast.literal_eval(payload_python.replace("null", "None"))
            except:
                logger.error('exception processing  conpot events:'+repr(payload))

            else:
                logger.info("Received message on Conpot channel! from:"+identifier)
                logger.info(msg)
                logger.info('inserting...'+ str(msg))

                db = insertCon['channels']
                collection = db['conpot']
                collection.insert_one(json.loads(msg))
                logger.info("Added to DB")


    def on_error(payload):
        logger.critical(' -> errormessage from server: {0}'.format(payload))
        hpc.stop()

    hpc.subscribe(CHANNELS)
    hpc.run(on_message, on_error)
    hpc.close()
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.critical("Unexpected exception!")
        sys.exit(0)
