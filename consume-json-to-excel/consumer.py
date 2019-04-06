import argparse
from kafka import KafkaConsumer
import threading
import pandas as pd
from pandas.io.json import json_normalize
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    parser.add_argument('--groupid')
    parser.add_argument('--brokerlist')
    args = parser.parse_args()
    topic = args.topic
    groupid = args.groupid
    brokerlist = args.brokerlist
    print("brokerlist: %s topic: %s groupid: %s" % (brokerlist, topic, groupid))
    
    kafkaListener = ListenerThread(brokerlist, topic, groupid)
    kafkaListener.start()
    
    listenToUser(kafkaListener, topic, groupid)

    

def listenToUser(kafkaListener, topic, groupid):
    excelFilename = './' + topic + '.' + groupid + '.xlsx'
    while True:
        userReq = input("What next, boss? ")
        if(userReq == 'excel'):
            try:
                jsonlist = kafkaListener.getMessages()
                if(len(jsonlist) == 0):
                    print("no messages yet")
                else:
                    jsonlistToExcel(jsonlist, excelFilename)
            except Exception as e:
                print(e)
        elif userReq == 'how many':
            print("Number of messages read: %d" % (kafkaListener.getMessagesRead()))
        elif userReq == 'exit':
            kafkaListener.askToDie()
            kafkaListener.join()
            exit(0)
        else:
            print('unrecognized command: ' + userReq)

def jsonlistToExcel(jsonlist, excelFilename):
    jsonString = ",".join(jsonlist)
    jsonString = "[" + jsonString + "]"
    jsonData = json.loads(jsonString)
    df = json_normalize(jsonData)
    df.to_excel(excelFilename, index=False)
    print("Outputted %d rows to: %s" % (len(jsonlist), excelFilename))

class ListenerThread(threading.Thread):
    __lock = threading.Lock()
    __askedToDie = False

    def __init__(self, brokerlist, topic, groupid):
        threading.Thread.__init__(self)
        self.__brokerlist = brokerlist
        self.__topic = topic
        self.__groupid = groupid
        self.__allMessages = []
        
    def run(self):
        consumer = KafkaConsumer(self.__topic, bootstrap_servers=self.__brokerlist, group_id=self.__groupid, auto_offset_reset='earliest', enable_auto_commit=True, value_deserializer=lambda x: x.decode('utf-8'))
        for msg in consumer:
            with self.__lock:
                if self.__askedToDie:
                    break
                self.__allMessages.append(msg.value)
        consumer.close()

    def getMessages(self):
        with self.__lock:
            return self.__allMessages.copy()
            
    def getMessagesRead(self):
        with self.__lock:
            return len(self.__allMessages);
    
    def askToDie(self):
        with self.__lock:
            self.__askedToDie = True
   

main()
