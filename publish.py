#####MAKE SURE THAT THE GOOGLE APPLICATION CREDENTIALS LOCAL VARIABLE IS SET FOR AUTHENTICATION PURPOSES#####

#####timestamps have not been added#####

#####Run using python 2.7#####

#####PROGRAM TO SIMULATE SENSOR DATA AND PUBLISH IT TO GOOGLE PUB/SUB



import __future__
import logging
from google.cloud import pubsub_v1
import random
import time
import json
from os import environ



####Credentials for using the google cloud API

PROJECT_ID=""
TOPIC = ""


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)



def publish(publisher, topic, message):
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data = data)



def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            TOPIC, message_future.exception()))
    else:
        print(message_future.result())


if __name__ == '__main__':
    num=0
    thingID = 0

    volumes = [40000, 60000, 20000, 35000, 70000, 10000, 15000, 10000, 80000, 50000]
    #list containing the volumes of the simulation tanks at a given time
    #The following three lists keep track of what event is occuring
    #The value in each index reflects how long a particular event has been occuring for the tank with that index
    #If the value is '0' then it means that the event is not currently going on#
    municipalFilling = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    tankerFilling = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    isFillingOverheadTank = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    numdat=0
    while True:
    #####Generating the JSON String#####
        #Dictionary to store the key value pairs#
        data={}


        data['volumeDiff'] = 0
        data['thingID'] = (thingID % 10) + 1
        #current stores the index of the tank for whom the json string is currently being generated#
        current = data['thingID'] -1
        if (municipalFilling[current] == 0 or tankerFilling[current] == 0 or isFillingOverheadTank[current] == 0):
            fill = random.randrange(100)
            if (fill < 90):
                data['volumeDiff'] += 0
            elif (fill >= 90 and fill < 96 and (not (isFillingOverheadTank[current] > 0)) ):
                isFillingOverheadTank[current] += 1

            elif (fill >= 96 and fill < 99 and (not (municipalFilling[current] > 0))):
                municipalFilling[current] += 1

            elif(fill == 99 and (not (tankerFilling[current] > 0))):
                tankerFilling[current] += 1

        if (municipalFilling[current] > 0):
            municipalFilling[current] = (municipalFilling[current] + 1) % 101
            volumes[current] += 200
            data['volumeDiff'] += 200
        if (tankerFilling[current] > 0):
            tankerFilling[current] = (tankerFilling[current] + 1) % 21
            volumes[current] += 500
            data['volumeDiff'] += 500
        if (isFillingOverheadTank[current] > 0):
            isFillingOverheadTank[current] = (isFillingOverheadTank[current] + 1) % 201
            volumes[current] -= 400
            data['volumeDiff'] += -400
        data['volume']=volumes[current]
        line = json.dumps(data)
        print line +' '+ str(numdat)
        numdat+=1
        thingID +=1

        #####PUBLISH THE STRING TO PUB/SUB
        message_future = publish(publisher, topic_path, line)
        message_future.add_done_callback(callback)
        time.sleep(0.1)
