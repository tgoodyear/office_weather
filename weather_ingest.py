import datetime
import json
import sys
import time

from influxdb import InfluxDBClient


def processInput():
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('weather')

    # {	'time': '2019-05-15 14:39:50', 
    #  	'model': 'Ambient Weather F007TH Thermo-Hygrometer', 
    # 	'device': 254, 
    # 	'channel': 3, 
    # 	'battery': 'OK', 
    # 	'temperature_F': 73.7, 
    # 	'humidity': 42, 
    # 	'mic': 'CRC'}

    location = {
            1: 'E1244B',
            2: 'E1252B',
            3: 'E1251',
            4: 'E1209'
    }

    for line in sys.stdin:
        # Received data packet. Attempt JSON conversion
        now = lambda : datetime.datetime.now()
        try:
            inData = json.loads(line.rstrip())
        except Exception as e:
            print('[%s] Failed to convert to JSON: %s' % (now(),line.rstrip()))
            print('[%s] Exception: %s' % (now(), e))
            with open('anomolies.txt','a+') as f:
                f.write(str(now())+'\n'+line)
            continue

        # Successful JSON conversion. Check for valid fields
        print('[%s] RX:      %s' % (now(), inData))
        validData = True 
        dataFields = ['channel','battery','model','temperature_F','humidity','device']
        for field in dataFields:
            if field not in inData:
                print('[%s] Anomolous Data: %s' % (now(),inData))
                validData = False
                with open('anomolies.txt','a+') as f:
                    f.write(str(now())+'\n'+json.dumps(inData)+'\n')
                break

        if not validData:
            continue
        
        # Channel number does not have an associated location
        if inData['channel'] not in location:
            print('[%s] Skipping Ch %s, No location.' % (now(),inData['channel']))
            with open('anomolies.txt','a+') as f:
                f.write(str(now())+'\n'+json.dumps(inData)+'\n')
            continue

        # Valid data point. Construct packet and send to database
        data = {}
        data['measurement'] = 'weather'
        data['tags'] = {	'channel':inData['channel'],
                                'battery':inData['battery'],
                                'model':inData['model'],
                                'device':inData['device'],
                                'location':location[inData['channel']]}

        data['fields'] = {'temperature_F':inData['temperature_F'],'humidity':inData['humidity']}
        data['time'] = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())

        try:
            client.write_points([data])
        except Exception as e:
            print('[%s] Failed to insert %s. Writing to file.' % (now(),data))
            with open('dbWriteFailed.json','a+') as f:
                f.write(json.dumps(inData)+'\n')
            raise
        print('[%s] Inserted %s' % (now(), data))

def processLoop():
    while True:
        try:
            print("Starting",datetime.datetime.now())
            processInput()

        except Exception as e:
            print(e)
            print("Restarting",datetime.datetime.now())
            processLoop()


processLoop()
