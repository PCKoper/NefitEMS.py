#################################################################################
#
# Generic Information
#
#################################################################################
# EMS DataGram Layout. (BREAK, is represented as 3 bytes 0xFF,0x00,0x00)        #
#################################################################################
# Byte 1  #Byte 2   # Byte 3    # Byte 4 # Byte 5..n-1 # Byte n # Bytes n+1-n+3 #
# Sender  #Receiver # Frametype # Offset # Data bytes  # CRC    # BREAK         #
#################################################################################
#
# Mini HowTo Using Raspberry Pi to readout EMS bus with level shifter from:
# (https://shop.hotgoodies.nl/ems/) The board provides an electical interface 
# between TTL level logic and the EMS bus.
#
# Follow the board instructions on how to connect it to the RaspBerry Pi GPIO pins.
#
# On the Raspberry Pi, make sure the GPIO Tx & Rx are hooked up to /dev/ttyAMA0
# and not to the Pi3 default ttyS0, that did not work that well for me.
#
# Howto make sure this happens? add the following line to your /boot/config.txt
# dtoverlay=pi3-miniuart-bt
# This will result in serial0 to be pointing to ttyAMA0 i.s.o. ttyS0:
#
# lrwxrwxrwx  1 root root           7 feb 10 18:26 serial0 -> ttyAMA0
# lrwxrwxrwx  1 root root           5 feb 10 18:26 serial1 -> ttyS0
#
# Of course you should enable the UART in /boot/config.txt by using the config
# program or manually add the line: 
# enable_uart=1
#
# Also make sure you disable the serial console.
#
# Modify in the script below the Domoticz URLs, to only contain the stuff you want
# to log, and change the URL to match your own Domoticz URL and the idx-es to match
# those that you got when you added the dummy sensors.
# Extending with additional message types to parse is as simple as adding a parse 
# function for the message type, and add it to the MessageParseDispatcher dictionary.
#
#################################################################################

#################################################################################
#Imports
#################################################################################
import serial
import termios
import numpy
import time
import datetime
import urllib2
import ssl

#################################################################################
#Some definitions To use
#################################################################################

DomoticzHost="https://192.168.225.86:443/"

#Domoticz URLs to push data
RoomTemperatureURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=69&nvalue=0&svalue="
FlowTemperatureURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=70&nvalue=0&svalue="
ReturnFlowTemperatureURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=71&nvalue=0&svalue="
BurnerTemperatureURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=72&nvalue=0&svalue="
BurnerDutyCycleURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=73&nvalue=0&svalue="
PumpDutyCycleURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=74&nvalue=0&svalue="
SystemPressureURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=75&nvalue=0&svalue="
RoomSetpointURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=76&nvalue=0&svalue="
IonizationCurrentURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=79&nvalue=0&svalue="
DeltaTURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=83&nvalue=0&svalue="

#Creating a context to indicate to urllib2 that I don't want SSL verification
#because my domoticz setup does not have a valid CERT certificate.
UnverifiedContext = ssl._create_unverified_context()

#Status dictionary, easy way to translate the status code into a human readable message
# ToDo: add the error status messages as well, will need dictionary of dictionaries for that, 
# some status codes have multiple error codes and thus messages.
StatusDictionary = {
   '-A' : 'Service Mode Enabled',
   '-H' : 'Heating Mode Enabled',
   '=H' : 'Domestic Hot Water Mode Enabled',
   '0A' : 'Waiting...',
   '0C' : 'Preparing to Ignite the burner',
   '0E' : 'Waiting, anti-pendel',
   '0H' : 'Standby, ready to heat',
   '0L' : 'Adjusting Gas intake',
   '0U' : 'Starting up the unit',
}


#################################################################################
# Our Communication Functions
# The EMS protocol has a nasty feature to seperate messages, they make the
# Bus low for 1.1-1.2 ms which is interpreted by RS232 as sending 11 or so bits of value 0.
# We can detect that by explicitly enabling PARMRK, the character parity or
# framing error detect feature in termios. 
# It will replace this "BREAK" seen as parity or frame error with 3 bytes:
# 0xff 0x00 0x00. This will be used as our message seperator!
#################################################################################
def StartEMS():
   MyEMS = serial.Serial ("/dev/serial0", 9600) 
   attr= termios.tcgetattr(MyEMS.fd)
   attr[0]|= termios.PARMRK
   termios.tcsetattr(MyEMS.fd, termios.TCSANOW, attr )
   return(MyEMS)

def StopEMS(MyEMS):
   MyEMS.close()        

#################################################################################
# Some helper functions to calculate and check the CRC value of the message
#################################################################################

def CalculateNefitEMSCRC(SerialBuffer):
  # First remove the last byte of the buffer, crc is calculated over the part of the message
  # before that.
  StrippedBuffer=SerialBuffer[:-1]
  crc = 0x0
  d = 0x0
  for Entry in StrippedBuffer:
    d = 0;
    if ( crc & 0x80 ):
       crc ^= 12
       d = 1
    crc  = (crc << 1) & 0xfe
    crc |= d
    crc ^= int(Entry,16)
  return (crc)

def CRCOK(SerialBuffer):
   CRC = CalculateNefitEMSCRC(SerialBuffer)
   BufferLength = len(SerialBuffer)
   OK = (CRC==int(SerialBuffer[(BufferLength-1)],16))
   return (OK)

#################################################################################
# This function will read the next message from the serial port.
# Broadcast Messages containing only the slave ID (<4 bytes) that indicates 
# when a Bus slave is allowed to send data are beeing ignored for the 
# moment, no plans to write on the bus yet...
#################################################################################
def NextMessage(MyEMS):
   MessageReceived = False
   Message = []
   BreakBytes = 0
   while not MessageReceived:
      char = numpy.uint8(ord(MyEMS.read(1))).__hex__()
      if BreakBytes == 0:
	 if char == '0xff':
            BreakBytes = 1
	 else:
            Message.append(char)
      elif BreakBytes == 1:
	 if char == '0x0':
            BreakBytes = 2
	 else:
	    Message.append('0xff')
	    Message.append(char)
	    BreakBytes = 0
      elif BreakBytes == 2:
	 if char == '0x0':
	    #Complete Break Received
	    BreakBytes = 0
	    # Now check for message with valid CRC meaning we have a complete message
	    # if we don't have that, we throw it away and continue receiving the next
	    # message after this break.
	    if len(Message) > 4 and CRCOK(Message):
	       MessageReceived = True
	    else:
	       Message = []  
	 else:
	    Message.append('0xff')
	    Message.append('0x0')
	    Message.append(char)
	    BreakBytes = 0
      #print('Char='+char.__str__()+', BreakBytes='+BreakBytes.__str__()+', MessageReceived='+MessageReceived.__str__()+' ,MessageStarted='+MessageStarted.__str__())
   return(Message)

#################################################################################
# This function will return the Next Message of interest, other messages are
# skipped. It will use the message parse dispatcher dictionary and only 
# return the messages that have a key in that dictionary.
#################################################################################
def NextMessageOfInterest(MyEMS):
   MessageReceived = False
   while not MessageReceived:
      Message = NextMessage(MyEMS)
      if MessageParseDispatcher.has_key(Message[2]):
	 MessageReceived = True
      #Comment out to print messages not parsed.
      #else:
	 #print('Unknown Message ID='+Message[2]+', Message='+Message.__str__())
   return (Message)

#################################################################################
# Message Data Conversion Functions, input is list of numpy.uint8
#################################################################################   

def ConvertToFloat(MsgData, scalar):
   length=len(MsgData)
   if length == 1:
      data=(float(int(MsgData[0],16))*scalar)
   elif length == 2:
      data=(float((256*int(MsgData[0],16))+int(MsgData[1],16))*scalar)
   else:
      #unknown, let's return 0.0 for now.
      data=float(0.0)
   return(data)

def ConvertToint(MsgData):
   length=len(MsgData)
   if length == 1:
      data=(int(MsgData[0],16))
   elif length == 2:
      data=(256*int(MsgData[0],16)+int(MsgData[1],16))
   elif length == 3:
      data=(65536*int(MsgData[0],16)+(256*int(MsgData[1],16))+int(MsgData[2],16))
   else:
      #unknown, let's return 0 for now.
      data=int(0)
   return(data)

#################################################################################
# Update Domoticz, try, except here to be robust for network problems or problems on
# the domoticz host.
#################################################################################
def UpdateDomoticz(URL, Value):
   try:
      Page=urllib2.urlopen(URL+Value.__str__(), context=UnverifiedContext)
      DataString=Page.read()
   except (urllib2.HTTPError, urllib2.URLError) as fout:
      print("Error: "+str(fout)+" URL: "+URL)

#################################################################################
# Our Message Parse Functions
#################################################################################
def UBAMonitorFast(Msg):
   Result = dict()
   #First do sanity check on MsgID, if ok parse the message.
   if (Msg[2] == '0x18'):
      Result['FlowTemperature']=ConvertToFloat([Msg[5],Msg[6]],0.1)
      Result['BurnerDutyCycle']=ConvertToFloat([Msg[8]],0.01)
      Result['Boiler']=ConvertToFloat([Msg[15],Msg[16]],0.1)
      Result['FlowReturnTemperature']=ConvertToFloat([Msg[17],Msg[18]],0.1)
      Result['IonizationCurrent']=ConvertToFloat([Msg[19],Msg[20]],0.1)
      Result['Pressure']=ConvertToFloat([Msg[21]],0.1)
      Result['StatusCode']=(chr(int(Msg[22],16))+(chr(int(Msg[23],16))))
      Result['ErrorCode']=ConvertToint([Msg[24],Msg[25]])
      Result['DeltaT']=Result['FlowTemperature']-Result['FlowReturnTemperature']
      if StatusDictionary.has_key(Result['StatusCode']):
         Result['StatusText']=StatusDictionary[Result['StatusCode']]
      #Result['WarmWaterOut']=(float((256*int(Msg[13],16))+int(Msg[14],16))/10)
      
      #Update Domoticz:
      UpdateDomoticz(DeltaTURL, Result['DeltaT']) 
      UpdateDomoticz(FlowTemperatureURL, Result['FlowTemperature']) 
      UpdateDomoticz(ReturnFlowTemperatureURL, Result['FlowReturnTemperature']) 
      UpdateDomoticz(BurnerDutyCycleURL, Result['BurnerDutyCycle']) 
      UpdateDomoticz(SystemPressureURL, Result['Pressure']) 
      UpdateDomoticz(IonizationCurrentURL, Result['IonizationCurrent']) 
   return(Result)

def UBAMonitorSlow(Msg):
   Result = dict()
   #First do sanity check on MsgID, if ok parse the message.
   if (Msg[2] == '0x19'):
      Result['BurnerOutWaterTemperature']=ConvertToFloat([Msg[6],Msg[7]],0.1)
      Result['PumpDutyCycle']=ConvertToFloat([Msg[13]],0.01)
      Result['BurnerStarts']=ConvertToint([Msg[14],Msg[15],Msg[16]])
      Result['BurnerRuntimeInMinutes']=ConvertToint([Msg[17],Msg[18],Msg[19]])
      Result['HeatingRuntimeInMinutes']=ConvertToint([Msg[23],Msg[24],Msg[25]])
      Result['HotWaterRuntimeInMinutes']=Result['BurnerRuntimeInMinutes']-Result['HeatingRuntimeInMinutes']
      UpdateDomoticz(BurnerTemperatureURL, Result['BurnerOutWaterTemperature'])
      UpdateDomoticz(PumpDutyCycleURL, Result['PumpDutyCycle'])
   return(Result)

def Moduline300Status(Msg):
   Result = dict()
   #First do sanity check on MsgID, if ok parse the message.
   if (Msg[2] == '0x91'):
      Result['Setpoint']=ConvertToFloat([Msg[5]],0.5)
      Result['Actual']=ConvertToFloat([Msg[15],Msg[16]],0.1)
      UpdateDomoticz(RoomTemperatureURL, Result['Actual'])
      UpdateDomoticz(RoomSetpointURL, Result['Setpoint'])
   return(Result)

def UBAMonitorWWMessage(Msg):
   Result = dict()
   #First do sanity check on MsgID, if ok parse the message.
   if (Msg[2] == '0x34'):
      Result['BoilerTemperature']=ConvertToFloat([Msg[7],Msg[8]],0.1)
      Result['WarmWaterOutTemperature']=ConvertToFloat([Msg[5],Msg[6]],0.1)
      Result['WarmWaterFlow']=ConvertToFloat([Msg[13]],0.1)
   return(Result)

# Dumping Raw Message, usefull for inpecting unknown message types
def Raw(Msg):
   Result = dict()
   Result['Raw']=Msg[4:]
   return(Result)

#################################################################################
# Our Parse Table, its a dictionary with the Message Typ as Key, an easy
# way to explicitly parse the messages, it is also the definition of what to
# parse and thus implicitly to skip anything else.
#################################################################################
MessageParseDispatcher = {
   '0x18': UBAMonitorFast,
   '0x19': UBAMonitorSlow,
   '0x34': UBAMonitorWWMessage,
   '0x91': Moduline300Status,
}


#################################################################################
# Main Program
#################################################################################

MyEMS=StartEMS()

#Flush to start with an empty buffer, no old data required.
MyEMS.flushInput()
while (1):
   Result = NextMessageOfInterest(MyEMS)
   ProcessedResult = MessageParseDispatcher[Result[2]](Result)
   Now = datetime.datetime.now().strftime("%H:%M:%S")
   print(Now+', Data='+ProcessedResult.__str__())
   time.sleep(1)

StopEMS(MyEMS)
