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
import httplib

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
StatusURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=84&nvalue=0&svalue="
RuntimeHeatingURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=85&nvalue=0&svalue="
RuntimeHotWaterURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=86&nvalue=0&svalue="
BurnerStartsURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=87&nvalue=0&svalue="
HotWaterFlowURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=89&nvalue=0&svalue="
SystemEfficiencyURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=90&nvalue=0&svalue="
SystemStatusURL=DomoticzHost+"json.htm?type=command&param=udevice&idx=91&nvalue=0&svalue="

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


# This dictionary translate the return temperature into a heater efficiency %
# by taking into account the both heating efficiency as well as water vapor condensation
# that occurs within HR heaters as function of return water temperature.
# Note, the numbers are linear estimation/interpolation based on several distinct numbers between
# 10C - 60C.
EfficiencyDictionary = {
   10 : '111',
   11 : '110.8',
   12 : '110.6',
   13 : '110.4',
   14 : '110.2',
   15 : '110',
   16 : '109.8',
   17 : '109.6',
   18 : '109.4',
   19 : '109.2',
   20 : '109',
   21 : '108.7',
   22 : '108.4',
   23 : '108.1',
   24 : '107.8',
   25 : '107.5',
   26 : '107.2',
   27 : '106.9',
   28 : '106.6',
   29 : '106.3',
   30 : '106',
   31 : '105.5',
   32 : '105',
   33 : '104.5',
   34 : '104',
   35 : '103.5',
   36 : '103',
   37 : '102.5',
   38 : '102',
   39 : '101.5',
   40 : '101',
   41 : '100.2',
   42 : '99.4',
   43 : '98.6',
   44 : '97.8',
   45 : '97',
   46 : '96.2',
   47 : '95.4',
   48 : '94.6',
   49 : '93.8',
   50 : '93',
   51 : '89.7',
   52 : '89.4',
   53 : '89.1',
   54 : '88.8',
   55 : '88.5',
   56 : '88.2',
   57 : '87.9',
   58 : '87.6',
   59 : '87.3',
   60 : '87',
   61 : '86.7',
   62 : '86.4',
   63 : '86.1',
   64 : '85.8',
   65 : '85.5',
   66 : '85.2',
   67 : '84.9',
   68 : '84.6',
   69 : '84.3',
   70 : '84',
   71 : '83.7',
   72 : '83.4',
   73 : '83.1',
   74 : '82.8',
   75 : '82.5',
   76 : '82.2',
   77 : '81.9',
   78 : '81.6',
   79 : '81.3',
   80 : '81',
   81 : '80.7',
   82 : '80.4',
   83 : '80.1',
   84 : '79.8',
   85 : '79.5',
   86 : '79.2',
   87 : '78.9',
   88 : '78.6',
   89 : '78.3',
   90 : '78',
   91 : '77.7',
   92 : '77.4',
   93 : '77.1',
   94 : '76.8',
   95 : '76.5',
   96 : '76.2',
   97 : '75.9',
   98 : '75.6',
   99 : '75.3',
   100 : '75',
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
   if not OK:
      print('CRC not OK, Expected='+SerialBuffer[(BufferLength-1)]+', Calculated='+CRC.__str__()+', Message='+SerialBuffer.__str__())
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
	    if len(Message) > 4: 
	       #First PostProcess the messsage to remove redundant 0xff before calculating CRC.
               Message = PostProcessMessage(Message)
	       if CRCOK(Message):
	          MessageReceived = True
	       else:
	          Message = []  
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
# Post Process Message Data, remove double 0xff 0xff in case the message contained 
# an actual 0xff byte. (Because our PARMRK termios setting as side effect will
# replace each 0xff in the bytestream with 0xff 0xff to be able to distinguish
# between a parity frame error and a message containing a 0xff 0x0 0x0 sequence.) 
#################################################################################   
def PostProcessMessage(Message):
   if '0xff' in Message:
      ByteRemoved = False
      NewMessage = []
      for byte in Message:
	 if byte == '0xff': 
	    if not ByteRemoved:
	       ByteRemoved = True
	    else:
	       ByteRemoved = False
	       NewMessage.append(byte)
	 else:
            NewMessage.append(byte)
   else:
      NewMessage = Message
   return(NewMessage)
      
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
   except (urllib2.HTTPError, urllib2.URLError, httplib.BadStatusLine) as fout:
      print("Error: "+str(fout)+" URL: "+URL)

def UpdateDomoticzText(URL, Text):
   TextValue=urllib2.quote(Text)
   try:
      Page=urllib2.urlopen(URL+TextValue, context=UnverifiedContext)
      DataString=Page.read()
   except (urllib2.HTTPError, urllib2.URLError, httplib.BadStatusLine) as fout:
      print("Error: "+str(fout)+" URL: "+URL)

#################################################################################
# Calculate System efficiency by interpolation i.c.w. lookup dictionary
#################################################################################
def CalculateSystemEfficiency(Temperature):
   LowValue=float(EfficiencyDictionary[(int(Temperature))])
   HighValue=float(EfficiencyDictionary[(int(Temperature)+1)])
   Fraction=Temperature-(int(Temperature))
   #Efficiency goes down as temperature goes up, the high and low refer to the
   #temperature, not the efficiency. Fraction*(HighValue-LowValue) is a negative number.
   Efficiency=LowValue+(Fraction*(HighValue-LowValue))
   return(Efficiency)

#################################################################################
# Our Message Parse Functions
#################################################################################
def UBAMonitorFast(Msg):
   Result = dict()
   #First do sanity check on MsgID and size, if ok parse the message.
   MsgSize=len(Msg)
   if (Msg[2] == '0x18') and MsgSize == 30:
      Result['RequestedFlowTemperature']=ConvertToFloat([Msg[4]],1.0)
      Result['FlowTemperature']=ConvertToFloat([Msg[5],Msg[6]],0.1)
      Result['RequestedBurnerDutyCycle']=ConvertToFloat([Msg[7]],1.0)
      Result['BurnerDutyCycle']=ConvertToFloat([Msg[8]],1.0)
      Result['Boiler']=ConvertToFloat([Msg[15],Msg[16]],0.1)
      Result['FlowReturnTemperature']=ConvertToFloat([Msg[17],Msg[18]],0.1)
      Result['IonizationCurrent']=ConvertToFloat([Msg[19],Msg[20]],0.1)
      Result['Pressure']=ConvertToFloat([Msg[21]],0.1)
      Result['StatusCode']=(chr(int(Msg[22],16))+(chr(int(Msg[23],16))))
      Result['ErrorCode']=ConvertToint([Msg[24],Msg[25]])
      Result['DeltaT']=Result['FlowTemperature']-Result['FlowReturnTemperature']
      if StatusDictionary.has_key(Result['StatusCode']):
         Result['StatusText']=StatusDictionary[Result['StatusCode']]
         UpdateDomoticzText(StatusURL, Result['StatusText'])
	 if Result['StatusCode'] == '-H':
	    Result['SystemStatus'] = 1
	 elif Result['StatusCode'] == '=H':
	    Result['SystemStatus'] = 2
	 else:
	    Result['SystemStatus'] = 0
	 UpdateDomoticz(SystemStatusURL, Result['SystemStatus'])
      #Result['WarmWaterOut']=(float((256*int(Msg[13],16))+int(Msg[14],16))/10)
      UpdateDomoticz(DeltaTURL, Result['DeltaT']) 
      UpdateDomoticz(FlowTemperatureURL, Result['FlowTemperature']) 
      UpdateDomoticz(ReturnFlowTemperatureURL, Result['FlowReturnTemperature']) 
      UpdateDomoticz(BurnerDutyCycleURL, Result['BurnerDutyCycle']) 
      UpdateDomoticz(SystemPressureURL, Result['Pressure']) 
      UpdateDomoticz(IonizationCurrentURL, Result['IonizationCurrent'])
      Result['Efficiency']=CalculateSystemEfficiency(float(Result['FlowReturnTemperature']))
      UpdateDomoticz(SystemEfficiencyURL, Result['Efficiency'])
   return(Result)

def UBAMonitorSlow(Msg):
   Result = dict()
   #First do sanity check on MsgID and size, if ok parse the message.
   MsgSize=len(Msg)
   if (Msg[2] == '0x19') and MsgSize == 32:
      Result['BurnerOutWaterTemperature']=ConvertToFloat([Msg[6],Msg[7]],0.1)
      Result['PumpDutyCycle']=ConvertToFloat([Msg[13]],1.0)
      Result['BurnerStarts']=ConvertToint([Msg[14],Msg[15],Msg[16]])
      Result['BurnerRuntimeInMinutes']=ConvertToint([Msg[17],Msg[18],Msg[19]])
      Result['HeatingRuntimeInMinutes']=ConvertToint([Msg[23],Msg[24],Msg[25]])
      Result['HotWaterRuntimeInMinutes']=Result['BurnerRuntimeInMinutes']-Result['HeatingRuntimeInMinutes']
      UpdateDomoticz(BurnerTemperatureURL, Result['BurnerOutWaterTemperature'])
      UpdateDomoticz(PumpDutyCycleURL, Result['PumpDutyCycle'])
      UpdateDomoticz(RuntimeHeatingURL, (float(Result['HeatingRuntimeInMinutes'])/60))
      UpdateDomoticz(RuntimeHotWaterURL, (float(Result['HotWaterRuntimeInMinutes'])/60))
      UpdateDomoticz(BurnerStartsURL, Result['BurnerStarts'])
   return(Result)

def Moduline300Status(Msg):
   Result = dict()
   #First do sanity check on MsgID and size, if ok parse the message.
   MsgSize=len(Msg)
   if (Msg[2] == '0x91') and MsgSize == 19:
      Result['Setpoint']=ConvertToFloat([Msg[5]],0.5)
      Result['Actual']=ConvertToFloat([Msg[15],Msg[16]],0.1)
      UpdateDomoticz(RoomTemperatureURL, Result['Actual'])
      UpdateDomoticz(RoomSetpointURL, Result['Setpoint'])
   return(Result)

def UBAMonitorWWMessage(Msg):
   Result = dict()
   #First do sanity check on MsgID and size, if ok parse the message.
   MsgSize=len(Msg)
   if (Msg[2] == '0x34') and MsgSize == 22:
      Result['BoilerTemperature']=ConvertToFloat([Msg[7],Msg[8]],0.1)
      Result['WarmWaterOutTemperature']=ConvertToFloat([Msg[5],Msg[6]],0.1)
      Result['WarmWaterFlow']=ConvertToFloat([Msg[13]],0.1)
      UpdateDomoticz(HotWaterFlowURL, Result['WarmWaterFlow'])
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
# Messages seen on the bus, nothing done with yet, for future reference.
#################################################################################
# RCTimeMessage:
#Unknown Message ID=0x6, Message=['0x17', '0x0', '0x6', '0x0', '0x13', '0x2', '0xd', '0x9', '0x31', '0x17', '0x5', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0xb7']
# ?:
#Unknown Message ID=0x7, Message=['0x8', '0x0', '0x7', '0x0', '0x3', '0x80', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x6b']
# UBASollwerte:
#Unknown Message ID=0x1a, Message=['0x17', '0x8', '0x1a', '0x0', '0x0', '0x0', '0x0', '0x0', '0x3a']
# UBA WartungsMeldung:
#Unknown Message ID=0x1c, Message=['0x8', '0x0', '0x1c', '0x0', '0x80', '0x1', '0x1', '0x1', '0x11', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0xed']
# Flags ?:
#Unknown Message ID=0x35, Message=['0x17', '0x8', '0x35', '0x0', '0x11', '0x0', '0xc1']
# ?:
#Unknown Message ID=0xa2, Message=['0x17', '0x0', '0xa2', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x0', '0x51']
# RC Temp Message?
#Unknown Message ID=0xa3, Message=['0x17', '0x0', '0xa3', '0x0', '0x0', '0x0', '0x0', '0x77']




#################################################################################
# Main Program
#################################################################################

MyEMS=StartEMS()

#Flush to start with an empty buffer, no old data required.
MyEMS.flushInput()
while (1):
   Result = NextMessageOfInterest(MyEMS)
   #MessageLength=len(Result)
   ProcessedResult = MessageParseDispatcher[Result[2]](Result)
   Now = datetime.datetime.now().strftime("%H:%M:%S")
   #print(Now+', Size='+MessageLength.__str__()+', MsgType='+[Result[2]].__str__()+', Data='+ProcessedResult.__str__())
   print(Now+', Data='+ProcessedResult.__str__())
   time.sleep(1)

StopEMS(MyEMS)
