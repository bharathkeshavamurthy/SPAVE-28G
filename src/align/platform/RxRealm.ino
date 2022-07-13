/*
 * Eitri: The metal worker in Project Odin.
 * An Arduino sketch to perform the following tasks: 
 * 1. Publish the GPS readings received from the SparkFun UBlox ZED-F9P RTK-GPS module;
 * 2. Receive the required angles of rotation (yaw and pitch) from the XXRealm Python Controller over Bluetooth/USB; and
 * 3. Yaw and Pitch rotations using the SparkFun Blackboard Rev C, IMU BNO080, and ServoCity continuous servos -- along with IMU logging.
 * 
 * Eitri_Rx: With GPS | With Bluetooth
 * 
 * Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
 * Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
 * Copyright (c) 2021. All Rights Reserved.
*/

/* Pre-processor Directives */
#include <Wire.h>
#include <Servo.h>
#include <SoftwareSerial.h>
#include "SparkFun_Ublox_Arduino_Library.h"
#include "SparkFun_BNO080_Arduino_Library.h"

/* Rotation Control Logic Definitions */
#define STEADY_STATE 5                     // The number of initial sample reads for steady state
#define READ_EQUILIBRIUM 5                 // The number of samples to be read before we decide to use the IMU report

/* Servo PWM Control Values */
#define MAX_PWM 2250                       // Forward Motion (PWM ON pulse duration)
#define MID_PWM 1500                       // Stop Motion (PWM ON pulse duration)
#define MIN_PWM 750                        // Backward Motion (PWM ON pulse duration)

/* IMU Instance Creation
 * Servo control pin definitions | Servo (X and Z) instance creation
 * X = Yaw movements | Z = Pitch & Roll movements | For pitch movements, place the object being rotated perpendicular to the pan plate -- in the X-Z plane | Manual height adjustments
*/
BNO080 bno080;
int pinX = 9, pinZ = 10;
Servo servoX, servoZ;

/* UBlox ZED-F9P Instance Creation */
SFE_UBLOX_GPS ublox;

/* SparkFun Bluetooth Mate Silver Instance Creation */
int bluetoothTx = 2;                                                                // TX-O pin of bluetooth mate, Arduino D2
int bluetoothRx = 3;                                                                // RX-I pin of bluetooth mate, Arduino D3
SoftwareSerial bluetooth(bluetoothTx, bluetoothRx);

/* Core Rotational Variables */
float yaw = 0.0, prevYaw = 0.0, pitch = 0.0, prevPitch = 0.0, yawDelta = 0.0;       // The estimated yaw angle and its previous time-step counterpart | The estimated pitch angle and its previous time-step counterpart | A variable to hold the difference between current yaw and previous yaw (mandate-triggered) in order to evaluate the stopping criterion
float tmpYaw = 720.0, requiredYawAngle = 0.0, currentYawAngle = 0.0;                // The core yaw axis variables facilitating motor control
float tmpPitch = 720.0, requiredPitchAngle = 0.0, currentPitchAngle = 0.0;          // The core pitch axis variables facilitating motor control
float yawReconfigRequirement = 0.0, pitchReconfigRequirement = 0.0;                 // The reconfigured mandates in terms of the amount of angle that needs to be traversed -- either in the anti-clockwise direction or in the clockwise direction
float aclkYawRotation = 0.0, clkYawRotation = 0.0;                                  // The movement variables that encapsulate the amount of yaw rotation in the anti-clockwise & clockwise directions, respectively
float aclkPitchRotation = 0.0, clkPitchRotation = 0.0;                              // The movement variables that encapsulate the amount of pitch rotation in the anti-clockwise & clockwise directions, respectively
boolean motorsInitiated = false, mandateAchieved = true;                            // A flag to indicate if the motors have been initiated or not | A flag to indicate that the overall correction mandate from the XXRealm Python Controller has been met
boolean proceedToJunk = false;                                                      // A flag to indicate if the third scenario emulation has been enabled...
unsigned int sampleCounter = 0, steadyStateCounter = 0;                             // A counter for ensuring that the IMU values are read post-equilibrium | A counter to force steady-state reads...

/* Core GPS Variable */
long prevTime = 0;                                                                  // A previous time member in order to limit the amount of traffic coming in from the UBlox ZED-F9P RTK-GPS module

/* Core Mandate Extraction Variables */
const byte numChars = 20;                                                           // The length of the mandate (correction string from the XXRealm Python Controller) character array
char receivedChars[numChars];                                                       // The mandate (correction string from the XXRealm Python Controller) character array
boolean newData = false;                                                            // A flag to indicate that a new mandate has been received

/* The setup routine */
void setup() {
  bluetooth.begin(115200);                                                          // The Bluetooth Mate defaults to 115200bps
  while (!bluetooth);                                                               // Wait for the bluetooth instance to begin
  bluetooth.print("$");                                                             // Print three times individually
  bluetooth.print("$");
  bluetooth.print("$");                                                             // Enter command mode
  delay(100);                                                                       // Short delay, wait for the Mate to send back CMD
  bluetooth.println("U,9600,N");                                                    // Temporarily Change the baudrate to 9600bps, no parity
  bluetooth.begin(9600);                                                            // Start bluetooth serial at 9600bps
  while (!bluetooth);                                                               // Wait for the bluetooth instance to begin
  
  Wire.begin();                                                                     // Begin the Wire instance to start reading the I2C interface...
  
  if (!bno080.begin()) {                                                            // Check for the IMU at the I2C interface
    return;
  }
  Wire.setClock(400000);                                                            // I2C Data Rate = 400kHz
  bno080.calibrateAll();                                                            // Turn on calibration for Accelerometer, Gyroscope, and Magnetometer
  bno080.enableGameRotationVector(1);                                               // IMU Refresh for rotation vector data at 1ms
  bno080.enableMagnetometer(1);                                                     // IMU Refresh for magnetometer data at 1ms
  
  if (ublox.begin(Wire) == false) {                                                 // Check to see if the ZED-F9P RTK-GPS module is connected -- if not, default to the pre-defined values
    return;
  }
  ublox.setI2COutput(COM_TYPE_UBX);                                                 // Set the I2C port to output UBX only (turn off NMEA noise)
  
  servoX.attach(pinX);                                                              // Attach to servoX | Reset to zero motion | Yaw Servo
  servoX.writeMicroseconds(MID_PWM);
  servoZ.attach(pinZ);                                                              // Attach to servoZ | Reset to zero motion | Pitch Servo
  servoZ.writeMicroseconds(MID_PWM);
}

/* The loop routine */
void loop() {
  /* 
   * Data extraction and logging:
   * 1. Read the Quaternion Complex Vector values [i, j, k, real] and Normalize them;
   * 2. Parse Input Report from BNO080 --> Bosch Sensor Hub Transport Protocol (SHTP) over I2C --> Parse raw quaternion values if the channel is CHANNEL_GYRO in the SHTP Header OR if the SHTP Payload is referenced by SENSOR_*_ROTATION_VECTOR; and
   * 3. Use the BNO080 Library for this | Modify the C++ code if needed with the Q-values | https://github.com/fm4dd/pi-bno080 | https://github.com/sparkfun/SparkFun_BNO080_Arduino_Library/blob/master/src/SparkFun_BNO080_Arduino_Library.cpp.
  */
  if (bno080.dataAvailable()) {
    float quatI = bno080.getQuatI();
    float quatJ = bno080.getQuatJ();
    float quatK = bno080.getQuatK();
    float quatReal = bno080.getQuatReal();
    float norm = sqrt( (quatI * quatI) + (quatJ * quatJ) + (quatK * quatK) + (quatReal * quatReal) );
    quatI /= norm;
    quatJ /= norm;
    quatK /= norm;
    quatReal /= norm;
    yaw = atan2( ( 2.0 * ( (quatReal * quatK) + (quatI * quatJ) ) ), ( 1.0 - ( 2.0 * ( (quatJ * quatJ) + (quatK * quatK) ) ) ) ) * (180 / PI);     // Determine the Yaw Angle (in degrees)
    pitch = atan2( ( 2.0 * ( (quatReal * quatI) + (quatJ * quatK) ) ), ( 1.0 - ( 2.0 * ( (quatI * quatI) + (quatJ * quatJ) ) ) ) ) * (180 / PI);   // Determine the Pitch Angle (in degrees)
    sampleCounter += 1;                                                                                                                            // Update the sample counter
    steadyStateCounter += 1;
  }

  if (steadyStateCounter < STEADY_STATE) {
    return;
  }
    
  /* Mandate Extraction */
  recvWithStartEndMarkers();
  if (newData == true) {
    tmpYaw = atof(strtok(receivedChars, "#"));
    tmpPitch = atof(strtok(NULL, "#"));
    newData = false;
  }

  /* Mandate Execution Dispatch */
  if ( (tmpYaw != 720.0) && (tmpPitch != 720.0) && ( (tmpYaw != requiredYawAngle) || (tmpPitch != requiredPitchAngle) ) ) {
    if (motorsInitiated == true) {
      servoX.writeMicroseconds(MID_PWM);
      motorsInitiated = false;
    }
    mandateAchieved = false;
    proceedToJunk = false;
    sampleCounter = 0;
    
    requiredYawAngle = tmpYaw;
    requiredPitchAngle = tmpPitch;
    sampleCounter = 0;                                                                                                   // Start fresh with new samples in order to achieve READ_EQUILIBRIUM
    prevYaw = yaw;                                                                                                       // Raw, unfiltered previous yaw store
    prevPitch = pitch;                                                                                                   // Raw, unfiltered previous pitch store
  }
  
  /* Yaw Rotation Control */
  if ( (mandateAchieved == false) && (proceedToJunk == false) && (sampleCounter >= READ_EQUILIBRIUM) ) {
    currentYawAngle = (yaw < 0.0) ? (360.0 + yaw) : yaw;
    if (motorsInitiated == false) {
      if (currentYawAngle <= requiredYawAngle) {                                                                         // Variant-1
        aclkYawRotation = requiredYawAngle - currentYawAngle;
        clkYawRotation = 360 - (requiredYawAngle - currentYawAngle);
      } else {                                                                                                           // Variant-2
        clkYawRotation = currentYawAngle - requiredYawAngle;
        aclkYawRotation = 360 - (currentYawAngle - requiredYawAngle);
      }
      yawReconfigRequirement = (aclkYawRotation <= clkYawRotation) ? aclkYawRotation : clkYawRotation;
      servoX.writeMicroseconds( (aclkYawRotation <= clkYawRotation) ? MAX_PWM : MIN_PWM );                               // Use the optimal rotation direction (a simple comparison-based decision heuristic)
      motorsInitiated = true;
    } else {
      yawDelta = ( abs(yaw - prevYaw) > 180.0 ) ? ( 360.0 - abs(yaw - prevYaw) ) : abs(yaw - prevYaw);
      if ( yawDelta >= yawReconfigRequirement ) {                                                                        // Stop, once the mandate has been met -- and, move on to pitch adjustments...
        servoX.writeMicroseconds(MID_PWM);
        proceedToJunk = true;
        motorsInitiated = false;
      }
    }
    sampleCounter = 0; 
  }

  /* Junk (third scenario) emulation */
  if ( (mandateAchieved == false) && (proceedToJunk == true) && (sampleCounter >= READ_EQUILIBRIUM) ) {
    proceedToJunk = false;
    mandateAchieved = true;
    sampleCounter = 0;
    delay(1);
  }
  
   /* Query module only every second | The ZED-F9P RTK-GPS module only responds when a new position is available
   *  SparkFun UBlox GPS Library API calls:
   *    getHighResLatitude: Returns the latitude from HPPOSLLH as an int32_t in degrees * 10^-7;
   *    getHighResLatitudeHp: Returns the high resolution component of latitude from HPPOSLLH as an int8_t in degrees * 10^-9;
   *    getHighResLongitude: Returns the longitude from HPPOSLLH as an int32_t in degrees * 10^-7;
   *    getHighResLongitudeHp: Returns the high resolution component of longitude from HPPOSLLH as an int8_t in degrees * 10^-9;
   *    getElipsoid: Returns the height above ellipsoid as an int32_t in mm;
   *    getElipsoidHp: Returns the high resolution component of the height above ellipsoid as an int8_t in mm * 10^-1;
   *    getMeanSeaLevel: Returns the height above mean sea level as an int32_t in mm;
   *    getMeanSeaLevelHp: Returns the high resolution component of the height above mean sea level as an int8_t in mm * 10^-1; and
   *    getHorizontalAccuracy: Returns the horizontal accuracy estimate from HPPOSLLH as an uint32_t in mm * 10^-1. These are a few relevant calls in this GPS API.
  */
  if ( (mandateAchieved == true) && (proceedToJunk == false) && (motorsInitiated == false) && ( (millis() - prevTime) > 1000 ) ) {
    prevTime = millis();
    
    servoX.detach();
    servoZ.detach();
    
    double d_lat, d_lon;
    float f_ellipsoid, f_msl;
      
    bool is_gnss_fix_ok = ublox.getGnssFixOk();
    uint8_t siv = ublox.getSIV();
    uint8_t fix_type = ublox.getFixType();
    uint8_t carrier_solution_type = ublox.getCarrierSolutionType();
    
    int32_t latitude = ublox.getHighResLatitude();
    int8_t latitudeHp = ublox.getHighResLatitudeHp();
    d_lat = ((double)latitude) / 10000000.0;
    d_lat += ((double)latitudeHp) / 1000000000.0;
    
    int32_t longitude = ublox.getHighResLongitude();
    int8_t longitudeHp = ublox.getHighResLongitudeHp();
    d_lon = ((double)longitude) / 10000000.0;
    d_lon += ((double)longitudeHp) / 1000000000.0;
    
    int32_t ellipsoid = ublox.getElipsoid();
    int8_t ellipsoidHp = ublox.getElipsoidHp();
    f_ellipsoid = (ellipsoid * 10) + ellipsoidHp;
    f_ellipsoid = f_ellipsoid / 10000.0;
    
    int32_t msl = ublox.getMeanSeaLevel();
    int8_t mslHp = ublox.getMeanSeaLevelHp();
    f_msl = (msl * 10) + mslHp;
    f_msl = f_msl / 10000.0;

    bluetooth.print("$");
    bluetooth.print(is_gnss_fix_ok);
    bluetooth.print(",");
    bluetooth.print(siv);
    bluetooth.print(",");
    bluetooth.print(fix_type);
    bluetooth.print(",");
    bluetooth.print(carrier_solution_type);
    bluetooth.print(",");
    
    bluetooth.print(latitude);
    bluetooth.print(",");
    bluetooth.print(latitudeHp);
    bluetooth.print(",");
    bluetooth.print(d_lat, 9);
    bluetooth.print(",");
    
    bluetooth.print(longitude);
    bluetooth.print(",");
    bluetooth.print(longitudeHp);
    bluetooth.print(",");
    bluetooth.print(d_lon, 9);
    bluetooth.print(",");
    
    bluetooth.print(ellipsoid);
    bluetooth.print(",");
    bluetooth.print(ellipsoidHp);
    bluetooth.print(",");
    bluetooth.print(f_ellipsoid, 4);
    bluetooth.print(",");
    
    bluetooth.print(msl);
    bluetooth.print(",");
    bluetooth.print(mslHp);
    bluetooth.print(",");
    bluetooth.print(f_msl, 4);
    bluetooth.print("\n");

    servoX.attach(pinX);
    servoX.writeMicroseconds(MID_PWM);
    servoZ.attach(pinZ);
    servoZ.writeMicroseconds(MID_PWM);
  }
}

/* Mandate (Correction String from XXRealm Python Controller) Read Routine */
void recvWithStartEndMarkers() {
  static boolean recvInProgress = false;
  static byte ndx = 0;
  char startMarker = '<';
  char endMarker = '>';
  char rc;
  while ( (bluetooth.available() > 0) && (newData == false) ) {
    rc = bluetooth.read();
    if (recvInProgress == true) {
      if (rc != endMarker) {
        receivedChars[ndx] = rc;
        ndx++;
        if (ndx >= numChars) {
          ndx = numChars - 1;
        }
      }
      else {
        recvInProgress = false;
        ndx = 0;
        newData = true;
      }
    }
    else if (rc == startMarker) {
      recvInProgress = true;
    }
  }
}
