/*
  Get the high precision geodetic solution for latitude and longitude using double
  By: Nathan Seidle
  Modified by: Paul Clark (PaulZC)
  SparkFun Electronics
  Date: April 17th, 2020
  License: MIT. See license file for more information but you can
  basically do whatever you want with this code.

  This example shows how to inspect the accuracy of the high-precision
  positional solution. Please see below for information about the units.

  ** This example will only work correctly on platforms which support 64-bit double **

  Feel like supporting open source hardware?
  Buy a board from SparkFun!
  ZED-F9P RTK2: https://www.sparkfun.com/products/15136
  NEO-M8P RTK: https://www.sparkfun.com/products/15005

  Hardware Connections:
  Plug a Qwiic cable into the GPS and (e.g.) a Redboard Artemis https://www.sparkfun.com/products/15444
  or an Artemis Thing Plus https://www.sparkfun.com/products/15574
  If you don't have a platform with a Qwiic connection use the SparkFun Qwiic Breadboard Jumper (https://www.sparkfun.com/products/14425)
  Open the serial monitor at 115200 baud to see the output
*/

#include <Wire.h> // Needed for I2C to GPS

#define myWire Wire // This will work on the Redboard Artemis and the Artemis Thing Plus using Qwiic
//#define myWire Wire1 // Uncomment this line if you are using the extra SCL1/SDA1 pins (D17 and D16) on the Thing Plus

#include "SparkFun_Ublox_Arduino_Library.h" //http://librarymanager/All#SparkFun_u-blox_GNSS
SFE_UBLOX_GPS myGPS;

long lastTime = 0; //Simple local timer. Limits amount if I2C traffic to Ublox module.

void setup()
{
  Serial.begin(115200);
  while (!Serial); //Wait for user to open terminal

  myWire.begin();

  //myGPS.enableDebugging(Serial); // Uncomment this line to enable debug messages

  if (myGPS.begin(myWire) == false) //Connect to the Ublox module using Wire port
  {
    Serial.println(F("Ublox GPS not detected at default I2C address. Please check wiring. Freezing."));
    while (1)
      ;
  }

  // Check that this platform supports 64-bit (8 byte) double
  if (sizeof(double) < 8)
  {
    Serial.println(F("Warning! Your platform does not support 64-bit double."));
    Serial.println(F("The latitude and longitude will be inaccurate."));
  }

  myGPS.setI2COutput(COM_TYPE_UBX); //Set the I2C port to output UBX only (turn off NMEA noise)

  //myGPS.setNavigationFrequency(20); //Set output to 20 times a second

  byte rate = myGPS.getNavigationFrequency(); //Get the update rate of this module
  Serial.print("Current update rate: ");
  Serial.println(rate);

  //myGPS.saveConfiguration(); //Save the current settings to flash and BBR
}

void loop()
{
  //Query module only every second.
  //The module only responds when a new position is available.
  if (millis() - lastTime > 1000)
  {
    lastTime = millis(); //Update the timer

    // getHighResLatitude: returns the latitude from HPPOSLLH as an int32_t in degrees * 10^-7
    // getHighResLatitudeHp: returns the high resolution component of latitude from HPPOSLLH as an int8_t in degrees * 10^-9
    // getHighResLongitude: returns the longitude from HPPOSLLH as an int32_t in degrees * 10^-7
    // getHighResLongitudeHp: returns the high resolution component of longitude from HPPOSLLH as an int8_t in degrees * 10^-9
    // getElipsoid: returns the height above ellipsoid as an int32_t in mm
    // getElipsoidHp: returns the high resolution component of the height above ellipsoid as an int8_t in mm * 10^-1
    // getMeanSeaLevel: returns the height above mean sea level as an int32_t in mm
    // getMeanSeaLevelHp: returns the high resolution component of the height above mean sea level as an int8_t in mm * 10^-1
    // getHorizontalAccuracy: returns the horizontal accuracy estimate from HPPOSLLH as an uint32_t in mm * 10^-1

    // Defines storage for the lat and lon as double
    double d_lat; // latitude
    double d_lon; // longitude

    // Now define float storage for the heights and accuracy
    float f_ellipsoid;
    float f_msl;
    
    // First, let's collect the position data
    bool is_gnss_fix_ok = myGPS.getGnssFixOk();
    Serial.print("$");
    Serial.print(is_gnss_fix_ok);
    Serial.print(",");
    uint8_t siv = myGPS.getSIV();
    Serial.print(siv);
    Serial.print(",");
    uint8_t fix_type = myGPS.getFixType();
    Serial.print(fix_type);
    Serial.print(",");
    uint8_t carrier_solution_type = myGPS.getCarrierSolutionType();
    Serial.print(carrier_solution_type);
    Serial.print(",");
    int32_t latitude = myGPS.getHighResLatitude();
    Serial.print(latitude);
    Serial.print(",");
    int8_t latitudeHp = myGPS.getHighResLatitudeHp();
    Serial.print(latitudeHp);
    Serial.print(",");
    // Assemble the high precision latitude
    d_lat = ((double)latitude) / 10000000.0;      // Convert latitude from degrees * 10^-7 to degrees
    d_lat += ((double)latitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9)
    Serial.print(d_lat, 9);
    Serial.print(",");
    int32_t longitude = myGPS.getHighResLongitude();
    Serial.print(longitude);
    Serial.print(",");
    int8_t longitudeHp = myGPS.getHighResLongitudeHp();
    Serial.print(longitudeHp);
    Serial.print(",");
    // Assemble the high precision longitude
    d_lon = ((double)longitude) / 10000000.0;      // Convert longitude from degrees * 10^-7 to degrees
    d_lon += ((double)longitudeHp) / 1000000000.0; // Now add the high resolution component (degrees * 10^-9)
    Serial.print(d_lon, 9);
    Serial.print(",");
    int32_t ellipsoid = myGPS.getElipsoid();
    Serial.print(ellipsoid);
    Serial.print(",");
    int8_t ellipsoidHp = myGPS.getElipsoidHp();
    Serial.print(ellipsoidHp);
    Serial.print(",");
    f_ellipsoid = (ellipsoid * 10) + ellipsoidHp; // Calculate the height above ellipsoid in mm * 10^-1
    f_ellipsoid = f_ellipsoid / 10000.0;          // Now convert to m: Convert from mm * 10^-1 to m
    Serial.print(f_ellipsoid, 4);
    int32_t msl = myGPS.getMeanSeaLevel();
    Serial.print(msl);
    Serial.print(",");
    int8_t mslHp = myGPS.getMeanSeaLevelHp();
    Serial.print(mslHp);
    Serial.print(",");
    f_msl = (msl * 10) + mslHp;                  // Calculate the height above mean sea level in mm * 10^-1
    f_msl = f_msl / 10000.0;                     // Now convert to m: Convert from mm * 10^-1 to m
    Serial.print(f_msl, 4);
    int32_t speed = myGPS.getGroundSpeed();
    Serial.print(speed);
    Serial.print(",");
    int32_t heading = myGPS.getHeading();
    Serial.print(heading);
    Serial.print(",");
    int32_t horizontal_acc = myGPS.getHorizontalAccEst();
    Serial.print(horizontal_acc);
    Serial.print(",");
    int32_t vertical_acc = myGPS.getVerticalAccEst();
    Serial.print(vertical_acc);
    Serial.print(",");
    uint32_t speed_acc = myGPS.getSpeedAccEst();
    Serial.print(speed_acc);
    Serial.print(",");
    uint32_t heading_acc = myGPS.getHeadingAccEst();
    Serial.print(heading_acc);
    Serial.print(",");
    int32_t ned_north_vel = myGPS.getNedNorthVel();
    Serial.print(ned_north_vel);
    Serial.print(",");
    int32_t ned_east_vel = myGPS.getNedEastVel();
    Serial.print(ned_east_vel);
    Serial.print(",");
    int32_t ned_down_vel = myGPS.getNedDownVel();
    Serial.print(ned_down_vel);
    Serial.print(",");
    uint16_t pdop = myGPS.getPDOP();
    Serial.print(pdop);
    Serial.print(",");
    uint16_t mag_acc = myGPS.getMagAcc();
    Serial.print(mag_acc);
    Serial.print(",");
    int16_t mag_dec = myGPS.getMagDec();
    Serial.print(mag_dec);
    Serial.print(",");
    uint16_t geometric_dop = myGPS.getGeometricDOP();
    Serial.print(geometric_dop);
    Serial.print(",");
    uint16_t position_dop = myGPS.getPositionDOP();
    Serial.print(position_dop);
    Serial.print(",");
    uint16_t time_dop = myGPS.getTimeDOP();
    Serial.print(time_dop);
    Serial.print(",");
    uint16_t horizontal_dop = myGPS.getHorizontalDOP();
    Serial.print(horizontal_dop);
    Serial.print(",");
    uint16_t vertical_dop = myGPS.getVerticalDOP();
    Serial.print(vertical_dop);
    Serial.print(",");
    uint16_t northing_dop = myGPS.getNorthingDOP();
    Serial.print(northing_dop);
    Serial.print(",");
    uint16_t easting_dop = myGPS.getEastingDOP();
    Serial.print(easting_dop);
    Serial.print(",");
    uint32_t h_accuracy = myGPS.getHorizontalAccuracy();
    float horizontal_accuracy = h_accuracy / 10000.0;
    Serial.print(horizontal_accuracy, 4);
    Serial.print(",");
    uint32_t v_accuracy = myGPS.getVerticalAccuracy();
    float vertical_accuracy = v_accuracy / 10000.0;
    Serial.print(vertical_accuracy, 4);
    Serial.println();
  }
}
