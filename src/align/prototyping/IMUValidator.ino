/*
  An Arduino sketch to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080, and ServoCity continuous servos--along with IMU logging.
  
  Author: Bharath Keshavamurthy
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

/* Pre-processor Directives */
#include <Wire.h>
#include <Servo.h>
#include <limits.h>
#include "SparkFun_BNO080_Arduino_Library.h"

/* Strategy Definitions */
#define STEADY_STATE 100                  // Avoid "nan" yaw and pitch values by forcing steady-state reads...
#define READ_EQUILIBRIUM 1                // The number of samples to be read before we decide to use the IMU report
#define YAW_ROTATION_STRATEGIES 1         // The number of yaw angles to be tested
#define PITCH_ROTATION_STRATEGIES 5       // The number of pitch angles to be tested
#define YAW_OFFSET 0.5                    // The precision of Z-axis rotations evaluated during pre-trial manual test-runs
#define PITCH_OFFSET 0.5                  // The precision of Z-axis rotations evaluated during pre-trial manual test-runs
#define NUMBER_OF_TRIALS_PER_STRATEGY 50  // The number of tests/trials to be conducted per strategy

/* The PWM ON pulse durations for controlling the continuous servos */
#define MAX_PWM 2000                      // Forward Motion (PWM ON pulse duration)
#define MID_PWM 1500                      // Stop Motion (PWM ON pulse duration)
#define MIN_PWM 1000                      // Backward Motion (PWM ON pulse duration)

/* IMU Instance creation */
BNO080 bno080;

/* Servo control pin definitions | Servo (X and Z) instance creation */
/* X = Yaw movements | Z = Pitch & Roll movements | For pitch movements, place the object being rotated perpendicular to the pan plate--in the X-Z plane | Manual height adjustmtents */
int pinX = 9, pinZ = 10;
Servo servoX, servoZ;

/* Strategy Variables */
bool start = true;                                // Initial position flag
bool terminate = false;                           // Everything's done
bool antiClockwise = true;                        // A rotation direction flag for the yaw angle testing
float yaw = 0.0, prev_yaw = 0.0;                  // The estimated yaw angles (current and previous)
float pitch = 0.0, prev_pitch = 0.0;              // The estimated pitch angles (current and previous)
bool testYaw = true;                              // Start with the yaw rotation testing--then, flip this boolean to proceed with the pitch angle testing
bool homePlate = true;                            // An indicator variable to determine if the antenna setup is at 0/0 degrees (home): At startup, it is!
unsigned int strategyCounter = 0;                 // A counter for iterating through the strategies
unsigned int sampleCounter = 0;                   // A counter for ensuring that the IMU values are read post-equilibrium...(equilibrium threshold is determined by manual evaluation, i.e., trial-and-error)
unsigned int iteration = 0;                       // A counter to track the number of times each strategy is tested
unsigned int steadyStateCounter = 0;              // A counter to force steady state reads...
unsigned int print_index = 0;                     // A variable for indexing into the strategies collection during logging

/* Rotation strategies: Coarse-grained */
const float yawCoarseTestAngles[YAW_ROTATION_STRATEGIES] = {10.0};                                       // Yaw angles to be tested (in degrees)
const float pitchCoarseTestAngles[PITCH_ROTATION_STRATEGIES] = {10.0, 20.0, 30.0, 40.0, 50.0};           // Pitch angles to be tested (in degrees)

/* Rotation strategies: Fine-grained */
const float yawFineTestAngles[YAW_ROTATION_STRATEGIES] = {3.0};                                         // Yaw angles to be tested (in degrees)
const float pitchFineTestAngles[PITCH_ROTATION_STRATEGIES] = {5.0, 10.0, 15.0, 20.0, 25.0};             // Pitch angles to be tested (in degrees)

/* The setup routine */
void setup() {
  Serial.begin(9600);
  while (!Serial);
  Serial.println("[INFO] Odin setup: Pitch and Yaw Rotation Angle Estimation");
  /* I2C Scanner not needed here: Use bno080.begin() to check for an IMU connection */
  Wire.begin();
  /* Check for the IMU at the I2C interface */
  if (!bno080.begin()) {
    Serial.println("[WARN] IMU not connected!");
    return;
  }
  /* I2C Data Rate = 400 kHz | IMU Refresh at 100 ms */
  Wire.setClock(400000);
  bno080.enableRotationVector(100);

  /* Attach to servoX */
  servoX.attach(pinX);
  servoX.writeMicroseconds(MID_PWM);
 
  /* Attach to servoZ */
  servoZ.attach(pinZ);
  servoZ.writeMicroseconds(MID_PWM);

  /* Wait 10s for me to setup the platform */
  delay(10000);
}

/* The loop routine */
void loop() {
  /* Everything's done. Just wait...*/
  if (terminate) {
    delay(100000);
  }
  /* Calibration save (manual interrupt based save to Flash) */
  /* DCD save to the Flash outside the general auto-calibration & auto-recording done by the IMU every 300s */
  if (Serial.available()) {
    if (Serial.read() == 'S') {
      /* Save the current DCD to the Flash */
      bno080.saveCalibration();
      bno080.requestCalibrationStatus();
      for (int i = 0; i < 100; i++) {
        if (i > 99) {
          /* Calibration Failed */
          Serial.println("[ERROR] Odin loop: Flash Save Failed! Retry!");
          break;
        } else {
          if (bno080.dataAvailable() && bno080.calibrationComplete()) {
            /* Calibration successful */
            Serial.println("[INFO] Odin loop: Calibration & Flash Save Successful!");
            delay(1000);
            break;
          }
          delay(1);
        }
      }
    }
  }
  /* Data extraction and logging */
  if (bno080.dataAvailable()) {
    /* Read the Quaternion Complex Vector values [i, j, k, real] and Normalize them */
    /* Parse Input Report from BNO080 --> Bosch Sensor Hub Transport Protocol (SHTP) over I2C --> Parse raw quaternion values if the channel is CHANNEL_GYRO in the SHTP Header OR if the SHTP Payload is referenced by SENSOR_*_ROTATION_VECTOR */
    /* Use the BNO080 Library for this | Modify the C++ code if needed with the Q-values | https://github.com/fm4dd/pi-bno080 | https://github.com/sparkfun/SparkFun_BNO080_Arduino_Library/blob/master/src/SparkFun_BNO080_Arduino_Library.cpp */
    float quatI = bno080.getQuatI();
    float quatJ = bno080.getQuatJ();
    float quatK = bno080.getQuatK();
    float quatReal = bno080.getQuatReal();
    float norm = sqrt((quatI * quatI) + (quatJ * quatJ) + (quatK * quatK) + (quatReal * quatReal));
    quatI /= norm;
    quatJ /= norm;
    quatK /= norm;
    quatReal /= norm;
    /* Determine the Yaw Angle (in degrees) */
    yaw = atan2((2.0 * ((quatReal * quatK) + (quatI * quatJ))), (1.0 - (2.0 * ((quatJ * quatJ) + (quatK * quatK))))) * (180/PI);
    float temp = 2.0 * ((quatReal * quatJ) - (quatK * quatI));
    /* Sine Inverse Range Restrictions [-1.0, 1.0]*/
    temp = (temp > 1.0) ? 1.0 : temp;
    temp = (temp < -1.0) ? -1.0 : temp;
    /* Determine the Pitch Angle (in degrees) */
    pitch = asin(temp) * (180/PI);
    
    /* Log the Yaw and Pitch Angles */
//    Serial.print("Yaw = ");
//    Serial.print(yaw);
//    Serial.print(F(" | "));
//    Serial.print(" Pitch = ");
//    Serial.print(pitch);
//    Serial.println();

    /* Update the sample counter */
    sampleCounter += 1;
    /* Update the steady state counter */
    steadyStateCounter += 1;
  }
  if (steadyStateCounter < STEADY_STATE) {
    Serial.println("[WARN] Waiting for steady state | Possible NaN values");
    return;
  }
  /* TODO: Combine the logic here: there's no need for conditional separation between axes */
  /* Testing the rotation strategies (Motor Control | Yaw Rotation Testing) */
  if (testYaw) {
    if (sampleCounter >= READ_EQUILIBRIUM) {
      if (homePlate) {
        if (start) {
          start = false;
          prev_yaw = yaw;
        }
        /* Start Motors here... (forward) */
        servoX.writeMicroseconds( (antiClockwise) ? MAX_PWM : MIN_PWM );
        if ((yaw - prev_yaw) >= yawCoarseTestAngles[strategyCounter]) {
          /* Stop Motors here... */
          servoX.writeMicroseconds(MID_PWM);
          delay(10000);      // Delay 10 seconds for SwisTrack to capture a frame(s) with the terminal incident laser beam on the cardboard/thumbtack
          homePlate = false; // The system is not at home plate: get it home
          start = true;      // Re-initiate difference checker for the complement strategy
        }
      } else if (homePlate == false) { // The system is not at home plate: rotate back to it...
        if (start) {
          start = false;
          prev_yaw = yaw;
        }
        /* Start Motors here... (backward) */
        servoX.writeMicroseconds( (antiClockwise) ? MIN_PWM : MAX_PWM );
        if ((prev_yaw - yaw) >= yawCoarseTestAngles[strategyCounter]) {
          /* Stop Motors here... */
          servoX.writeMicroseconds(MID_PWM);
          delay(10000);           // Delay 10 seconds for SwisTrack to capture a frame(s) with the terminal incident laser beam on the cardboard/thumbtack
          homePlate = true;       // The system is at home plate...
          iteration += 1;         // Increment the strategy iteration counter
          start = true;           // Re-initiate difference checker for the next strategy
          if (iteration >= NUMBER_OF_TRIALS_PER_STRATEGY) {
            iteration = 0;        // Re-initiate iterator for the next strategy
            strategyCounter += 1; // Increment the strategy counter to test the next strategy
          }
          if (strategyCounter >= YAW_ROTATION_STRATEGIES && iteration >= NUMBER_OF_TRIALS_PER_STRATEGY) { // Finished all the yaw testing strategies--and, the system is at home!
            /* Yaw testing clean-up and handover initiation to the Pitch testing framework */
            testYaw = false;
            strategyCounter = 0;
            servoX.detach();
            servoZ.detach();
            terminate = true;
          }
        }
      } else {
        /* Nothing to do... */
      }
      /* Update the sample counter */
      sampleCounter = 0;
      /* Log the Yaw and Pitch Angles--along with the required and rotated angles */
      if (start) {
        Serial.print("Yaw = ");
        Serial.print(yaw);
        Serial.print(F(" $ "));
        Serial.print(" Pitch = ");
        Serial.print(pitch);
        Serial.println();
        Serial.print("Yaw Angle Rotated = ");
        Serial.print(yaw - prev_yaw);
        Serial.print(F(" % "));
        Serial.print("Required Yaw Angle Rotation = ");
        print_index = (iteration > 0) ? strategyCounter : (strategyCounter - 1); 
        Serial.print(((yaw - prev_yaw) >= 0.0) ? yawCoarseTestAngles[strategyCounter]: (-1 * yawCoarseTestAngles[print_index]));
        Serial.println();
        delay(3000);
      }
    }
  } else { // Testing the rotation strategies (Motor Control | Pitch Rotation Testing)
    /* TODO: For pitch movements, place the *laser pointer* OR *antenna* perpendicular to the pan plate--in the X-Z plane */
    if (sampleCounter >= READ_EQUILIBRIUM) {
      if (homePlate) {
        if (start) {
          start = false;
          prev_pitch = pitch;
        }
        /* Start Motors here... (forward) */
        servoZ.writeMicroseconds(MAX_PWM);
        if (abs(pitch - prev_pitch) >= pitchCoarseTestAngles[strategyCounter]) {
          /* Stop Motors here... */
          servoZ.writeMicroseconds(MID_PWM);
          delay(10000);      // Delay 10 seconds for SwisTrack to capture a frame(s) with the terminal incident laser beam on the cardboard/thumbtack
          homePlate = false; // The system is not at home plate: get it home!
          /* Log the Yaw and Pitch Angles */
          Serial.print("Yaw = ");
          Serial.print(yaw);
          Serial.print(F(" $$$ "));
          Serial.print(" Pitch = ");
          Serial.print(pitch);
          Serial.println();
          start = true;     // Re-initiate difference checker for the complement strategy
        }
      } else if (homePlate == false) { // The system is not at home plate: rotate back to it...
        if (start) {
          start = false;
          prev_pitch = pitch;
        }
        /* Start Motors here... (backward) */
        servoZ.writeMicroseconds(MIN_PWM);
        if (abs(prev_pitch - pitch) >= pitchCoarseTestAngles[strategyCounter]) {
          /* Stop Motors here... */
          servoZ.writeMicroseconds(MID_PWM);
          delay(10000);      // Delay 10 seconds for SwisTrack to capture a frame(s) with the terminal incident laser beam on the cardboard/thumbtack
          homePlate = true;  // The system is at home plate...
          iteration += 1;    // Increment the strategy iteration counter
          /* Log the Yaw and Pitch Angles */
          Serial.print("Yaw = ");
          Serial.print(yaw);
          Serial.print(F(" $$ "));
          Serial.print(" Pitch = ");
          Serial.print(pitch);
          Serial.println();
          start = true;           // Re-initiate difference checker for the next strategy
          if (iteration >= NUMBER_OF_TRIALS_PER_STRATEGY) {
            iteration = 0;        // Re-initiate iterator for the next strategy
            strategyCounter += 1; // Increment the strategy counter to test the next strategy
          }
          if (strategyCounter >= PITCH_ROTATION_STRATEGIES && iteration >= NUMBER_OF_TRIALS_PER_STRATEGY) { // Finished all the pitch testing strategies--and, the system is at home!
            /* Reset all counters and EXIT */
            testYaw = true;
            strategyCounter = 0;
            servoX.detach();
            servoZ.detach();
            terminate = true;
          }
        }
      } else {
        /* Nothing to do... */
      }
      /* Update the sample counter */
      sampleCounter = 0;
      /* Log the Yaw and Pitch Angles--along with the required and rotated angles */
      if (start) {
        Serial.print("Yaw = ");
        Serial.print(yaw);
        Serial.print(F(" $ "));
        Serial.print(" Pitch = ");
        Serial.print(pitch);
        Serial.println();
        Serial.print("Pitch Angle Rotated = ");
        Serial.print(yaw - prev_yaw);
        Serial.print(F(" % "));
        Serial.print("Required Pitch Angle Rotation = ");
        print_index = (iteration > 0) ? strategyCounter : (strategyCounter - 1); 
        Serial.print(((pitch - prev_pitch) >= 0.0) ? pitchCoarseTestAngles[strategyCounter]: (-1 * pitchCoarseTestAngles[print_index]));
        Serial.println();
        delay(3000);
      }
    }
  }
}
