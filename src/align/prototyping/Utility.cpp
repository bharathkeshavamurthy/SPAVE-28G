/*
  A Utility class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
    and ServoCity continuous servos--along with IMU logging.

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include <stdio.h>
#include "Utility.h"

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
#endif // _WIN32 || _WIN64

#if defined(_WIN32) || defined(_WIN64)
void curPos(int x, int y) {
  HANDLE hStdout;
  CONSOLE_SCREEN_BUFFER_INFO csbiInfo;
  hStdout=GetStdHandle(STD_OUTPUT_HANDLE);
  GetConsoleScreenBufferInfo(hStdout, &csbiInfo);
  csbiInfo.dwCursorPosition.X=x;
  csbiInfo.dwCursorPosition.Y=y;
  SetConsoleCursorPosition(hStdout, csbiInfo.dwCursorPosition);
}

void clearTerminal() {
    system( "cls" );
}
#endif  // _WIN32 || _WIN64

void updateTime(SYSTEMTIME *time, FILETIME *timeF, ULONGLONG *time64) {
    GetSystemTime(time);
    SystemTimeToFileTime(time, timeF);
    *time64 = (((ULONGLONG) (*timeF).dwHighDateTime) << 32) + (*timeF).dwLowDateTime;
}

ULONGLONG diffTimeSeconds(ULONGLONG timeStart, ULONGLONG timeEnd) {
    return ((timeEnd-timeStart)/10000000);
}

ULONGLONG diffTimeMilliseconds(ULONGLONG timeStart, ULONGLONG timeEnd) {
    return ((timeEnd-timeStart)/10000);
}

ULONGLONG diffTimeMicroSeconds(ULONGLONG timeStart, ULONGLONG timeEnd) {
    return ((timeEnd-timeStart)/10);
}

unsigned char QKeyPressed() {
#if defined(_WIN32) || defined(_WIN64)
    if(GetKeyState (0x51) < 0) {     // 'q'
        return 1;
    } else {
        return 0;
    }
#endif // _WIN32 || _WIN64
}

unsigned char SpaceKeyPressed() {
#if defined(_WIN32) || defined(_WIN64)
    if(GetKeyState (VK_SPACE) < 0) {     // spacebar
        return 1;
    } else {
        return 0;
    }
#endif // _WIN32 || _WIN64
}