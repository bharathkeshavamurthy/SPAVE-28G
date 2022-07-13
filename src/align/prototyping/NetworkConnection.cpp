/*
  A NetworkConnection class for the laser test-bed prototype to perform Yaw AND Pitch rotation strategy testing using the SparkFun Blackboard Rev C, IMU BNO080,
    and ServoCity continuous servos--along with IMU logging.

  A TCP connection between this controller and SwisTrack

  Author: Bharath Keshavamurthy (Adapted from GCTronic-Talos [Purdue ECE-IE] Shape Execution)
  Organization: School of Electrical and Computer Engineering, Purdue University, West Lafayette, IN.
  Copyright (c) 2020. All Rights Reserved.
*/

#include "NetworkConnection.h"

#if defined(_WIN32) || defined(_WIN64)
    #include "windows.h"
    #include <winsock2.h>
#endif // _WIN32 || _WIN64

SOCKET sock; // Socket handle

// CONNECTTOHOST – Connects to a remote host
signed char OpenTCPConnection(int PortNo, char* IPAddress) {
    // Start up Winsock…
    WSADATA wsadata;

    int error = WSAStartup(0x0202, &wsadata);

    // Did something happen?
    if (error)
        return -1;

    // Did we get the right Winsock version?
    if (wsadata.wVersion != 0x0202)
    {
        WSACleanup(); // Clean up Winsock
        return -1;
    }

    // Fill out the information needed to initialize a socket

    SOCKADDR_IN target; // Socket address information

    target.sin_family = AF_INET;                    // Address family Internet
    target.sin_port = htons (PortNo);               // Port to connect on
    target.sin_addr.s_addr = inet_addr (IPAddress); // Target IP

    sock = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP); // Create socket
    if (sock == INVALID_SOCKET)
    {
        return -1; // Couldn't create the socket
    }

    // Try connecting...
    if (connect(sock, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR) {
        return -1; // Couldn't connect
    } else {
        u_long iMode=1;
        ioctlsocket(sock,FIONBIO,&iMode);   // Non-blocking
        return 0; // Success
    }
}

signed int ReceiveTCP(char *server_reply) {
    return recv(sock , server_reply , 300 , 0);
}

// CLOSECONNECTION – shuts down the socket and closes any connection on it
void CloseTCPConnection ()
{
    // Close the socket if it exists
    if (sock)
        closesocket(sock);

    WSACleanup(); // Clean up Winsock
}