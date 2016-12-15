#pragma once

#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace std;

class TCPConn {
public:
	TCPConn() {}
	virtual void send(const string &msg) = 0;
	virtual string recv() = 0;
protected:
	int sockfd;
};

class TCPServer : TCPConn {
public:
	TCPServer() {}
	TCPServer(const string &addr, const string &port) { init(addr, port); }
	void init(const string &addr, const string &port);
	void send(const string &msg);
	int accept();
	string recv();
private:
	int initserver(int type, const struct sockaddr *addr,
			socklen_t alen, int qlen);
	int clfd;
};

class TCPClient : TCPConn {
public:
	TCPClient() {}
	TCPClient(const string &addr, const string &port) { connect(addr, port); }
	void connect(const string &addr, const string &port);
	void send(const string &msg);
	string recv();
private:
	int connect_retry(int sockfd, const struct sockaddr *addr, socklen_t alen);
};


