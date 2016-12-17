#include "network.h"

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <sys/socket.h>
#include <cstring>

#define MAXSLEEP 8

int TCPClient::connect_retry(int sockfd,
		const struct sockaddr *addr, socklen_t alen)
{
	int numsec;
	for (numsec = 1; numsec <= MAXSLEEP; numsec <<= 1) {
		if ( ::connect(sockfd, addr, alen) == 0 ) {
			return 0;
		}
		perror("connect");
		if (numsec <= (MAXSLEEP>>1))
			sleep(numsec);
	}
	return -1;
}

void TCPClient::connect(string const &addr, const string &port)
{
	sockfd = socket(AF_INET, SOCK_STREAM, 0);

	in_addr_t server_ip = inet_addr(addr.c_str());
	in_port_t server_port = atoi(port.c_str());

	sockaddr_in server_addr;
	memset(&server_addr, 0, sizeof server_addr);
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = server_ip;
	server_addr.sin_port = htons(server_port);

	if (connect_retry(sockfd,
				(sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
		fprintf(stderr, "Connect failed.\n");
		exit(-1);
	}
}

string TCPClient::recv()
{
#define BUFLEN 1024
	int n;
	char buf[BUFLEN];
	string ret;

	while ((n = ::recv(sockfd, buf, BUFLEN, 0)) > 0) {
		ret += string(buf, n);
		if (n < BUFLEN || (n == BUFLEN && buf[BUFLEN - 1] == '\0'))
			break;
	}

	return ret;
}

void TCPClient::send(const string &msg)
{
	::send(sockfd, msg.c_str(), msg.length(), 0);
}

int TCPServer::initserver(int type, const struct sockaddr *addr, socklen_t alen,
  int qlen)
{
	int fd, err;
	int reuse = 1;

	if ((fd = socket(addr->sa_family, type, 0)) < 0)
		return(-1);
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
	  sizeof(int)) < 0)
		goto errout;
	if (bind(fd, addr, alen) < 0) {
		perror("bind");
		goto errout;
	}
	if (type == SOCK_STREAM || type == SOCK_SEQPACKET)
		if (listen(fd, qlen) < 0)
			goto errout;
	return(fd);

errout:
	err = errno;
	close(fd);
	errno = err;
	return(-1);
}

void TCPServer::init(const string &addr, const string &port)
{
	in_addr_t my_ip = inet_addr(addr.c_str());
	in_port_t my_port = atoi(port.c_str());
	
	sockaddr_in my_addr;
	memset(&my_addr, 0, sizeof my_addr);
	my_addr.sin_family = AF_INET;
	my_addr.sin_addr.s_addr = my_ip;	
	my_addr.sin_port = htons(my_port);	
	
	if ((sockfd = initserver(SOCK_STREAM, (sockaddr*)&my_addr, sizeof my_addr, 128)) < 0) {
		fprintf(stderr, "Initserver failed.\n");
		exit(-1);
	}
}

int TCPServer::accept()
{
	if ((clfd = ::accept(sockfd, NULL, NULL)) < 0) {
		fprintf(stderr, "Accept error.\n");
		exit(-4);
	}
	return clfd;
}

void TCPServer::send(const string &msg)
{
	::send(clfd, msg.c_str(), msg.length(), 0);
}

string TCPServer::recv()
{
	int n;
	char buf[BUFLEN];
	string ret;

	while ((n = ::recv(clfd, buf, BUFLEN, 0)) > 0) {
		ret += string(buf, n);
		if (n < BUFLEN || (n == BUFLEN && buf[BUFLEN - 1] == '\0'))
			break;
	}

	return ret;
}

void TCPServer::end()
{
	::close(clfd);
}
