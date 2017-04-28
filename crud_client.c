////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server
int            sockfd = -1;

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
	uint8_t request;
	struct sockaddr_in v4;
	int written = 0, reed = 0;
	CrudResponse *resno = malloc(sizeof(CrudResponse)), resho;
	CrudRequest *reqno = malloc(sizeof(CrudRequest));

	request = (op >> 28) | 0x0;

	if (request == CRUD_INIT) {
		sockfd = socket(PF_INET, SOCK_STREAM, 0);
		if (sockfd == -1) {
			printf("Error on Socket Creation [%s] \n", strerror(errno));
			return -1;
		}

		v4.sin_family = AF_INET;
		v4.sin_port = htons(CRUD_DEFAULT_PORT);

		if (inet_aton(CRUD_DEFAULT_IP, &v4.sin_addr) == 0)
			return (-1);

		if (connect(sockfd, (const struct sockaddr *) &v4,
			sizeof(struct sockaddr)) == -1)
			return (-1);
	}


	*reqno = htonll64(op);
	while (written < sizeof(CrudRequest)) {
		written += write(sockfd,
			&((char *) reqno)[written], sizeof(CrudRequest) - written); 
	}
	free(reqno);


	if (request == CRUD_CREATE || request == CRUD_UPDATE)
	{
		written = 0;
		while (written < ((op >> 4) & 0xFFFFFF)) {
			printf("WRITTING\n");
			written += write(sockfd,
				&((char *)buf)[written], ((op >> 4) & 0xFFFFFF) - written);
		}
	}

	while (reed < sizeof(CrudResponse)) {
		reed += read(sockfd, &((char *)resno)[reed], sizeof(CrudResponse) - reed);
	}

	resho = ntohll64(*resno);
	free(resno);

	if (request == CRUD_READ) {
		reed = 0;
		while (reed < ((resho >> 4) & 0xFFFFFF)) {
			printf("READING\n");
			reed += read(sockfd, 
				&((char *) buf)[reed], ((resho >> 4) & 0xFFFFFF) - reed);
		}
	}

	if (request == CRUD_CLOSE) {
		close(sockfd);
		sockfd = -1;
	}

	return (resho);
}
