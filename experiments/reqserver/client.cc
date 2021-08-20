#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>       
#include <netinet/in.h>
#include <arpa/inet.h>
#include <random>

#define PORT 8080
#define SA struct sockaddr

std::random_device rd;
std::mt19937 e2(rd());

char get_op() {
    std::uniform_real_distribution<float> dist(0, 1);
    if (dist(e2) < 0.5) 
        return 'r';
    
    return 'u';
 }

void func(int sockfd)
{
    char buff;
    int resp;
    int n;
    int i = 0;
    for (; i < 1000; i++) {
        buff = get_op();
        write(sockfd, &buff, sizeof(char));
        bzero(&resp, sizeof(int));
        read(sockfd, &resp, sizeof(int));
        printf("Op: %c key from server : %d\n", buff, resp);
    }

    buff = 'e';
    write(sockfd, &buff, sizeof(char));
}
  
int main()
{
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
  
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        exit(0);
    }
    bzero(&servaddr, sizeof(servaddr));
  
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    servaddr.sin_port = htons(PORT);
  
    if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
        printf("connection with the server failed...\n");
        exit(0);
    }
    func(sockfd);
    close(sockfd);
}