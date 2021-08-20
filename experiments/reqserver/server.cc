#include <stdio.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <mutex>
#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <algorithm>
#include <iostream>
#include <random>
#include <chrono>
#include <boost/atomic.hpp>
#include <assert.h>
#include <fstream>

#define PORT 8080
#define SA struct sockaddr

int load_max_key = 1000000;
int update_max = 2000000;

std::random_device rd;
std::mt19937 e2(rd());
std::uniform_real_distribution<float> realdist(0, 1);
std::uniform_real_distribution<> oldreaddist(1, load_max_key);
std::uniform_real_distribution<> updatedist(load_max_key + 1, update_max);

std::mutex latest_keys_mutex;
std::vector<std::pair<int, std::chrono::time_point<std::chrono::high_resolution_clock>>> latest_keys;

int window_us = 100; // default 100 us
double fractionInWindow = 1.0; //

double get_real() {
    return realdist(e2);
}

int get_key() {
	double toss = get_real();
	if (toss <= fractionInWindow) {
		std::lock_guard<std::mutex> guard(latest_keys_mutex);

	    if (latest_keys.size() == 0)
	        return 1;

	    int index = std::floor(realdist(e2) * latest_keys.size());
	    return latest_keys[index].first;
	} else {
        return std::floor(oldreaddist(e2));
	}    
}

void add_latest_key(int key) {
    auto current_time = std::chrono::high_resolution_clock::now();
    std::lock_guard<std::mutex> guard(latest_keys_mutex);
    latest_keys.push_back(std::make_pair(key, current_time));

    int del_upto = -1;
    for(auto l: latest_keys) {
        if(std::chrono::duration_cast<std::chrono::microseconds>(current_time - l.second).count() > window_us) {
            del_upto++;
        }
    }

    if(del_upto >= 0) {
        latest_keys.erase(latest_keys.begin(), latest_keys.begin() + del_upto);
    }
}

int get_random_key_for_update() {
    return std::floor(updatedist(e2));
}

void func(int sockfd)
{
    char buff;
    int resp;
    int n;
    for (;;) {
        buff = 0;
        read(sockfd, &buff, sizeof(char));
        if(buff == 'U' or buff == 'u' or buff == 'I' or buff == 'i') {
            int key = get_random_key_for_update();
            add_latest_key(key);
            resp = key;
            write(sockfd, &resp, sizeof(int));
        } else if (buff == 'r' or buff == 'R') {
            int key = get_key();
            resp = key;
            write(sockfd, &resp , sizeof(int));
        } else if (buff == 'e') {
            printf("Server Exit...\n");
            break;
        } else {
        	printf("Unexpected char from client %c", buff);
        	break;
        }
    }
}
  
int main(int argc, char** argv)
{
    assert(argc == 3);
    window_us = std::stoi(argv[1]);
    fractionInWindow = std::stof(argv[2]);
    std::cout<< "Using window of " << window_us <<  " us" << std::endl;
    std::cout<< "Using fraction " << fractionInWindow <<  "" << std::endl;
    int sockfd, connfd;
    socklen_t len;
    struct sockaddr_in servaddr, cli;
  
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        exit(0);
    }
    else
        printf("Socket successfully created..\n");
    bzero(&servaddr, sizeof(servaddr));
  
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(PORT);
  
    if ((bind(sockfd, (SA*)&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed...\n");
        exit(0);
    }
  
    if ((listen(sockfd, 10)) != 0) {
        printf("Listen failed...\n");
        exit(0);
    }
  
    len = sizeof(cli);
    
    std::vector<std::thread> clients(10);
    
    int cli_id = 0;
    
    while(1) { 
        connfd = accept(sockfd, (SA*)&cli, &len);
        assert(connfd >= 0);
        clients[cli_id++] = std::thread(func, connfd);
    }
  
    std::cout<<"Waiting to join"<<std::endl;
    for(int i = 0; i < cli_id; i++) {
        clients[i].join();
    }
    
    close(sockfd);
}
