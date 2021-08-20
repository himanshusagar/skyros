all:
	g++ -g server.cc -std=c++11 -o server -L /usr/lib/ -lpthread -lboost_system
	g++ -g client.cc -std=c++11 -o client -L /usr/lib/x86_64-linux-gnu/ -lpthread 
clean:
	rm -rf server
	rm -rf client
