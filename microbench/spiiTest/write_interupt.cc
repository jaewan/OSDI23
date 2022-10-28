#include <iostream>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <set>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdlib.h>
#include <cstdlib>
#include <mutex>
#include <thread>

using namespace std;

#define NUM_TRIAL 10
#define BUFFER_SIZE 10240
#define FILE_SIZE (1UL<<35)

char buffer[BUFFER_SIZE];
int fd;

std::mutex running_;
std::recursive_mutex client_mutex_;

void *compute_intensive(void *threadid){
	auto start = chrono::steady_clock::now();

	unsigned long long a = 0;
	for(int i=0; i<2000000000; i++){
		a++;
	}
	
	auto end = chrono::steady_clock::now();
	auto duration = chrono::duration_cast<chrono::microseconds>(end-start);
	long long *ret = (long long*)malloc(sizeof(long long));
	*ret = duration.count();
  pthread_exit(ret);
}

void *write_thread(void *val){
	int len = FILE_SIZE / (sizeof(char)*BUFFER_SIZE);
	while(true){
		lseek(fd,0,SEEK_SET);
		for(int i=0; i<len; i++)
			write(fd, buffer, BUFFER_SIZE);
	}
}

unsigned long long initiate_compute_threads(){
  const unsigned int num_cores = thread::hardware_concurrency();
  pthread_t threads[num_cores];
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  
  unsigned long long res = 0;
  for(int trial = 0; trial<NUM_TRIAL; trial++){
	  unsigned long long trial_res = 0;
	  for(unsigned int i=0; i<num_cores; i++){
		  pthread_create(&threads[i], &attr, compute_intensive, NULL);
	  }
	  for(unsigned int i=0; i<num_cores; i++){
		  void* duration;
		  pthread_join(threads[i], &duration);
		  trial_res += (*((long long*)(duration)));
	  }
	  res += (trial_res/num_cores);
  }
  return res/NUM_TRIAL;
}

int main(int argc, char* argv[]){
  fd = open("test.txt", O_CREAT|O_RDWR);
  memcpy(buffer, "Hello World, Test Write", 23);
  unsigned long long no_write, background_write;

  no_write = initiate_compute_threads();
  cout << "No write: "<< no_write << endl;

  pthread_t writeThread;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_create(&writeThread, &attr, write_thread, NULL);

  background_write = initiate_compute_threads();
  cout << "Background write: "<< background_write << endl;
  double overhead = (1-((double)no_write/(double)background_write));
  cout << "Background write overhead: "<<  overhead << "%" << endl;
  close(fd);
  return 0;
}
