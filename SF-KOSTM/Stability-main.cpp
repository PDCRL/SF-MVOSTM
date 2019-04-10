/*
*DESCRIPTION    :   Main Test Application
*COMPANY        :   IIT Hyderabad
*/

#include <iostream>
#include <fstream>
#include "MV_OSTM.cpp"
#include <pthread.h>
#include <atomic>
#include <array>
#define MAX_KEY 30

using namespace std;
//Variables define to log abort counts.
atomic<bool> ready (false);
atomic<unsigned long int> txCount (0);
atomic<unsigned long int> rvaborts (0);
atomic<unsigned long int> updaborts (0);

MV_OSTM* lib = new MV_OSTM();
//Local variable to set the # of threads and I/D/L percentages.
#define num_threads 250
uint_t prinsert = 45, prlookup = 10, prdelete = 45;
uint_t num_op_per_tx = 10;
int testSize = 1;
thread *t;

int range = 12;
vector <atomic<int>> commitedTranx(range);
double startTime = 0.0;
int epsilon = 5;
/*************************Barrier code begins*****************************/
std::mutex mtx;
std::condition_variable cv;
bool launch = false;
double timee[num_threads];
int aborts[num_threads];
/*
* Barrier to sychronize all threads after creation.
*/
void wait_for_launch()
{
	std::unique_lock<std::mutex> lck(mtx);
	while (!launch) cv.wait(lck);
}
/*
 * Returns the time requested.
 */
double timeRequest() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double timevalue = tp.tv_sec + (tp.tv_usec/1000000.0);
  return timevalue;
}

/*
* Let threads execute their task after final thread has arrived.
*/
void shoot()
{
	std::unique_lock<std::mutex> lck(mtx);
	launch = true;
	cv.notify_all();
}

/*
 * The funstion which is invoked by each thread once all threads are notified.
 */
OPN_STATUS app1(int tid)
{   
    L_txlog *txlog = new L_txlog();
    OPN_STATUS ops, txs = ABORT;
    int* val = new int;
    int localAbortCnt = 0;
    int ts_idx = 0;
    while (ts_idx < testSize)
    {
        bool retry =  true;
        while(retry == true)
        {
			if(txlog->L_its == NIL) {
				txlog = lib->begin(NIL);
			} else {
				txlog = lib->begin(txlog->L_its);
			}
			for(int i = 0; i < num_op_per_tx; i++)
            {
                //generate randomly one of insert, lookup, delete.
                int opn = rand()%100;
                if(opn < prinsert)
					opn  = 0;
                else if(opn < (prinsert+prdelete))
                    opn = 1;
                else
                    opn = 2;
                    
                int key = rand()%MAX_KEY;
				if(0 == opn){ 
                    /*INSERT*/
                    ops = lib->tx_insert(txlog, key, 100);//inserting value = 100
                } else if(1 == opn) {
					/*DELETE*/
					ops = lib->tx_delete(txlog, key, val);
                } else {
                    /*LOOKUP*/
                    ops = lib->tx_lookup(txlog, key, val);
				}

                if(ABORT == ops) {
                     localAbortCnt++;
                     retry = true;
                     break;
                } 
            }

            if(ABORT != ops)
            {
                txs = lib->tryCommit(txlog);
                if(ABORT == txs) {
                    retry = true;
                    //updaborts++;
                    localAbortCnt++;
                } else {
                    retry = false;
                }
            }
        }
       // txCount++;
        ts_idx++;
    }
    aborts[tid] = localAbortCnt;
	return txs;
}

/*
* DESCP: worker for threads that call ht's function as per their distribution.
*/
void worker(uint_t tid)
{
	//barrier to synchronise all threads for a coherent launch
	wait_for_launch();
	struct timeval start_time, end_time;
	double endTime;
	//begin time
	//gettimeofday(&start_time, NULL);
	while(true) {
		app1(tid);
		//end time
		//gettimeofday(&end_time, NULL);
		endTime = timeRequest();
		int k = (endTime - startTime) / epsilon;
		if(k >= 12)
			return;
		commitedTranx[k]++;
		}
	timee[tid] = (end_time.tv_sec - start_time.tv_sec);
	timee[tid] += (end_time.tv_usec - start_time.tv_usec)/ 1000000.0;
}

int main(int argc, char **argv)
{
    double duration;
	struct timeval start_time, end_time;

	try {   
	time_t tt;
	srand(time(&tt));

    t = new std::thread [num_threads];

	
	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i] = std::thread(worker, i);
	}
	
	startTime = timeRequest(); 
	
	gettimeofday(&start_time, NULL);
	//notify all threads to begin the worker();
	shoot(); 
    ready = true;

	for (uint_t i = 0; i < num_threads; ++i)
	{
		t[i].join();
	}
	gettimeofday(&end_time, NULL);
    cout<<"#total aborts      :"<<(rvaborts+updaborts)<<endl;
 
	duration = (end_time.tv_sec - start_time.tv_sec);
	duration += (end_time.tv_usec - start_time.tv_usec)/ 1000000.0;
	cout<<"time..: "<<duration<<" seconds"<<std::endl;
	} catch(const std::system_error& e) {
        std::cout << "Caught system_error with code " << e.code() 
                  << " meaning " << e.what() << '\n';
    }
    
    double max_time = 0.0;
    int k = 0;
    int max_abort = 0;
	while(k<num_threads)
	{
		if(max_time<timee[k])
		{
			max_time = timee[k];
			max_abort = aborts[k];
		}
		k++;
	}
    cout<<"Worst time and aborts logged as : "<<max_time<<" "<<max_abort<<endl;
    for(int i=0;i<12;i++)
    {
		std::cout<<commitedTranx[i]<<" ";
	}
	cout<<endl;
 	return 0;
}

