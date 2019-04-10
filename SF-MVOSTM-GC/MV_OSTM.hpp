
#ifndef MV_OSTM_
#define MV_OSTM_

#include "dataStructure.cpp"

/**
 * This class define the structure of main MV-OSTM.
 **/
class MV_OSTM {    
public:
    /*keeps count of transactions for allocating new transaction IDs.*/
    std::atomic<int> G_cnt;
    /*shared memory -> will initialize hashtable as in main.cpp.*/
    HashMap* hash_table;
    /*Constructor of the class.*/
    MV_OSTM();
    /*Destructor of the class.*/
    ~MV_OSTM();
    /*Initializes the global counter.*/
    void init();
    /*starts a transaction by initing a log of it as well to record.*/
    L_txlog* begin(int its);
    /*Method to perform the look up operation over the hash table.*/
    OPN_STATUS tx_lookup(L_txlog* txlog, int L_key, int* value);
    /*Method to perform the delete operation over the hash table.*/
    OPN_STATUS tx_delete(L_txlog* txlog, int L_key, int* value);
    /*Method to perform the insert operation over the hash table.*/
    OPN_STATUS tx_insert(L_txlog* txlog, int L_key, int L_val);
    /*Method to commit the changes to the shared memory - the hash table.*/
    OPN_STATUS tryCommit(L_txlog* txlog);
    /*Metho that is invoked by a transaction when is about to abort.*/
    OPN_STATUS tryAbort(L_txlog* txlog);
};
void insertAndSortRVL(list<L_txlog*> *allRVL, L_txlog* txlog);
#endif //MV_OSTM_
