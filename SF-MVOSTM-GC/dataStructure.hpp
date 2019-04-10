

#ifndef dataStructure_hpp
#define dataStructure_hpp

#include <pthread.h>
#include "common.h"
#include <climits>
#include <assert.h>
#include <vector>
#include <atomic>
#include <algorithm>

using namespace std;

/*List top maintain the all the live transactions.*/
vector<int> *liveList = new vector<int>;
/*Lock to be acquired before acquiring lock on live list.*/
mutex *lockLiveList = new mutex;

class L_txlog;

/**
 *
 * Class defines struture of a single version of a Key-Node.
 *
 **/
class version {
    public:
		/*To maintain the timestamp of the version.*/
        int G_ts;
        /*To define the value contained by G_val.*/
        int G_val;
        /*To maintain the list of transactions read this version.*/
        vector<L_txlog*> *G_rvl = new vector<L_txlog*>;
        /*To maintain max rvl*/
        //int G_max_rvl = 0;
        /*To maintain the vrt of the version.*/
        int G_vrt;
};

/**
 *
 * Class defines struture of a single Key-Node.
 *
 **/
class G_node {
    public:
		/*To keep track of bucket ID.*/
        int L_bucket_id;
		/*Defines the key of the key-Node.*/
        int G_key;
        /*Defines the status flag of the key-Node.*/
        bool G_mark;
        /*List to define the versions of this key.*/
        vector<version*> *G_vl = new vector<version*>;
        /*Reinterant lock*/
        mutex lmutex;
        /*next red node*/
        G_node *red_next;
        /*next blue node*/
        G_node *blue_next;
        /*Default constructor to the class.*/
        G_node();
        /*init the node and create a version with key, value*/
        G_node(int key);
        /*Method to compare two G_nodes on the basis of their keys.*/
        bool compareG_nodes(G_node* node);
};

/** 
 * L_rec class is a class to create and maintain arecord withing the 
 * local log/buffer. 
 **/
class L_rec {
    public:
        /*To keep track of bucket ID.*/
        int L_bucket_id;
        /*To keep track of Key-Node.*/
        int L_key;
        /*To keep track of value.*/
        int L_value;
        /*To keep track of preds and curs of blue and red list.*/
        G_node **L_preds = new G_node*[2];
        G_node **L_currs = new G_node*[2];
        /*To keep track of operataion status.*/
        OPN_STATUS L_opn_status;
        /*To keep track of operation.*/
        OPN_NAME L_opn;
        /*FLag to keep track of key is in Shared memory of Not : 
         * Is to be used to remove Insert then Delete problem.*/
        bool isKeyInSM = FALSE;
        /*Setter methods of the class.*/
        void setBucketId(int bucketId);
        void setKey(int key);
        void setVal(int value);
        void setPredsnCurrs(G_node** preds, G_node** currs, int id);
        void setOpnStatus(OPN_STATUS op_status);
        void setOpn(OPN_NAME opn);        
        
        /* Getter methods of the class.*/
        int getBucketId();
        int getKey();
        int getVal();
        void getPredsnCurrs(G_node** preds, G_node** currs);
        OPN_STATUS getOpnStatus();
        OPN_NAME getOpn();
                
        /*Destructor of the class.*/
        //~L_rec();
};

/**
 * This class define the structure of the log/buffer loacl to each transaction.
 **/
class L_txlog{
    public:
        /*Transaction Id.*/
        int L_tx_id;
        /*Variables to keep track of starvation freedom.*/
        int L_tutl;
        int L_tltl;
        int L_its;
        int L_cts;
        int L_wts;
        int L_comTime;
        /*lock to secure the changes to the global variables of this transaction.*/
        mutex G_lock;
        /*Variables to check the valid and state of the transaction.*/
        bool G_valid;
        TRANX_STATE G_state;
        /*Transaction status.*/
        OPN_STATUS L_tx_status;
        /*Vector to store all the operations performed by the transaction.*/
        vector<L_rec*> *L_list;
        /*Array of all the G_Nodes that are locked by current transaction.*/
        vector<G_node*> lockedNodes;
        /*Constructor of the class.*/
        L_txlog();
        /*Constructor of the class.*/
        L_txlog(int L_tx_id);
        /*Funtion to find an entry in vector L_list for a specific key.*/
        L_rec* L_find(L_txlog* txlog, int L_bucket_id, int L_key);
        /*Method described to do the Intra transaction validations.*/
        void intraTransValidation(L_rec* record_i, G_node** G_preds, G_node** G_currs);
        /*Destructor of the class.*/
        ~L_txlog();
};

/**
 * HashMap Class Declaration
 */
class HashMap
{
private:
    G_node **htable;
public:
    /*Constructor of the class.*/
    HashMap();
    ~HashMap();
    
    /*Hash Function*/
    int HashFunc(int key);
    
    /*Insert Key-Node in the appropriate position with a default version.*/
    void list_Ins(vector<G_node*> *lockedNodes, int L_key, int* L_val, G_node** G_preds, G_node** G_currs, LIST_TYPE lst_type, L_txlog *tranx);
    
    /*Function to determine preds and curs.*/
    OPN_STATUS list_LookUp(vector<G_node*> *lockedNodes, int L_bucket_id, int L_key, G_node** G_preds, G_node** G_currs);
    
    /*Identify the right version of a G_node that is largest but less than current transaction id.*/
    version* find_lts(int L_wts, int L_tx_id, G_node *G_curr, version **nextVer);
    
    /*Check for a Key-Node if not then create one.*/
    OPN_STATUS commonLuNDel(vector<G_node*> *lockedNodes, L_txlog *tranx, int L_bucket_id, int L_key, int* L_val, G_node** G_preds, G_node** G_currs, int *L_tutl, int *L_tltl);
    
    /*Method to find the closest tuple created by transaction Tj with the largest timestamp smaller than L_tx_id.*/
    bool check_version(int L_wts, int L_tx_id, G_node* G_curr, version **prevVer, version **nextVer);
    
    /*Method to add a version in the appropriate Key_node version list in sorted order.*/
    void insertVersion(int L_wts, int L_val, G_node* key_Node, int L_tltl);
    
    /*Delete Element at a key*/
    void list_Del(G_node** preds, G_node** currs);
    
    /*Print the current table contents.*/
    void printHashMap(int L_bucket_id);
};

/*Function to acquire all locks taken during listLookUp().*/
OPN_STATUS acquirePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs);
/*Function to release all locks taken during listLookUp().*/
void releasePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs);
/*This method identifies the conflicts among the concurrent methods of different transactions.*/
OPN_STATUS methodValidation(G_node** G_preds, G_node** G_currs);
/*Function to find a node among locked nodes.*/
bool isNodeLocked(vector<G_node*> *lockedNodes, G_node* g_node);
/*Method to delete the preds and currs locked by the transaction from the locked list.*/
void removePredsCurrsFromLockList(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs);
void removePredsCurrs(vector<G_node*> *lockedNodes, G_node* g_node);
/*Function required to calculate the smaller and larger versions from the allRVL list.*/
void getLarAndSm(int G_wts, vector<L_txlog*> *G_rvl, list<L_txlog*> *largeRVL, list<L_txlog*> *smallRVL);
#endif /* dataStructure_hpp */
