

#include "MV_OSTM.hpp"

/*
 *Default constructor of the class L_txlog.
 */
L_txlog::L_txlog()
{
	//Initialize the L_its to NIL.
	this->L_its = NIL;
}

/*
 *Constructor to the class L_txlog.
 */
L_txlog::L_txlog(int L_tx_id)
{
    //Initialize the transaction ID.
    this->L_tx_id = L_tx_id;
    this->L_tx_status = OK;
    this->L_list = new vector<L_rec*>;
}

/*
 * Funtion to find an entry in vector L_list for a specific key.
 */
L_rec* L_txlog::L_find(L_txlog* txlog, int L_bucket_id, int L_key)
{
    L_rec *record = NULL;
     
    //Every method first identify the node corresponding to the key into local log.
    for(int i=0;i<txlog->L_list->size();i++)
    {
        if((txlog->L_list->at(i)->getKey() == L_key) && (txlog->L_list->at(i)->getBucketId() == L_bucket_id))
        {
			record = txlog->L_list->at(i);
            break;
        }
    }
    return record;
}

/*
 *Method described to do the Intra transaction validations.
 */
void L_txlog::intraTransValidation(L_rec* record_k, G_node** G_preds, G_node** G_currs)
{	
	if((G_preds[0]->G_mark) || (G_preds[0]->blue_next->G_key != G_currs[1]->G_key))
    {
		if(record_k->getOpn() == INSERT)
        {
			if(isNodeLocked(&this->lockedNodes, record_k->L_preds[0]->blue_next) == FALSE)
			{
				record_k->L_preds[0]->blue_next->lmutex.lock();
				cout<<"Lock "<<record_k->L_preds[0]->blue_next->G_key<<endl;
            }
            G_preds[0] = record_k->L_preds[0]->blue_next;
        } else {
			if(isNodeLocked(&this->lockedNodes, record_k->L_preds[0]) == FALSE)
			{
				record_k->L_preds[0]->lmutex.lock();
				cout<<"Lock "<<record_k->L_preds[0]->G_key<<endl;
			}
            G_preds[0] = record_k->L_preds[0];
        }
    }
    //If G_currs[0] and G_preds[1] is modified by prev operation then update them also.
    if(G_preds[1]->red_next->G_key != G_currs[0]->G_key)
    {
		if(isNodeLocked(&this->lockedNodes, record_k->L_preds[1]->red_next) == FALSE)
		{
			record_k->L_preds[1]->red_next->lmutex.lock();
			cout<<"Lock "<<record_k->L_preds[1]->red_next->G_key<<endl;
		}
        G_preds[1] = record_k->L_preds[1]->red_next;
    }
}

/*
 * Destructor to the class L_txlog.
 */
L_txlog::~L_txlog()
{
    for(int i = 0; i < this->L_list->size(); i++)
    {
        L_list->at(i)->~L_rec();
    }
    delete[] this->L_list;
}

/*******************************************************************************************/
/*
 * Set location of G_pred and G_curr acording to the node corrsponding to the key
 * into transaction local log.
 */
void L_rec::setPredsnCurrs(G_node** preds, G_node** currs, int id)
{
	if(preds[0] == NULL|| preds[1] == NULL || currs[0] == NULL || currs[1] == NULL)
        cout<<preds[0]<<" "<<preds[1]<<" "<<currs[0]<<" "<<currs[1]<<endl;
        if(preds[0]->blue_next == NULL)
        cout<<"Hell YA "<<id;
        if(preds[1]->red_next==NULL)
        cout<<"Helllow";
	
    this->L_preds[0] = preds[0];
    this->L_preds[1] = preds[1];
    this->L_currs[0] = currs[0];
    this->L_currs[1] = currs[1];   
}

/*
 * Set method name into transaction local log.
 */
void L_rec::setOpn(OPN_NAME opn)
{
    this->L_opn = opn;
}

/*
 * Set status of method into transaction local log.
 */
void L_rec::setOpnStatus(OPN_STATUS op_status)
{
    this->L_opn_status = op_status;
}

/*
 * Set value of the key into transaction local log.
 */
void L_rec::setVal(int value)
{
    this->L_value = value;
}

/*
 * Set key corresponding to the method from transaction local log.
 */
void L_rec::setKey(int key)
{
    this->L_key = key;
}

/*
 * Set bucket Id corresponding to the method from transaction local log.
 */
void L_rec::setBucketId(int bucketId)
{
    this->L_bucket_id = bucketId;
}

/*
 * Get location of G_preds and G_currs according to thenode corresponding
 * to the key from the transaction local log.
 */
void L_rec::getPredsnCurrs(G_node** preds, G_node** currs)
{
    preds[0] = this->L_preds[0];
    preds[1] = this->L_preds[1];
    currs[0] = this->L_currs[0];
    currs[1] = this->L_currs[1];
}

/*
 * Get method name from the transaction local log.
 */
OPN_NAME L_rec::getOpn()
{
    return this->L_opn;
}

/*
 * Get status of the method from transaction local log.
 */
OPN_STATUS L_rec::getOpnStatus()
{
    return this->L_opn_status;
}

/*
 * Get value of the key from transaction local log.
 */
int L_rec::getVal()
{
    return this->L_value;
}

/*
 * Get key corresponding to the method from transaction local log.
 */
int L_rec::getKey()
{
    return this->L_key;
}

/*
 * Get L_bucket_id corresponding to the method from transaction local log.
 */
int L_rec::getBucketId()
{
    return this->L_bucket_id;
}

/*************************** MV_OSTM class operations are defined here.***************************/

/*
 * Constructor to the class MV-OSTM.
 */
MV_OSTM::MV_OSTM()
{
    init();
    /*shared memory - will init hashtablle as in main.cpp*/
    hash_table = new HashMap();
    //Intialize the gloabl counter.
    //G_cnt.store(0);
}

/*
 * Destructor of the class MV_OSTM.
 */
MV_OSTM::~MV_OSTM()
{
    free(this);
}

/*
 * This method is invoked at the start of the STM system.
 * Initializes the global counter(G_cnt).
 */
void MV_OSTM::init()
{
    //Initiazation of global counter to 0.
    G_cnt.store(1);
}
/*
 * It is invoked by a thread to being a new transaction Ti.
 * It creates transactional log and allocate unique id.
 */
L_txlog* MV_OSTM::begin(int its)
{
    //Get the transaction Id.
    int tx_id = G_cnt++;
    L_txlog* tx_log = new L_txlog(tx_id);
    //Set the local its, cts, and wts of the transaction.
	if(its == NIL)
	{	
		tx_log->L_its = tx_id;
		tx_log->L_cts = tx_id;
	} else {
		tx_log->L_its = its;
		tx_log->L_cts = tx_id;
	}
	tx_log->G_state = LIVE_TX;
	tx_log->G_valid = TRUE;

	//Return the tranx log instance to the calling part.
    return tx_log;
}

/*
 * This method looks up for a key-Node first in local log if not found then in the shared memory.
 */
OPN_STATUS MV_OSTM::tx_lookup(L_txlog* txlog, int L_key, int* value)
{
	//To keep track of the position at which the record to be inserted in the tx log.
	bool flag = FALSE;
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    //Value to be returned.
    int L_val;
    value = &L_val;
    //The bucket id for the corresponding key is.
    int L_bucket_id = hash_table->HashFunc(L_key);
    //first identify the node corresponding to the key in the local log.
    L_rec *record = txlog->L_find(txlog,L_bucket_id,L_key);
    
    if(record != NULL && record->isKeyInSM == TRUE)
    {
        //Getting the previous operation's name.
        OPN_NAME L_opn = record->getOpn();
        
        //If previous operation is insert/lookup then get the value/opn_status
        //based on the previous operations value/op_status.
        if(INSERT == L_opn || LOOKUP == L_opn)
        {
            L_val = record->getVal();
            L_opn_status = record->getOpnStatus();
        } else
        {
            //If the previous operation is delete then set the value as NULL
            value = NULL;
            L_opn_status = FAIL;
        }
    } else if(record != NULL && record->isKeyInSM == TRUE)
    {
			/*Sanity Check*/
		txlog->G_lock.lock();
		if(txlog->G_valid == FALSE)
		{
			txlog->G_lock.unlock();
			return ABORT;
		}
		txlog->G_lock.unlock();
		G_node *G_preds[2];
        G_node *G_currs[2];
        //If node corresponding to the key is not a part of local log.
        L_opn_status = hash_table->commonLuNDel(&txlog->lockedNodes, txlog,L_bucket_id,L_key,value,G_preds,G_currs);
        if(L_opn_status == ABORT)
        {
			return L_opn_status;
        }
        //Create local log record and append it into increasing order of keys.
        if(value != NULL)
            record->setVal(*value);
        else
            record->setVal(-1);
        record->setPredsnCurrs(G_preds, G_currs, 343);
		//Update the local log.
		record->setOpn(LOOKUP);
		record->setOpnStatus(L_opn_status);
		//Set the flag to TRUE : stating that the key is inserted in SM.
		record->isKeyInSM = TRUE;	 
	} else
    {
		/*Sanity Check*/
		txlog->G_lock.lock();
		if(txlog->G_valid == FALSE)
		{
			txlog->G_lock.unlock();
			return ABORT;
		}
		txlog->G_lock.unlock();
		G_node *G_preds[2];
        G_node *G_currs[2];
        //If node corresponding to the key is not a part of local log.
        L_opn_status = hash_table->commonLuNDel(&txlog->lockedNodes, txlog,L_bucket_id,L_key,value,G_preds,G_currs);
        if(L_opn_status == ABORT)
        {
			return L_opn_status;
        }
        //Create local log record and append it into increasing order of keys.
        record = new L_rec();
        record->setBucketId(L_bucket_id);
        record->setKey(L_key);
        if(value != NULL)
            record->setVal(*value);
        else
            record->setVal(-1);
        record->setPredsnCurrs(G_preds, G_currs, 343);
        //Add itself to the local log.
        for(int i=0;i<txlog->L_list->size();i++)
        {
            //Insert the record in the sorted order.
            if(txlog->L_list->at(i)->L_key > L_key)
            {
                txlog->L_list->insert(txlog->L_list->begin()+i, record);
                flag = TRUE;
                break;
            }
        }
        if(flag == FALSE)
        {
			txlog->L_list->push_back(record);
		}
		//Update the local log.
		record->setOpn(LOOKUP);
		record->setOpnStatus(L_opn_status);
		//Set the flag to TRUE : stating that the key is inserted in SM.
		record->isKeyInSM = TRUE;
    }
    //Return the operation status.
    return L_opn_status;
}


/*
 * This method deletes up for a key-Node first in local logif not found then in the shared memory.
 */
OPN_STATUS MV_OSTM::tx_delete(L_txlog* txlog, int L_key, int* value)
{
	//To keep track of the position at which the record to be inserted in the tx log.
	bool flag = FALSE;
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    //Value to be returned.
    int L_val;
    value = &L_val;
    //The bucket id for the corresponding key is.
    int L_bucket_id = hash_table->HashFunc(L_key);
    //first identify the node corresponding to the key in the local log.
    L_rec *record = txlog->L_find(txlog,L_bucket_id,L_key);
    
    if(record != NULL && record->isKeyInSM == TRUE)
    {
        //Getting the previous operation's name.
        OPN_NAME L_opn = record->getOpn();
        
        /*If previous operation is insert then get the value based on the previous
        operations value and set the value as NULL and operation name as DELETE.*/
        if(INSERT == L_opn)
        {
            L_val = record->getVal();
            L_opn_status = OK;
            //Update the local log.
			record->setOpn(DELETE);
			record->setOpnStatus(L_opn_status);
        } else if(DELETE == L_opn)
        {
            //If the previous operation is delete then set the value as NULL
            value = NULL;
            L_opn_status = FAIL;
        } else {
            /*If previous operation is lookup then get the value based on the previous
              operations value and set the value as NULL and operation name as DELETE.*/
            L_val = record->getVal();
            L_opn_status = record->getOpnStatus();
            //Update the local log.
			record->setOpn(DELETE);
            record->setOpnStatus(L_opn_status);
        }
        
        record->setVal(-1);
    } else if(record != NULL && record->isKeyInSM == FALSE)
    {
		value = NULL;
		G_node *G_preds[2];
        G_node *G_currs[2];
        //If node corresponding to the key is not a part of local log.
        L_opn_status = hash_table->commonLuNDel(&txlog->lockedNodes,txlog,L_bucket_id,L_key,value,G_preds,G_currs);
        //If operation status is returned as ABORT then abort the transaction.
		if(L_opn_status == ABORT)
        {
			return L_opn_status;
        }
		//Set the value of the record.
		if(value != NULL)
            record->setVal(*value);
        else
            record->setVal(-1);
        //Set the preds and curs of the record.
        record->setPredsnCurrs(G_preds, G_currs, 441);
        //Set the flag to TRUE : stating that the key is inserted in SM.
		record->isKeyInSM = TRUE;
		//Update the local log.
		record->setOpn(DELETE);
		record->setOpnStatus(L_opn_status);
    } else {
		/*Sanity Check*/
		txlog->G_lock.lock();
		if(txlog->G_valid == FALSE)
		{
			txlog->G_lock.unlock();
			return ABORT;
		}
		txlog->G_lock.unlock();
		
		value = NULL;
        G_node *G_preds[2];
        G_node *G_currs[2];
        //If node corresponding to the key is not a part of local log.
        L_opn_status = hash_table->commonLuNDel(&txlog->lockedNodes,txlog,L_bucket_id,L_key,value,G_preds,G_currs);
        if(L_opn_status == ABORT)
        {
			return L_opn_status;
        }
        //Create local log record and append it into increasing order of keys.
        record = new L_rec();
        record->setBucketId(L_bucket_id);
        record->setKey(L_key);
        //Set the value of the record.
        if(value != NULL)
            record->setVal(*value);
        else
            record->setVal(-1);
        record->setPredsnCurrs(G_preds, G_currs, 485);
        //Add itself to the local log.
        for(int i=0;i<txlog->L_list->size();i++)
        {
            //Insert the record in the sorted order.
            if(txlog->L_list->at(i)->L_key > L_key)
            {
                txlog->L_list->insert(txlog->L_list->begin()+i, record);
                flag = TRUE;
                break;
            }
        }
        if(flag == FALSE)
        {
			txlog->L_list->push_back(record);
		}
		//Set the flag to TRUE : stating that the key is inserted in SM.
		record->isKeyInSM = TRUE;
		//Update the local log.
		record->setOpn(DELETE);
		record->setOpnStatus(L_opn_status);
    }
    //Return the operation status.
    return L_opn_status;
}

/*
 * This method inserts a key-Node in local log and optimistically,
 * the actual insertion happens optimistically in tryCommit() method.
 */
OPN_STATUS MV_OSTM::tx_insert(L_txlog* txlog, int L_key, int L_val)
{
	//To keep track of the position at which the record to be inserted in the tx log.
	bool flag = FALSE;
	//The bucket id for the corresponding key is.
    int L_bucket_id = hash_table->HashFunc(L_key);
    
    //first identify the node corresponding to the key in the local log.
    L_rec *record = txlog->L_find(txlog,L_bucket_id,L_key);
    
    if(record == NULL)
    {
		//Create local log record and append it into increasing order of keys.
        record = new L_rec();
        record->setBucketId(L_bucket_id);
        record->setKey(L_key);
        for(int i=0;i<txlog->L_list->size();i++)
        {
            //Insert the record in the sorted order.
            if(txlog->L_list->at(i)->L_key > L_key)
            {
                txlog->L_list->insert(txlog->L_list->begin()+i, record);
                flag = TRUE;
                break;
            }
        }
        if(flag == FALSE)
        {
			txlog->L_list->push_back(record);
		}
    }
    /*Updating the local log.*/
    record->setVal(L_val);
    record->setOpn(INSERT);
    record->setOpnStatus(OK);
    return OK;
}

/*
 * This method is used to commit the transactions.
 */
OPN_STATUS MV_OSTM::tryCommit(L_txlog* txlog)
{
	// Sanity Check
	txlog->G_lock.lock();
	if(txlog->G_valid == FALSE)
	{
		txlog->G_valid = FALSE;
		txlog->G_state = ABORT_TX;
		txlog->G_lock.unlock();
		return ABORT;
	}
	txlog->G_lock.unlock();
	
	// Iterator objects.
	L_txlog *tranx_iterator;
    list<L_txlog*>::iterator list_iterator;
    vector<L_txlog*>::iterator vector_iterator;
	list<int>::iterator ver_iterator;
	// Lists used to ensure starvation freedom.
	list<L_txlog*> *allRVL = new list<L_txlog*>;
	// Get the local log list corresponding to each transaction which is in increasing order of keys.
    vector<L_rec*> *L_list = new vector<L_rec*>;
    
    // Sort the records in the txlog of the transaction.
	for(int i=0;i<TABLE_SIZE;i++)
    {
		for(int j=0;j<txlog->L_list->size();j++)
		{
			if(txlog->L_list->at(j)->getBucketId() == i && ((txlog->L_list->at(j)->getOpn() == INSERT) 
				|| (txlog->L_list->at(j)->getOpn() == DELETE && txlog->L_list->at(j)->getOpnStatus() == OK)))
			{
				L_list->push_back(txlog->L_list->at(j));
			}
		}
	}
	 
    // Identify the new G_preds[] and G_currs[] for all update methods of a transaction and validate it.
    
    for(int i=0;i<L_list->size();i++)
    {
		// Identify the new G_pred and G_curr location with the help of listLookUp().
        G_node *G_preds[2];
        G_node *G_currs[2];
        
        hash_table->list_LookUp(&txlog->lockedNodes, L_list->at(i)->getBucketId(), L_list->at(i)->getKey(), G_preds, G_currs);
        
        int L_key = L_list->at(i)->getKey();

		if(G_currs[1]->G_key != L_key && G_currs[0]->G_key != L_key)
		{ 
			// Update local log entry.
			L_list->at(i)->setPredsnCurrs(G_preds, G_currs, 610); 
			continue;
		 }
        
        if(G_currs[1]->G_key == L_key)
        {
			vector_iterator = G_currs[1]->G_vl->at(0)->G_rvl->begin();
			while(vector_iterator != G_currs[1]->G_vl->at(0)->G_rvl->end())
			{
				tranx_iterator = *vector_iterator;
				insertAndSortRVL(allRVL,tranx_iterator);
				vector_iterator++;
			}
		} else {
			vector_iterator = G_currs[0]->G_vl->at(0)->G_rvl->begin();
			while(vector_iterator != G_currs[0]->G_vl->at(0)->G_rvl->end())
			{
				tranx_iterator = *vector_iterator;
				insertAndSortRVL(allRVL,tranx_iterator);
				vector_iterator++;
			}
		}
		                  
        // Update local log entry.
        L_list->at(i)->setPredsnCurrs(G_preds, G_currs, 659);
    }
    
    // Add current transaction to allRVL.
    insertAndSortRVL(allRVL, txlog);
    
     
    // Acquire lock on all the transactions in the allRVL list.
    list_iterator = allRVL->begin();
    while(list_iterator != allRVL->end())
    {
		tranx_iterator = *list_iterator;
		//cout<<tranx_iterator->L_cts<<" ";
		tranx_iterator->G_lock.lock();
		list_iterator++;
	}
    
    // Verify if G_valid of current transaction is FALSE.
    if(txlog->G_valid == FALSE)
    {
		txlog->G_state = ABORT_TX;
		// Unlock all the transactions.
		list_iterator = allRVL->begin();
		while(list_iterator != allRVL->end())
		{
			tranx_iterator = *list_iterator;
			tranx_iterator->G_lock.unlock();
			list_iterator++;
		}
		// Unlock all the variables.
        for(int j=0;j<txlog->lockedNodes.size();j++)
        {
			//cout<<"UnLock "<<txlog->lockedNodes.at(j)->G_key<<endl;
			txlog->lockedNodes.at(j)->lmutex.unlock();
        }
		return ABORT;
	}
    
    // Initialize abort Return Value List (abortRVL).
    list<L_txlog*> *abortRVL = new list<L_txlog*>;
    
    if(txlog->L_its == findLTS(allRVL))
    {
		list_iterator = allRVL->begin();
		while(list_iterator != allRVL->end())
		{
			tranx_iterator = *list_iterator;
			
			if(tranx_iterator->L_cts == txlog->L_cts || (tranx_iterator->G_valid == FALSE)||(tranx_iterator->G_state == ABORT_TX))
			{
				list_iterator++;
				continue;
			}
			// Transaction Tk has lower priority and not yet commited, so needs to be aborted.
			// Store Tk in the abortRVL.
				abortRVL->push_back(tranx_iterator);
			list_iterator++;
		}	
	} else {
			txlog->G_state = ABORT_TX;
			txlog->G_valid = FALSE;
			// Unlock all the transactions.
			list_iterator = allRVL->begin();
			while(list_iterator != allRVL->end())
			{
				tranx_iterator = *list_iterator;
				tranx_iterator->G_lock.unlock();
				list_iterator++;
			}
			// Unlock all the variables.
			for(int j=0;j<txlog->lockedNodes.size();j++)
			{
				//cout<<"UnLock "<<txlog->lockedNodes.at(j)->G_key<<endl;
				txlog->lockedNodes.at(j)->lmutex.unlock();
			}
			return ABORT;
		}
	
	
	// Abort all the transactions in abortRVL since Ti cannot abort.
	list_iterator = abortRVL->begin();
    while(list_iterator != abortRVL->end())
    {
		tranx_iterator = *list_iterator;
		tranx_iterator->G_valid = FALSE;
		tranx_iterator->G_state = ABORT_TX;
		list_iterator++;
	}
	
    // Get each update method one by one and take effect in underlying datastructure.
    for(int i=0;i<L_list->size();i++)
    {
        G_node *G_preds[2];
        G_node *G_currs[2];
        L_rec* record_i = L_list->at(i);
        L_rec* record_k = L_list->at(i);
        if(i>0)
            record_k = L_list->at(i-1);
        OPN_NAME L_opn = record_i->getOpn();
        int L_key = record_i->getKey();
        int val = record_i->getVal();
        //int L_tx_id = txlog->L_tx_id;
        record_i->getPredsnCurrs(G_preds, G_currs);
        
        // Modify the G_preds[] and G_currs[] for the consecutive update methods which are working on overlapping zone in lazy-list.
        if(i>0)
			txlog->intraTransValidation(record_k, G_preds, G_currs);
	
		isNodeLocked(&txlog->lockedNodes, G_preds[0]);
		isNodeLocked(&txlog->lockedNodes, G_preds[1]);
		
		// If operation is insert then after successfull completion of its node corresponding to the key should be part of BL.
        if(L_opn == INSERT)
        {
			if(G_currs[1]->G_key == L_key)
            {
                // Add a new version to the respective G_node.
              hash_table->insertVersion(txlog->L_cts,val,G_currs[1]);
            } else if(G_currs[0]->G_key == L_key) {
				hash_table->list_Ins(&(txlog->lockedNodes), L_key, &val, G_preds, G_currs, RL_BL, txlog);
                hash_table->insertVersion(txlog->L_cts,val,G_currs[0]);
            } else {
                hash_table->list_Ins(&(txlog->lockedNodes), L_key, &val, G_preds, G_currs, BL, txlog);
                isNodeLocked(&(txlog->lockedNodes), G_preds[0]->blue_next);
            }
        } else if(L_opn == DELETE)
        {
			if(G_currs[1]->G_key == L_key) {
			G_currs[1]->G_mark = true;
            hash_table->insertVersion(txlog->L_cts,-1,G_currs[1]);
            hash_table->list_Del(G_preds, G_currs);
			} 
        }
        // Update the pred and currs accordingly.
        L_list->at(i)->setPredsnCurrs(G_preds,G_currs, 894);
    }
    
	// Commit the current transaction.
	txlog->G_state = COMMIT_TX; 
   
	// Unlock all the transactions.
	list_iterator = allRVL->begin();
	while(list_iterator != allRVL->end())
	{
		tranx_iterator = *list_iterator;
		tranx_iterator->G_lock.unlock();
		list_iterator++;
	}
    
	// Unlock all the variables in the increasing order.
	for(int i=0;i<txlog->lockedNodes.size();i++)
	{
		//cout<<"UnLock "<<txlog->lockedNodes.at(i)->G_key<<endl;
		txlog->lockedNodes.at(i)->lmutex.unlock();
	}    
	
    return OK;
}

/*
 * This method is used to abort the transactions.
 */
OPN_STATUS MV_OSTM::tryAbort(L_txlog* txlog)
{
    return OK;
}


/*
 * Insert a transaction in the reader's list of a version of a transaction object.
 * */
void insertAndSortRVL(list<L_txlog*> *allRVL, L_txlog* txlog)
{
	bool insertFlag = FALSE;
	L_txlog *tranx_iterator;
    list<L_txlog*>::iterator list_iterator;	
	
	if(txlog != NULL && allRVL != NULL) {

		//If the reader's list is empty push the gtrans to the reader's list
		if(allRVL->size() == 0) {
			allRVL->push_back(txlog);
			return;
		} 
		//If the reader's list is not empty and has elements more than ONE.
		list_iterator = allRVL->begin();
		while(list_iterator != allRVL->end())
		{
			tranx_iterator = *list_iterator;
							
			/*if reader's list transaction is greater than passed 
				transaction's wts value insert the transaction*/
			if(txlog->L_cts < tranx_iterator->L_cts) {
				insertFlag = TRUE;
				break;
			} else if(txlog->L_cts == tranx_iterator->L_cts) {
					return;
			}
			list_iterator++;
		}
		
		//if not inserted, insert it in the last of the list
		if(insertFlag == FALSE) {
			allRVL->push_back(txlog);
		} else {
			allRVL->insert(list_iterator,txlog);
		}
	}	 
}

/*
 * Find the lowest its value amongst all the trasaction of the 
 * list passed as an argument to the function
 * */
long int findLTS(list<L_txlog*> *allRVL)
{
	//holds the minimum its
	long int min_its;
	L_txlog *tranx_iterator;
    list<L_txlog*>::iterator list_iterator;
	
	list_iterator = allRVL->begin();
	min_its = INFINITE;													//Not necessarily be LIVE ###########
		while(list_iterator != allRVL->end())
		{
			tranx_iterator = *list_iterator;
			if(tranx_iterator->L_its < min_its && tranx_iterator->G_state == LIVE_TX) {
				min_its = tranx_iterator->L_its;
			}
			list_iterator++;
		}
	return min_its;
}
