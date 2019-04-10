

#include "dataStructure.hpp"

/*
 * Constructor of the class G_node.
 */
G_node::G_node(int key)
{
	this->L_bucket_id =  (key % TABLE_SIZE);
	this->G_key = key;
    this->red_next = NULL;
    this->blue_next = NULL;
    this->G_mark = DEFAULT_MARKED;
    //push the default version back to the G_node.
    version *T0_version = new version;
    T0_version->G_ts = 0;
    T0_version->G_val = -1;
    T0_version->G_vrt = 0;
    this->G_vl->push_back(T0_version);
}
/*
 * Method to compare two G_nodes on the basis of their keys.
 */
bool G_node::compareG_nodes(G_node* node)
{
    if(this->L_bucket_id == node->L_bucket_id && this->G_key == node->G_key)
    {
        return TRUE;
    }
    return FALSE;
}

/**********************************************************************/

/*
 * Constructor of the class HashMap.
 */
HashMap::HashMap()
{
    htable = new G_node* [TABLE_SIZE];
    /*Initialize head and tail sentinals nodes for all 
     * the buckets of HashMap.*/
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        //htable[i] = NULL;
        htable[i] = new G_node(INT_MIN);
        htable[i]->L_bucket_id = i;
        htable[i]->red_next = new G_node(INT_MAX);
        htable[i]->red_next->L_bucket_id = i;
        htable[i]->blue_next = htable[i]->red_next;
    }
}

/*
 * Destructor of the class HashMap.
 */
HashMap::~HashMap()
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        if (htable[i] != NULL)
        {
            G_node *prev = NULL;
            G_node *entry = htable[i];
            while (entry != NULL)
            {
                prev = entry;
                entry = entry->red_next;
                delete prev;
            }
        }
    }
    delete[] htable;
}

/*
 * Function to define the object Id or bucket number.
 */
int HashMap::HashFunc(int key)
{
    return key % TABLE_SIZE;
}

/*
 *Insert Key-Node in the appropriate position with a default version.
 */
void HashMap::list_Ins(vector<G_node*> *lockedNodes, int L_key, int *L_val, G_node** G_preds, 
					   G_node** G_currs, LIST_TYPE list_type, 
					   L_txlog *tranx)
{
	//Get the tx_id of the transaction passed.
	int L_wts = tranx->L_wts;
	
    //Inserting the node from redList to blueList
    if(RL_BL == list_type)
    { 
		if(G_currs[0]->G_mark == TRUE)
		{
			G_currs[0]->G_mark = FALSE;
			G_currs[0]->blue_next = G_currs[1];
			G_preds[0]->blue_next = G_currs[0];
			assert(L_key == G_currs[0]->G_key);
		}
    }
    //Inserting the node in redlist only.
    else if(RL == list_type)
    {
		G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        //cout<<"Lock "<<node->G_key<<endl;
        //Add current transaction to the rv-list of the 0th version.
        node->G_vl->at(0)->G_rvl->push_back(tranx);
        node->G_mark = TRUE;
        node->red_next = G_currs[0];
        //node->blue_next = G_currs[1];
        //Find the node is locked or not ?
		for(int i=0;i<lockedNodes->size();i++)
		{
			if(lockedNodes->at(i)->L_bucket_id == G_currs[0]->L_bucket_id && lockedNodes->at(i)->G_key == G_currs[0]->G_key)
			{
				//If yes then erase it from the locked list.
				lockedNodes->erase(lockedNodes->begin()+i);
				//Unlock the G_node.
				G_currs[0]->lmutex.unlock();
				//cout<<"UnLock "<<G_currs[0]->G_key<<endl;
				break;
			}
		}
        G_currs[0] = node;
        G_preds[1]->red_next = G_currs[0];
     }
    //Inserting the node in red as well as blue list.
    else
    {
		G_node *node = new G_node(L_key);
        //After creating node lock it.
        node->lmutex.lock();
        //cout<<"Lock "<<node->G_key<<endl;
        //Add another version with current transaction's timestamp.
        version *T_key_version = new version;
        T_key_version->G_ts = L_wts;
        if(L_val != NULL)
            T_key_version->G_val = *L_val;
        else
            T_key_version->G_val = -1;
        //Update the G_vrt for thisversion.
        T_key_version->G_vrt = tranx->L_cts;
       
        //Push the current timestamp version to the node's RV list.
        node->G_vl->push_back(T_key_version);
        node->red_next = G_currs[0];
        node->blue_next = G_currs[1];
        G_preds[1]->red_next = node;
        G_preds[0]->blue_next = node;
    }
 }

/*
 * Funtion to determine preds and currs.
 */
OPN_STATUS HashMap::list_LookUp(vector<G_node*> *lockedNodes, int L_bucket_id, int L_key, 
								G_node** G_preds, G_node** G_currs)
{
    OPN_STATUS op_status = RETRY;
    G_node *head = NULL;
    
    //If key to search is not in the range b/w -infinity to +infinity.
    if((L_key <= INT_MIN) && (L_key >= INT_MAX))
    {
        assert((L_key > INT_MIN) && (L_key < INT_MAX));
    }
    
    //Run until status of operation doesn't change from RETRY.
    while(RETRY == op_status)
    {
        /*Get the head of the bucket in chaining hash-table with the 
         * help of L_obj_id and L_key.*/
        //if bucket is empty
        if (htable[L_bucket_id] == NULL)
        {
            cout<<"bucket is empty \n";
            return BUCKET_EMPTY;
        }
        else
        {
            head = htable[L_bucket_id];
        }
        
        G_preds[0] = head;
        G_currs[1] = G_preds[0]->blue_next;
        
        //search blue pred and curr
        while(G_currs[1] != NULL && G_currs[1]->G_key < L_key)
        {
            G_preds[0] = G_currs[1];
            G_currs[1] = G_currs[1]->blue_next;
        }
        G_preds[1] = G_preds[0];
        G_currs[0] = G_preds[0]->red_next;
        
        //search red pred and curr
        while(G_currs[0] != NULL && G_currs[0]->G_key < L_key)
        {
            G_preds[1] = G_currs[0];
            G_currs[0] = G_currs[0]->red_next;
        }
        
        //Acquire locks on all preds and currs.
        op_status = acquirePredCurrLocks(lockedNodes, G_preds, G_currs);
		
		if(RETRY == op_status)
        {
			continue;
		}
        //validation
        op_status = methodValidation(G_preds, G_currs);
        
        /*If op_status is still RETRY and not OK release all the aquired 
         * locks and try again.*/
        if(RETRY == op_status)
        {
            releasePredCurrLocks(lockedNodes, G_preds, G_currs);
        }
    }
    
    #if DEBUG_LOGS
    cout<<"list_LookUp:: nodes " <<G_preds[0]->G_key<<" "<< 
		G_preds[1]->G_key<<" "<<G_currs[0]->G_key<<" "<<
		G_currs[1]->G_key<<" "<<L_key<<endl;
    #endif // DEBUG_LOGS
    return op_status;
}

/*
 * Identify the right version of a G_node that is largest but less than 
 * current transaction id.
 */
version* HashMap::find_lts(int L_wts, int L_tx_id, G_node *G_curr, version **nextVer)
{
	bool flag = FALSE;
    //Initialize the closest tuple and next version.
    version *closest_tuple = new version();
    *nextVer = new version();
    closest_tuple->G_ts = 0;
    
    /*For all the versions of G_currs[] identify the largest timestamp 
     * less than L_tx_id.*/
    for(int i=0; i<G_curr->G_vl->size();i++)
    {
        int p = G_curr->G_vl->at(i)->G_ts;
        if((p < L_wts) && (closest_tuple->G_ts <= p)) {
            closest_tuple->G_ts = G_curr->G_vl->at(i)->G_ts;
            closest_tuple->G_val = G_curr->G_vl->at(i)->G_val;
            closest_tuple->G_rvl = G_curr->G_vl->at(i)->G_rvl;
            closest_tuple->G_vrt = G_curr->G_vl->at(i)->G_vrt;
            
            //Get the nextVersion saved as well.
            if((i+1)<G_curr->G_vl->size())
            {
				(*nextVer)->G_ts = G_curr->G_vl->at(i+1)->G_ts;
				(*nextVer)->G_val = G_curr->G_vl->at(i+1)->G_val;
				(*nextVer)->G_rvl = G_curr->G_vl->at(i+1)->G_rvl;
				(*nextVer)->G_vrt = G_curr->G_vl->at(i)->G_vrt;
			} else {
				*nextVer = NULL;
			}
            
            flag = TRUE;
        } else if((p == L_wts) && (closest_tuple->G_ts <= p)) {
			if(G_curr->G_vl->at(i)->G_vrt < L_tx_id) {
				closest_tuple->G_ts = G_curr->G_vl->at(i)->G_ts;
				closest_tuple->G_val = G_curr->G_vl->at(i)->G_val;
				closest_tuple->G_rvl = G_curr->G_vl->at(i)->G_rvl;
				closest_tuple->G_vrt = G_curr->G_vl->at(i)->G_vrt;
				
				//Get the nextVersion saved as well.
				if((i+1)<G_curr->G_vl->size())
				{
					(*nextVer)->G_ts = G_curr->G_vl->at(i+1)->G_ts;
					(*nextVer)->G_val = G_curr->G_vl->at(i+1)->G_val;
					(*nextVer)->G_rvl = G_curr->G_vl->at(i+1)->G_rvl;
					(*nextVer)->G_vrt = G_curr->G_vl->at(i)->G_vrt;
				} else {
					*nextVer = NULL;
				}
			}
			
		}
    }
    //If no version is find to be read from.
    if(flag == FALSE) 
    {
		closest_tuple = NULL;
		*nextVer = NULL;
	}
    return closest_tuple;
}

/*
 * This method does below steps ->
 * 1. Check if the key exists or not, if yes then check for the LTS 
 *    version to read from.
 * 2. Else create the key and its respective version to be read from.
 * 3. If version to be read from is found then add current transaction 
 * 	  to its RV list.
 */

OPN_STATUS HashMap:: commonLuNDel(vector<G_node*> *lockedNodes, L_txlog *tranx, int L_bucket_id, int L_key, 
								  int* L_val, G_node** G_preds, 
								  G_node** G_currs, int *G_tutl, int *G_tltl)
{
	//Get the tx_id of the transaction passed.
	int L_wts = tranx->L_wts;
	int L_tx_id = tranx->L_tx_id;
    //Operation status to be returned.
    OPN_STATUS L_opn_status;
    /*If node corresponding to the key is not present in local log then 
     * search into underlying DS with the help of listLookUp().*/
    L_opn_status = list_LookUp(lockedNodes, L_bucket_id,L_key,G_preds,G_currs);
    
    //If node corresponding to the key is part of BL.
    if(G_currs[1]->G_key == L_key)
    {
		version *nextVer;
        version *curVer = find_lts(L_wts, L_tx_id, G_currs[1], &nextVer);
        if(curVer == NULL)
        { 
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}
		//Returns the smallest ts value greater than G_wts else NIL.
		 if(nextVer != NULL)
        { 
			(*G_tutl) = min(*G_tutl, nextVer->G_vrt-1);
		}
		(*G_tltl) = max(*G_tltl, curVer->G_vrt+1);
		//If limits have crossed each other, then T is aborted.
		if((*G_tltl) > (*G_tutl))
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}		
				
        // Push the read entry of tranx to the rvl of this version.
        curVer->G_rvl->push_back(tranx);
        
        /*If the Key-Node mark field is TRUE then L_op_status and 
         * L_val set as FAIL and NULL otherwise set OK and value of 
         * closest_tuple respectively.*/
        if(curVer->G_val == -1)
        {
			L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(curVer->G_val);
        }
    }
    //If node corresponding to the key is part of RL.
    else if(G_currs[0]->G_key == L_key)
    {
        version *nextVer;
        version *curVer = find_lts(L_wts, L_tx_id, G_currs[1], &nextVer);
        if(curVer == NULL)
        { 
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}
		//Returns the smallest ts value greater than G_wts else NIL.
		 if(nextVer != NULL)
        { 
			(*G_tutl) = min(*G_tutl, nextVer->G_vrt-1);
		}
		(*G_tltl) = max(*G_tltl, curVer->G_vrt+1);
		//If limits have crossed each other, then T is aborted.
		if((*G_tltl) > (*G_tutl))
		{
			//Releasing the locks in the incresing order.
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return ABORT;
		}	
        // Push the read entry of tranx to the rvl of this version.
        curVer->G_rvl->push_back(tranx);
        
        /*If the Key-Node mark field is TRUE then L_op_status and L_val 
         * set as FAIL and NULL otherwise set OK and value of 
         * closest_tuple respectively.*/
        if(curVer->G_val == -1)
        {
			L_opn_status = FAIL;
            L_val = NULL;
        } else {
            L_opn_status = OK;
            L_val = &(curVer->G_val);
        }
    }
    /*If node corresponding to the key is not part of RL as well as BL 
     * then create the node into RL with the help of list_Ins().*/
    else {
		//Insert the version tuple for transaction T0.
        list_Ins(lockedNodes, L_key, L_val, G_preds, G_currs, RL, tranx);
        //Push the new node lock in the lock list record.
        isNodeLocked(lockedNodes, G_currs[0]);
        L_opn_status = FAIL;
        int x = -1;
        L_val = &x;
    }
    
    //Releasing the locks in the incresing order.
    releasePredCurrLocks(lockedNodes, G_preds, G_currs);
    return L_opn_status;
}

/*
 *Method to find the closest tuple created by transaction Tj with the 
 * largest timestamp smaller than L_ix_id.
 */
bool HashMap::check_version(int L_wts, int L_tx_id, G_node* G_curr, version **prevVer, version **nextVer)
{
	 //Identify the tuple that has highest timestamp but less than itself.
    (*prevVer) = find_lts(L_wts, L_tx_id, G_curr, nextVer);
    
    if(*prevVer == NULL)
    {
		return FALSE;
	}
	 
    /*If in the rv list all the transactions are of lower ts then the 
     * current transaction then return TRUE.*/
    return TRUE;
}

/*
 * Method to add a version in the appropriate key_node version list 
 * in sorted order.
 */
void HashMap::insertVersion(int L_wts, int L_val, G_node* key_Node, int L_tltl)
{
    version *newVersion = new version();
    newVersion->G_ts = L_wts;
    newVersion->G_val = L_val;
    newVersion->G_vrt = L_tltl;
    
    /*If the size of the versionlist is less than K, then insert the 
    version in the appropriate order in the version list.*/
    if(key_Node->G_vl->size() < K) {
		for(int i=0;i<key_Node->G_vl->size();i++)
		{
			/*Look for an appropriate place to put the new version in 
			 * orer to maintain the order of the version list.*/
			if(key_Node->G_vl->at(i)->G_ts > L_wts)
			{
				key_Node->G_vl->insert(key_Node->G_vl->begin()+i, 
									   newVersion);
				return;
			}
		}
	} 
	/*If the version list is of size K or more than first delete the 
	 * first version of the list and then insert the new version in the 
	 * appropriate order in the version list.*/
	else {
		key_Node->G_vl->erase(key_Node->G_vl->begin());
		for(int i=0;i<key_Node->G_vl->size();i++)
		{
			/*Look for an appropriate place to put the new version 
			 * in orer to maintain the order of the version list.*/
			if(key_Node->G_vl->at(i)->G_ts > L_wts)
			{
				key_Node->G_vl->insert(key_Node->G_vl->begin()+i, 
				                       newVersion);
				return;
			}
		}
	}
    /*Else push at the last.*/
    key_Node->G_vl->push_back(newVersion);
}

/*
 *Function to delete a node from blue link and place it in red 
 * link after marking it.
 */
void HashMap::list_Del(G_node** G_preds, G_node** G_currs)
{
	if(G_currs[1]->blue_next == NULL)
        cout<<G_currs[1]->G_key;
    G_preds[0]->blue_next = G_currs[1]->blue_next;
    //G_currs[0]->blue_next = NULL;
}

/**
 * Print the current table contents.
 **/
void HashMap::printHashMap(int L_bucket_id)
{
	G_node *head = NULL;
	
	if (htable[L_bucket_id] == NULL)
    {
       elog("bucket is empty \n");
       return;
    }
    else
    {
       head = htable[L_bucket_id];
    }
       
    while(head != NULL && head->G_key < INT_MAX)
    {
		head = head->red_next;
    }
}
/**********************************************************************/

/*
 *Function to acquire all locks taken during listLookUp().
 */
OPN_STATUS acquirePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	if(isNodeLocked(lockedNodes, G_preds[0]) == FALSE)
	{
		if(RETRY == methodValidation(G_preds, G_currs))
		{	
			removePredsCurrs(lockedNodes,G_preds[0]);	
			return RETRY;
		}
		//cout<<"Lock "<<G_preds[0]->G_key<<endl;
		G_preds[0]->lmutex.lock();
	}
    if(isNodeLocked(lockedNodes, G_preds[1]) == FALSE)
    {
    	if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_preds[1]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		//cout<<"Lock "<<G_preds[1]->G_key<<endl;
		G_preds[1]->lmutex.lock();
	}
	if(isNodeLocked(lockedNodes, G_currs[0]) == FALSE)	
	{
		if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_currs[0]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		//cout<<"Lock "<<G_currs[0]->G_key<<endl;
		G_currs[0]->lmutex.lock();
	}
	if(isNodeLocked(lockedNodes, G_currs[1]) == FALSE)
	{
		if(RETRY == methodValidation(G_preds, G_currs))
    	{
			removePredsCurrs(lockedNodes,G_currs[1]);
			releasePredCurrLocks(lockedNodes, G_preds, G_currs);
			return RETRY;
		}
		//cout<<"Lock "<<G_currs[1]->G_key<<endl;
		G_currs[1]->lmutex.lock();
	}
	return OK;
}

/*
 *Function to release all locks taken during listLookUp().
 */
void releasePredCurrLocks(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	removePredsCurrsFromLockList(lockedNodes,G_preds,G_currs);
}

/*
 *This method identifies the conflicts among the concurrent methods of 
 * different transactions.
 */
OPN_STATUS methodValidation(G_node** G_preds, G_node** G_currs)
{
    /*Validating g_pred[] and G_currs[].*/
    if((G_preds[0]->G_mark) || (G_currs[1]->G_mark) || 
		(G_preds[0]->blue_next->G_key != G_currs[1]->G_key) || 
		(G_preds[1]->red_next->G_key != G_currs[0]->G_key))
		return RETRY;
    else
        return OK;
}
/*
 * Function to find a node among locked nodes.
 * */
bool isNodeLocked(vector<G_node*> *lockedNodes, G_node* g_node)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		if(lockedNodes->at(i)->L_bucket_id == g_node->L_bucket_id && lockedNodes->at(i)->G_key == g_node->G_key)
		{
			return TRUE;
		} else if(lockedNodes->at(i)->L_bucket_id == g_node->L_bucket_id && lockedNodes->at(i)->G_key > g_node->G_key)
		{
			lockedNodes->insert(lockedNodes->begin()+i,g_node);
			return FALSE;
		}
	}
	//If node is not found to be locked then add it into the list and return FALSE.
	lockedNodes->push_back(g_node);
	return FALSE;
}
/*
 * Method to delete the preds and currs locked by the transaction 
 * from the locked list.
 * */
void removePredsCurrsFromLockList(vector<G_node*> *lockedNodes, G_node** G_preds, G_node** G_currs)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		//cout<<"Release unlock -> "<<lockedNodes->at(i)->G_key<<endl;
		if(lockedNodes->at(i)->L_bucket_id == G_preds[0]->L_bucket_id && lockedNodes->at(i)->G_key == G_preds[0]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//Unlock the G_node.
			//cout<<"UnLock "<<G_preds[0]->G_key<<endl;
			G_preds[0]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->L_bucket_id == G_preds[1]->L_bucket_id && lockedNodes->at(i)->G_key == G_preds[1]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//Unlock the G_node.
			//cout<<"UnLock "<<G_preds[1]->G_key<<endl;
			G_preds[1]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->L_bucket_id == G_currs[0]->L_bucket_id && lockedNodes->at(i)->G_key == G_currs[0]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//cout<<"UnLock "<<G_currs[0]->G_key<<endl;
			G_currs[0]->lmutex.unlock();
			i--;
			continue;
		}
		if(lockedNodes->at(i)->L_bucket_id == G_currs[1]->L_bucket_id && lockedNodes->at(i)->G_key == G_currs[1]->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			//cout<<"UnLock "<<G_currs[1]->G_key<<endl;
			G_currs[1]->lmutex.unlock();
			i--;
			continue;
		}
	}
}

/*
 * Method to delete the preds and currs locked by the transaction 
 * from the locked list.
 * */
void removePredsCurrs(vector<G_node*> *lockedNodes, G_node* g_node)
{
	//Find the node is locked or not ?
	for(int i=0;i<lockedNodes->size();i++)
	{
		if(lockedNodes->at(i)->L_bucket_id == g_node->L_bucket_id && lockedNodes->at(i)->G_key == g_node->G_key)
		{
			//If yes then erase it from the locked list.
			lockedNodes->erase(lockedNodes->begin()+i);
			return;
		}
	}
}

/*
 * Function required to calculate the smaller and larger versions from the allRVL list.
 * */
void getLarAndSm(int G_wts, vector<L_txlog*> *prevVer_rvl, list<L_txlog*> *largeRVL, list<L_txlog*> *smallRVL)
{
	/*Iterator objects.*/
	L_txlog *tranx_iterator;
    vector<L_txlog*>::iterator list_iterator;
    
    list_iterator = prevVer_rvl->begin();
	while(list_iterator != prevVer_rvl->end())
	{
		tranx_iterator = *list_iterator;
		if(G_wts<tranx_iterator->L_wts)
		{
			largeRVL->push_back(tranx_iterator);
		} else {
			smallRVL->push_back(tranx_iterator);
		}		
		list_iterator++;
	}
}

