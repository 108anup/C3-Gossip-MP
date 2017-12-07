
/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

int mleSize();

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
  if ( memberNode->bFailed ) {
    return false;
  }
  else {
    return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
  Address joinaddr;
  joinaddr = getJoinAddress();

  // Self booting routines
  if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if( !introduceSelfToGroup(&joinaddr) ) {
    finishUpThisNode();
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
    exit(1);
  }

  return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
  // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
  static char s[1024];
#endif

  if ( 0 == memcmp((char *)&(memberNode->addr.addr),
                   (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
    // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Starting up group...");
#endif

    addSelfToGroup();
  }
  else {
    size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr),
           &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif
    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
  /*
   * Your code goes here
   */
  return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
    return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if( !memberNode->inGroup ) {
    return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while ( !memberNode->mp1q.empty() ) {
    ptr = memberNode->mp1q.front().elt;
    size = memberNode->mp1q.front().size;
    memberNode->mp1q.pop();
    recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
	/*
	 * Your code goes here
	 */

  MessageHdr *msg_recv = (MessageHdr *) data;
  if (msg_recv->msgType == JOINREQ){
    MessageHdr *msg;
      
    //Add to MemberList
    MemberListEntry mle; char *itr = (data+sizeof(MessageHdr));
    Address memAddr; long heartbeat;
    memcpy (&memAddr.addr, (char *)(itr), sizeof(memAddr.addr));
    itr += sizeof(memAddr.addr);
    itr++;
    memcpy (&heartbeat, (char *)(itr), sizeof(long));
    itr += sizeof(long);

    mle.id = *(int*)(&memAddr.addr);
    mle.port = *(short*)(&memAddr.addr[4]);
    mle.heartbeat = heartbeat;
    mle.timestamp = par->getcurrtime();

    updateMember (mle);

    //JOINREP
    size_t listSize;
    char *ptr = serializeList (memberNode->memberList, &listSize);

    size_t msgsize = sizeof(MessageHdr) + listSize + sizeof(size_t);
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREP message 
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), ptr, (sizeof(size_t) + listSize)*sizeof(char));

    // send JOINREP message to the new member
    emulNet->ENsend(&memberNode->addr, &memAddr, (char *)msg, msgsize);

    free(msg);
    free(ptr);
  }
  else if (msg_recv->msgType == JOINREP){
    vector<MemberListEntry> ml = deserializeList (data + sizeof(MessageHdr));
    int numMembers = ml.size();

    addSelfToGroup();
    for (int i=0; i<numMembers; i++){
      updateMember (ml[i]);
    }
  }
  else if (msg_recv->msgType == HEARTBEAT && memberNode->inGroup){
    vector<MemberListEntry> ml = deserializeList (data + sizeof(MessageHdr));
    int numMembers = ml.size();

    for (int i=0; i<numMembers; i++){
      updateMember (ml[i]);
    }
  }

  return 1;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

  vector<MemberListEntry> &ml = memberNode->memberList;
  int current_time = par->getcurrtime();
  
  //Update Heartbeat
  memberNode->heartbeat++;
  int id = memberNode->addr.getid();
  short port = memberNode->addr.getport();

  //First member of list is self
  ml[0].setheartbeat(memberNode->heartbeat);
  ml[0].settimestamp(current_time);

  if (ml[0].getid() != id)
    printf ("Problem!\n");
  
  //Update Membership List
  for (int i = 0; i<ml.size(); i++){
    if (current_time - ml[i].gettimestamp() > TREMOVE){

      // printf ("removing: time: %d | host: id %d, port %d | guest: id %d, port %d "
      //         "hbt: %d, timestamp %d\n",
      //         par->getcurrtime(), id, port, ml[i].getid(),
      //         ml[i].getport(), ml[i].getheartbeat(), ml[i].gettimestamp());

#ifdef DEBUGLOG
      Address addr(ml[i].getid(), ml[i].getport());
      log->logNodeRemove(&memberNode->addr, &addr);
#endif
      ml.erase (ml.begin() + i);
      i--;
    }
  }

  // if (memberNode->heartbeat == 600){
  //   printf ("Membership list of node with id: %d, port: %d\n", id, port);
  //   for(int i = 0; i<ml.size(); i++){
  //     printf ("MLE: id: %d, port: %d, timestamp: %d, heartbeat: %d\n",
  //             ml[i].getid(), ml[i].getport(), ml[i].gettimestamp(), ml[i].getheartbeat());
  //   }
  // }
  

  //HEARTBEAT (Propagate)
  if (ml.size() > 1){
    size_t listSize;
    char *ptr = serializeList (ml, &listSize);

    size_t msgsize = sizeof(MessageHdr) + listSize + sizeof(size_t);
    MessageHdr* msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create 
    msg->msgType = HEARTBEAT;
    memcpy((char *)(msg+1), ptr, (sizeof(size_t) + listSize)*sizeof(char));

    // send to GOSSIPFANOUT randomly selected nodes
    for (int i = 1; i<ml.size(); i++){
      int member_to_send = i;//rand() % (ml.size()-1) + 1;
      Address addr(ml[member_to_send].getid(), ml[member_to_send].getport());
      emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
    }

    free(msg);
    free(ptr);
  }  
  return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
  Address joinaddr;

  memset(&joinaddr, 0, sizeof(Address));
  *(int *)(&joinaddr.addr) = 1;
  *(short *)(&joinaddr.addr[4]) = 0;

  return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
  printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
         addr->addr[3], *(short*)&addr->addr[4]) ;    
}

int mleSize(){
  return sizeof(int) + sizeof(short) + 2*sizeof(long);
}

char* MP1Node::serializeList (vector<MemberListEntry> &memberList, size_t *listSize) {
  int numMembers = memberList.size();
  *listSize = mleSize() * numMembers;
  char *ptr = (char *)malloc((*listSize + sizeof(size_t))* sizeof(char));
  char *itr = ptr; 

  memcpy((char *)(itr), listSize, sizeof(size_t));
  itr += sizeof(size_t);

  const long m1 = 0;
  for(int i= 0; i<numMembers; i++){
    memcpy(itr, &memberList[i].id, sizeof(int));
    itr += sizeof(int);
    memcpy(itr, &memberList[i].port, sizeof(short));
    itr += sizeof(short);
    if (par->getcurrtime() - memberList[i].gettimestamp() > TFAIL)
      memcpy(itr, &m1, sizeof(long));
    else
      memcpy(itr, &memberList[i].heartbeat, sizeof(long));
    itr += sizeof(long);
    memcpy(itr, &memberList[i].timestamp, sizeof(long));
    itr += sizeof(long);
  }
  return ptr;
}

vector<MemberListEntry> MP1Node::deserializeList (char *ptr){
  char *itr = ptr;
  size_t listSize;
  memcpy (&listSize, (char *)(itr), sizeof(size_t));
  itr += sizeof(size_t);

  int numMembers = listSize / mleSize();
  vector<MemberListEntry> ml(numMembers);

  for (int i=0; i<numMembers; i++){
    memcpy (&ml[i].id, itr, sizeof(int));
    itr += sizeof(int);
    memcpy (&ml[i].port, itr, sizeof(short));
    itr += sizeof(short);
    memcpy (&ml[i].heartbeat, itr, sizeof(long));
    itr += sizeof(long);
    memcpy (&ml[i].timestamp, itr, sizeof(long));
    itr += sizeof(long);
  }

  return ml;
} 

void MP1Node::updateMember (MemberListEntry mle){
  vector<MemberListEntry> &ml = memberNode->memberList;
  int id = mle.getid();
  short port = mle.getport();
  int current_time = par->getcurrtime();

  for(int i = 0; i<ml.size(); i++){
    if(ml[i].getid() == id && ml[i].getport() == port){
      if (ml[i].getheartbeat() < mle.getheartbeat())
      {
        ml[i].settimestamp(current_time);
        ml[i].setheartbeat(mle.getheartbeat());
      }
      return;
    }
  }

  if (mle.getheartbeat() != -1){
  mle.settimestamp(par->getcurrtime());
  ml.push_back(mle);

#ifdef DEBUGLOG
  Address addr(mle.id, mle.port);
  log->logNodeAdd(&memberNode->addr, &addr);
#endif
  }
}

void MP1Node::addSelfToGroup (){
  memberNode->inGroup = true;
  // Add itself to member list
  MemberListEntry mle(memberNode->addr.getid(), memberNode->addr.getport(),
                      memberNode->heartbeat, par->getcurrtime());
  memberNode->memberList.push_back(mle);
#ifdef DEBUGLOG
  log->logNodeAdd(&memberNode->addr, &memberNode->addr);
#endif
}
