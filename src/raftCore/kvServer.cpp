#include "kvServer.h"

#include <rpcprovider.h>

#include "mprpcconfig.h"

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER {
    // for (const auto &item: m_kvDB) {
    //     DPrintf("[DBInfo ----]Key : %s, Value : %s", &item.first, &item.second);
    // }
    m_skipList.display_list();
  };
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  // if op.IfDuplicate {   //get请求是可重复执行的，因此可以不用判复
  //	return
  // }
  m_mtx.lock();

  m_skipList.insert_set_element(op.Key, op.Value);

  // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
  //     m_kvDB[op.Key] = m_kvDB[op.Key] + op.Value;
  // } else {
  //     m_kvDB.insert(std::make_pair(op.Key, op.Value));
  // }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  //    DPrintf("[KVServerExeAPPEND-----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  m_mtx.lock();
  *value = "";
  *exist = false;
  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
    // *value = m_skipList.se //value已经完成赋值了
  }
  // if (m_kvDB.find(op.Key) != m_kvDB.end()) {
  //     *exist = true;
  //     *value = m_kvDB[op.Key];
  // }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  if (*exist) {
    //                DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.ClientId,
    //                op.RequestId, op.Key, value)
  } else {
    //        DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.ClientId,
    //        op.RequestId, op.Key)
  }
  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  // m_kvDB[op.Key] = op.Value;
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  //    DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.ClientId, op.RequestId,
  //    op.Key, op.Value)
  DprintfKVDB();
}

// 处理来自clerk的Get RPC
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  // 将 RPC 的请求参数组装成一个操作（Op）对象，这个对象用于记录此次操作的具体信息。
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  // 将操作提交给 Raft 节点（m_raftNode），Start 方法返回的是 raftIndex，它是 Raft 预计的日志索引。
  m_raftNode->Start(op, &raftIndex, &_,
                    &isLeader);

  if (!isLeader) {// 如果当前节点不是 Raft 集群的 Leader，返回错误提示客户端尝试其他节点。
    reply->set_err(ErrWrongLeader);
    return;
  }

  // 创建等待队列，用于等待操作的应用结果。
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock();


  Op raftCommitOp;
  // 如果超时没有从队列中获取到操作结果，检查当前节点是否还是 Leader。
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    // 检查请求是否已经被提交过，如果是且当前节点仍为 Leader，则可以直接执行操作。
    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
      std::string value;
      bool exist = false;

      // 在本地键值数据库上执行 Get 操作。
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);  // 返回值给客户端。
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");   // 键不存在，返回错误。
      }
    } else {// 如果请求没有提交或当前节点不是 Leader，提示客户端换节点重试。
      reply->set_err(ErrWrongLeader);
    }
  } else {
    // 如果获取到了 Raft 集群提交的操作结果。
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);// 在本地键值数据库上执行 Get 操作。
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }
  m_mtx.lock();
  // 清理等待队列，删除已完成的任务的队列。
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
    // execute command
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }
    //  kv.lastRequestId[op.ClientId] = op.RequestId  在Executexxx函数里面更新的
  }
  //到这里kvDB已经制作了快照
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
    // todo :不存在这个client就创建
  }
  return RequestId <= m_lastRequestId[ClientId];
}


void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];

  m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;

  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
    } else {
      reply->set_err(ErrWrongLeader);  ///这里返回这个的目的让clerk重新尝试
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      //可能发生leader的变更导致日志被覆盖，因此必须检查
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  m_mtx.lock();

  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

//一个循环负责消费chan里的消息
void KvServer::ReadRaftApplyCommandLoop() {
  //负责消费通道的信息
  while (true) {
    //如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();  //阻塞弹出
    DPrintf(
        "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // listen to every command applied by its raft ,delivery to relative RPC Handler

    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}


void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {

    return;
  }
  parseFromString(snapshot);
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    return false;
  }
  waitApplyCh[raftIndex]->Push(op);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  return true;
}

void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {
    // Send SnapShot Command
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot);
  }
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string KvServer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
  KvServer::PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
  KvServer::Get(request, response);
  done->Run();
}

KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6) {
  //Persister对象
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

  m_me = me;
  m_maxRaftState = maxraftstate;

  applyChan = std::make_shared<LockQueue<ApplyMsg> >();

  m_raftNode = std::make_shared<Raft>();
  ////////////////clerk层面 kvserver开启rpc接受功能
  //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
  std::thread t([this, port]() -> void {
    // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(
        this->m_raftNode.get());  // todo：这里获取了原始指针，后面检查一下有没有泄露的问题 或者 shareptr释放的问题
    // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
    provider.Run(m_me, port);
  });
  t.detach();

  ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
  ////这里使用睡眠来保证
  std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
  sleep(6);
  std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
  //获取所有raft节点ip、port ，并进行连接  ,要排除自己
  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str());
  std::vector<std::pair<std::string, short> > ipPortVt;
  for (int i = 0; i < INT_MAX - 1; ++i) {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty()) {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str()));  //沒有atos方法，可以考慮自己实现
  }
  std::vector<std::shared_ptr<RaftRpcUtil> > servers;
  //进行连接
  for (int i = 0; i < ipPortVt.size(); ++i) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;
    //维护一个rpc连接
    auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

    std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
  }
  sleep(ipPortVt.size() - me);  //等待所有节点相互连接成功，再启动raft
  m_raftNode->init(servers, m_me, persister, applyChan);
  // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

  //////////////////////////////////

  // You may need initialization code here.
  // m_kvDB; //kvdb初始化
  m_skipList;
  waitApplyCh;
  m_lastRequestId;
  m_lastSnapShotRaftLogIndex = 0;  // todo:感覺這個函數沒什麼用，不如直接調用raft節點中的snapshot值？？？
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this);  //马上向其他节点宣告自己就是leader
  t2.join();  //由於ReadRaftApplyCommandLoop一直不會結束，达到一直卡在这的目的
}
