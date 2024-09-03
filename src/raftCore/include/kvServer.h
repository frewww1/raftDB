//
// Created by swx on 23-6-1.
//

#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"

class KvServer : raftKVRpcProctoc::kvServerRpc {
 private:
  //互斥锁
  std::mutex m_mtx;
  //节点id
  int m_me;
  //指向Raft的共享指针
  std::shared_ptr<Raft> m_raftNode;
  //指向LockQueue<ApplyMsg>的共享指针
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // kvServer和raft节点的通信管道
  //Raft状态最大的大小
  int m_maxRaftState;                               // snapshot if log grows this big

  // 存放序列化后的数据
  std::string m_serializedKVData;  // todo ： 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
  //跳表?
  SkipList<std::string, std::string> m_skipList;
  //哈希表，kv数据库
  std::unordered_map<std::string, std::string> m_kvDB;
  //哈希表?
  std::unordered_map<int, LockQueue<Op> *> waitApplyCh;
  // index(raft) -> chan  //？？？字段含义   waitApplyCh是一个map，键是int，值是Op类型的管道

  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

  // 最后一个快照点
  int m_lastSnapShotRaftLogIndex;

 public:
  KvServer() = delete;

  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

  void StartKVServer();
  //打印数据库详情
  void DprintfKVDB();
  //存入数据
  void ExecuteAppendOpOnKVDB(Op op);
  //获取数据
  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);
  //存入数据
  void ExecutePutOpOnKVDB(Op op);
  //获取
  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply
               *reply);  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
  /**
   * 從raft節點中獲取消息  （不要誤以爲是執行【GET】命令）
   * @param message
   */
  void GetCommandFromRaft(ApplyMsg message);

  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // 存入
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  ////一直等待raft传来的applyCh
  void ReadRaftApplyCommandLoop();

  void ReadSnapShotToInstall(std::string snapshot);

  bool SendMessageToWaitChan(const Op &op, int raftIndex);

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  // Handler the SnapShot from kv.rf.applyCh
  void GetSnapShotFromRaft(ApplyMsg message);

  std::string MakeSnapShot();

 public:  // for rpc
  void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

  void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

  /////////////////serialiazation start ///////////////////////////////
  // notice ： func serialize
 private:
  friend class boost::serialization::access;

  // When the class Archive corresponds to an output archive, the
  // & operator is defined similar to <<.  Likewise, when the class Archive
  // is a type of input archive the & operator is defined similar to >>.
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)  //这里面写需要序列话和反序列化的字段
  {
    ar &m_serializedKVData;

    // ar & m_kvDB;
    ar &m_lastRequestId;
  }

  std::string getSnapshotData() {
    //从跳表取数据
    m_serializedKVData = m_skipList.dump_file();
    //将对象数据序列化到字符串流中
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  void parseFromString(const std::string &str) {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
  }

  /////////////////serialiazation end ///////////////////////////////
};

#endif  // SKIP_LIST_ON_RAFT_KVSERVER_H
