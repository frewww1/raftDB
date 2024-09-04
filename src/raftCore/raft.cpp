#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

//总结：follower回应心跳以及日志的方法
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);

  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的

  // 检查 term，以确保处理的请求来自有效的领导者
  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  // 注意从过期的领导人收到消息不要重设定时器
  }

   DEFER { persist(); };  //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.

  // 如果收到的 term 大于当前 term，更新 term 并降级
  if (args->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
  }
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));

  m_status = Follower;  // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower

  /// 从这里开始term相等
  m_lastResetElectionTime = now();  // 重置选举超时定时器

  ///	比较日志，日志有3种情况
  //1.对方发来的日志索引比我最后日志索引要大，就拒绝，并告诉他我最大索引
  if (args->prevlogindex() > getLastLogIndex()) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);  // 返回日志最新索引
       return;
  }
  //2.如果发来的日志比我的快照还小，也是告诉他我的最大索引
  else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {
    // 如果prevlogIndex还没有更上快照
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1);
    return;
  }

  //3.发来的日志是我需要的，如果匹配了，就说明之前的日志一定是一致的
   if (matchLog(args->prevlogindex(), args->prevlogterm())) {
      //更新日志
      for (int i = 0; i < args->entries_size(); i++) {
        auto log = args->entries(i);
        if (log.logindex() > getLastLogIndex()) {
          //超过就直接添加日志
          m_logs.push_back(log);
        } else {
          // 更新现有日志条目
          if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
              m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {

            //相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
            myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                   " {%d:%d}却不同！！\n",
                                   m_me, log.logindex(), log.logterm(), m_me,
                                   m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                   log.command()));
          }
          // 更新提交索引
          if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
            //不匹配就更新
            m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
          }
        }
      }
      myAssert(
          getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
          format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
                 m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
      if (args->leadercommit() > m_commitIndex) {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
      //对的，因为leader会先发送日志，再提交
  }
  myAssert(getLastLogIndex() >= m_commitIndex,
           format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                  getLastLogIndex(), m_commitIndex));
  reply->set_success(true);
  reply->set_term(m_currentTerm);
  return;
  } else {// 4.日志不匹配，就往前匹配
    reply->set_updatenextindex(args->prevlogindex());
    //从不匹配的地方往前找到前一个term
    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }

}

//总结：这是一个触发器，获取所有要应用的日志，丢到chan里面
void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
              m_commitIndex);
    }

    // 获取需要应用的日志条目
    auto applyMsgs = getApplyLogs();

    m_mtx.unlock();
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver報告的applyMsgs長度爲：{%d}", m_me, applyMsgs.size());
    }
    // 将日志条目推送到kv通道中
    for (auto& message : applyMsgs) {
      applyChan->Push(message);
    }

    // 睡眠一段时间，等待下一次检查，确保定时器的时间间隔
    sleepNMilliseconds(ApplyInterval);
  }
}

//空
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  return true;
  //// Your code here (2D).
  // rf.mu.Lock()
  // defer rf.mu.Unlock()
  // DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex {%v} to check
  // whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)
  //// outdated snapshot
  // if lastIncludedIndex <= rf.commitIndex {
  //	return false
  // }
  //
  // lastLogIndex, _ := rf.getLastLogIndexAndTerm()
  // if lastIncludedIndex > lastLogIndex {
  //	rf.logs = make([]LogEntry, 0)
  // } else {
  //	rf.logs = rf.logs[rf.getSlicesIndexFromLogIndex(lastIncludedIndex)+1:]
  // }
  //// update dummy entry with lastIncludedTerm and lastIncludedIndex
  // rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
  //
  // rf.persister.Save(rf.persistData(), snapshot)
  // return true
}

//总结：给自己投票，并发给每个节点
void Raft::doElection() {
  std::lock_guard<std::mutex> g(m_mtx);

  //能走到这的只能是非leader了
  if (m_status != Leader) {
    DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);

    ///开始新一轮的选举
    m_status = Candidate;    // 更新节点状态为 Candidate，准备开始新一轮的选举
    m_currentTerm += 1;  // 增加当前的任期
    m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同辈的candidate投
    persist();   // 将当前的状态持久化到磁盘
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 初始化一个用于存储票数的智能指针
    m_lastResetElectionTime = now(); //	重新设置定时器

    //	发布RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

      //设置rpc请求参数
      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 开一个线程去发送该rpc
      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);
      t.detach();
    }
  }
}

//总结：leader发送心跳，会带上日志
void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    auto appendNums = std::make_shared<int>(1);  //记录返回响应的节点数量

    //对Follower（除了自己外的所有节点发送AE）
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));

      //如果对方节点期望的索引已经被快照了，就直接发快照
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) {
        std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程,发送快照
        t.detach();
        continue;
      }

      //构造RPC请求
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm); //获取该follower的前一个日志索引和任期
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);

      //添加需要发送的日志条目
      if (preLogIndex != m_lastSnapshotIncludeIndex) {  //正常发送期望索引之后所有日志
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];  //"="是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      } else {  //发送快照之后所有的日常
        for (const auto& item : m_logs) {
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;
        }
      }

      int lastLogIndex = getLastLogIndex();  //获取最后一个日志条目的索引

      // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));

      //执行RPC请求
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                    appendNums);  // 使用新线程发送日志给follower
      t.detach();
    }
    m_lastResetHearBeatTime = now();  // 更新最后发送心跳时间
  }
}

//总结:这是定时触发器，满足条件就选举
void Raft::electionTimeOutTicker() {

  while (true) {
     //leader不需要执行这个，睡眠就行
    while (m_status == Leader) {
      usleep(
          HeartBeatTimeout);  //定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
    }

    //获取当前时间点
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }
    //睡一觉
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                << std::endl;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0) {
      //说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    }

    //以上只是为了保证到时间再执行该方法
    doElection();
  }
}

//总结：根据提交索引和应用索引，获取所有未应用的日志
std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;// 更新已应用的日志索引
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);

  }
  return applyMsgs;
}

// 总结：获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}


// 总结：字面意思    leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {

  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    //要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

//总结：获取raft状态
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    // todo 暂时不清楚会不会导致死锁
    m_mtx.unlock();
  };
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

//加载快照
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.term{%v}<rf{%v}.term{%v} ", rf.me, args.LeaderId,
    //        args.Term, rf.me, rf.currentTerm)

    return;
  }
  if (args->term() > m_currentTerm) {
    //后面两种情况都要接收日志
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  m_status = Follower;
  m_lastResetElectionTime = now();
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex) {
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.LastSnapShotIncludeIndex{%v} <=
    //        rf{%v}.lastSnapshotIncludeIndex{%v} ", rf.me, args.LeaderId, args.LastSnapShotIncludeIndex, rf.me,
    //        rf.lastSnapshotIncludeIndex)
    return;
  }
  //截断日志，修改commitIndex和lastApplied
  //截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
  //但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex()) {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  } else {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  std::thread t(&Raft::pushMsgToKvServer, this, msg);  // 创建新线程并执行b函数，并传递参数
  t.detach();
  //看下这里能不能再优化
  //    DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
  //    args.LeaderId, args.LastSnapShotIncludeIndex)
  //持久化
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }


//总结：这是一个触发器，用来定时发送心跳
void Raft::leaderHearBeatTicker() {
  while (true) {
    //不是leader的话就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
    while (m_status != Leader) {
      usleep(1000 * HeartBeatTimeout);
    }

    static std::atomic<int32_t> atomicCount = 0;  //原子计数

    //计算一个睡眠时间
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }

    //可以睡眠的话，就让该协程睡眠
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << atomicCount << "\033[1;35m leaderHearBeatTicker();函数实际睡眠时间为: " << duration.count()
                << " 毫秒\033[0m" << std::endl;
      ++atomicCount;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0) {
      //睡眠的这段时间有重置定时器，没有超时，再次睡眠
      continue;
    }
    ///以上就是保证到时候再执行发送心跳
    doHeartBeat();
  }
}


void Raft::leaderSendSnapShot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  //中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    //三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}


void Raft::leaderUpdateCommitIndex() {
  m_commitIndex = m_lastSnapshotIncludeIndex;
  // for index := rf.commitIndex+1;index < len(rf.log);index++ {
  // for index := rf.getLastIndex();index>=rf.commitIndex+1;index--{
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
    // log.Printf("lastSSP:%d, index: %d, commitIndex: %d, lastIndex: %d",rf.lastSSPointIndex, index, rf.commitIndex,
    // rf.getLastIndex())
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
  //    DPrintf("[func-leaderUpdateCommitIndex()-rf{%v}] Leader %d(term%d) commitIndex
  //    %d",rf.me,rf.me,rf.currentTerm,rf.commitIndex)
}

//总结：判断index对应的任期跟我给你的是否一致
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);

}

//总结：raft持久化
void Raft::persist() {
  // Your code here (2C).
  auto data = persistData();
  m_persister->SaveRaftState(data);
  // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
  // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
}

//总结：回应选举RPC，判断是否授予
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);

  DEFER {
    persist();
  };

  // 1. 如果请求中的任期小于当前节点的任期，拒绝投票
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm); // 返回当前节点的任期
    reply->set_votestate(Expire);  // 表示请求过期
    reply->set_votegranted(false);  // 不授予选票
    return;
  }

  // 2.对方的term更大，此时要变成follower
  if (args->term() > m_currentTerm) {
    m_status = Follower;  // 更改状态为 Follower
    m_currentTerm = args->term(); // 更新当前任期
    m_votedFor = -1;   // 重置投票状态
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));

  ///到这里，任期就一致了，下面需要判断索引了

  int lastLogTerm = getLastLogTerm();

  // 如果候选者的日志不够新，不授予选票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  }

  //如果我已经投过票，并且不是投给对方的，也要拒绝
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  } else {
    m_votedFor = args->candidateid();
    //投出票的时候才重置定时器
    m_lastResetElectionTime = now();
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    return;
  }
}

//总结：判断该日志是否比现在的要新
bool Raft::UpToDate(int index, int term) {
  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

//总结：如名字
void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
//总结：如名字
int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

//总结：如名字
int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
//总结：如名字
int Raft::getLogTermFromLogIndex(int logIndex) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}
//总结：如名字
int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

//总结：找到index对应的真实下标
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  //获取当前最后一个日志条目的索引
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

//总结：发送选举rpc，并判断是否选举成功
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {

  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 開始", m_me, m_currentTerm, getLastLogIndex());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 完畢，耗時:{%d} ms", m_me, m_currentTerm,
          getLastLogIndex(), now() - start);

  if (!ok) {
    return false;
  }
  ///理论上，1.对方的任期比自己高，就会变follower
  ///2.根据是否投票的标识决定是否获取选票，具体的逻辑交给对方判断

  //对回应进行处理，要记得无论什么时候收到回复就要检查term
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) { //1.对方的term更大，此时要变成follower
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  } else if (reply->term() < m_currentTerm) {//2.对方的term更小（理论上不会出现，因为对面会保证更新自己的term），无事发生
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));

  if (!reply->votegranted()) { //3.对方没给你投票，无事发生
    return true;
  }

  //能到这就说明获得一个投票了
  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    //变成leader
    *votedNum = 0;
    if (m_status == Leader) {//理论上leader不会到这里
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }

    //	第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;  //有效下标从1开始，因此要+1
      m_matchIndex[i] = 0;                //每换一个领导都是从0开始，见fig2
    }

    std::thread t(&Raft::doHeartBeat, this);  //马上向其他节点宣告自己就是leader
    t.detach();

    persist();
  }
  return true;
}

//总结：发送日志和心跳RPC，获取回应后还需要对齐日志索引,大多数同意就提交日志
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {

  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc開始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());

  //调用RPC util发送
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);

  //如果未发送成功，直接返回
  if (reply->appstate() == Disconnected) {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 检查返回的 term，如果对方的term比自己大，就要降级
  if (reply->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  } else if (reply->term() < m_currentTerm) {//逻辑由对方处理，这个情况不会发生
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader) {
    //如果不是leader，那么就不要对返回的情况进行处理了(都走到这了还能不是leader?)
    return ok;
  }
  /// 到这里term一定相等

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));

  //日志匹配失败，处理回退，跟新nextIndex
  if (!reply->success()) {
       if (reply->updatenextindex() != -100) {
        DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  //失败是不更新mathIndex的
    }
  } else {//日志匹配成功
    //成功，+1
    *appendNums = *appendNums + 1;
    DPrintf("---------------------------tmp------------------------- 節點{%d}返回true,當前*appendNums{%d}", server,
            *appendNums);

    //更新follower的信息
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;

    int lastLogIndex = getLastLogIndex();
    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));

    //如果大多数节点都接受了日志，更新commitIndex
    if (*appendNums >= 1 + m_peers.size() / 2) {
      *appendNums = 0;
      if (args->entries_size() > 0) {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf(
            "---------------------------tmp------------------------- 當前term有log成功提交，更新leader的m_commitIndex "
            "from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());

        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                      m_commitIndex));
    }
  }
  return ok;
}


///RPC
void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  RequestVote(request, response);
  done->Run();
}
///RPC

//总结：处理上层来的命令，并记录该日志，返回新日志的索引和任期
void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 不是 Leader，就不能保存日志
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;  // 设置新日志索引为 -1，表示失败
    *newLogTerm = -1;  // 设置新日志任期为 -1，表示失败
    *isLeader = false;  // 设置 isLeader 为 false，表示当前不是 Leader
    return;
  }

  // 创建新的日志条目
  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());  // 设置命令
  newLogEntry.set_logterm(m_currentTerm);   // 设置当前任期
  newLogEntry.set_logindex(getNewCommandIndex());  // 设置新的日志索引
  m_logs.emplace_back(newLogEntry);   // 将新的日志条目添加到日志中

  int lastLogIndex = getLastLogIndex();  // 获取最新的日志索引

  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  persist();
  *newLogIndex = newLogEntry.logindex();   // 设置新日志的索引
  *newLogTerm = newLogEntry.logterm();   // 设置新日志的任期
  *isLeader = true;   // 设置 isLeader 为 true，表示当前是 Leader
}


//总结：初始化raft后添加三个触发器
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
  ///设置raft状态
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  this->applyChan = applyCh;
  m_currentTerm = 0;  //当前任期
  m_status = Follower;   //当前状态
  m_commitIndex = 0;   //提交索引
  m_lastApplied = 0;   //最后应用的日志索引
  m_logs.clear();    //清空日志
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);    // 初始化 matchIndex
    m_nextIndex.push_back(0);      // 初始化 nextIndex
  }
  m_votedFor = -1;    // 初始化投票目标
  m_lastSnapshotIncludeIndex = 0;     // 初始化快照包含的最后索引
  m_lastSnapshotIncludeTerm = 0;     // 初始化快照包含的最后任期
  m_lastResetElectionTime = now();    // 初始化最后重置选举时间
  m_lastResetHearBeatTime = now();    // 初始化最后重置心跳时间
  readPersist(m_persister->ReadRaftState()); // 从持久化存储中读取先前保存的状态
  if (m_lastSnapshotIncludeIndex > 0) {
    m_lastApplied = m_lastSnapshotIncludeIndex;  //设置成快照的最后索引
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  //创建一个io管理器
  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // 启动三个循环定时器
  // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
  m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });
  m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); });

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();
}

//总结：序列化raft的状态，用于持久化
std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

//总结：读取持久化的数据
void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

//总结：生成快照
void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  // 检查是否可以替换快照
  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  //为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  //制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index; // 更新快照包含的最后日志索引
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm(); // 更新快照包含的最后日志任期
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;   // 存储快照之后的日志
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    //注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }

  // 更新快照信息
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;

  // 替换日志条目，只保留快照之后的日志
  m_logs = trunckedLogs;
  // 更新提交索引和应用索引
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}