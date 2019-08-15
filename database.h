#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <condition_variable>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "utils.h"
using namespace std;

class Transaction;
class Scheduler;
class DataBase;

// * 1つのthreadに1つのtxが対応
// * schedulerは複数のtxを管理して適切にスケジュール, yieldする
// * databaseはthreadを適切にjoinする

enum ChangeMode {
    New,    // set
    Delete  // delete
};

class Transaction {
    public:
        using Key = string;
        using Logic = function<void(Transaction*)>;

        Transaction(Logic logic, DataBase* db, Scheduler* scheduler);

        void Begin();
        void Commit();
        void Abort();

        bool Set(Key key, int val);  // insert & update
        optional<int> Get(Key key);  // read
        bool Del(Key key);           // delete
        vector<string> Keys();

        void set_thread(thread&& th) { thread_ = move(th); }

        // schedulerが起こすときに呼ぶ
        void Notify() { turn = true; cv_.notify_one(); }
        // 処理をschedulerに渡す
        void Wait();
        void Terminate() { thread_.join(); }

        map<Key, pair<ChangeMode, int>> write_set = {};
        bool is_done = false;
        bool turn = false;
        Logic logic;

    private:
        // returns if |db_| or |write_set| has the specified key
        bool has_key(Key key);

        unique_lock<mutex> lock_;
        condition_variable cv_;
        thread thread_;
        DataBase* db_;
        Scheduler* scheduler_;

};

class Scheduler {
    public:
        // TODO: これをunique_ptrにする
        iterable_queue<Transaction*> transactions;

        void add_tx(Transaction::Logic logic);
        void set_db(DataBase* db) { db_ = db; }

        // starts spawning threads
        void Start();

        // Runs round-robin schedule
        void Run();

        // |OnTxFinish| gets called every time a transaction is finished
        void OnTxFinish();

        bool turn = false;
        condition_variable cond_tx_done_;

    private:
        DataBase* db_;
};

class DataBase {
    public:
        using Key = string;

        DataBase(Scheduler* scheduler, string dumpfilename, string logfilename);
        ~DataBase();

        Transaction* generate_tx(Transaction::Logic logic);

        void apply_tx(Transaction* tx);

        // TODO: allow other types (string, char, ...)
        // TODO: impl B+-tree (future work)
        map<Key, int> table = {};

    private:
        // Persistence
        void recover();

        string make_log_format(ChangeMode mode, Key key, int value);

        Scheduler* scheduler_;
        string dumpfilename_;
        string logfilename_;
        int fd_log_;

        // format of output files:
        // DB file:  [key] [value]
        // log file: [checksum] [key] [0/1] [value]
};

#endif  // __DATABASE_H__
