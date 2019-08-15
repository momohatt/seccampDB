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

enum ChangeMode {
    New,    // set
    Delete  // delete
};

class Transaction {
    public:
        using Key = string;
        using Logic = function<void(Transaction*)>;

        Transaction(Logic logic, DataBase* db, Scheduler* scheduler);

        void begin();
        void commit();
        void abort();

        bool set(Key key, int val);  // insert & update
        optional<int> get(Key key);  // read
        bool del(Key key);           // delete
        vector<string> keys();
        int get_until_success(Key key); // retry 'get' until succeess

        void set_thread(thread&& th) { thread_ = move(th); }

        // schedulerが起こすときに呼ぶ
        void notify() { turn_ = true; cv_.notify_one(); }
        void terminate() { thread_.join(); }

        map<Key, pair<ChangeMode, int>> write_set = {};
        vector<Key> read_set = {};  // 読んだkeyの集合
        bool is_done = false;
        Logic logic;

    private:
        // 処理をschedulerに渡してwait
        void wait();
        // 処理をschedulerに渡すがwaitしない
        void finish();

        // returns if |db_| or |write_set| has the specified key
        bool has_key(Key key);

        bool turn_ = false;
        unique_lock<mutex> lock_;
        condition_variable cv_;
        thread thread_;
        DataBase* db_;
        Scheduler* scheduler_;
};

class Scheduler {
    public:
        iterable_queue<unique_ptr<Transaction>> transactions;

        void add_tx(Transaction::Logic logic);
        void set_db(DataBase* db) { db_ = db; }

        // starts spawning threads
        void start();

        // Runs round-robin schedule
        void run();

        void notify() { turn_ = true; cv_.notify_one(); }

    private:
        void wait(Transaction* tx);
        bool turn_ = false;
        condition_variable cv_;
        unique_lock<mutex> lock_;
        DataBase* db_;
};

class DataBase {
    public:
        using Key = string;

        enum LockType {
            Read,
            Write,
        };

        struct RecordInfo {
            int value;

            // Reader/Writer lock
            // 0  -> no lock
            // -1 -> write lock
            // n > 0 -> read lock (by n threads)
            int nlock = 0;
            vector<Transaction*> txs = {};
        };

        DataBase(Scheduler* scheduler, string dumpfilename, string logfilename);
        ~DataBase();

        unique_ptr<Transaction> generate_tx(Transaction::Logic logic);
        bool get_lock(Transaction* tx, Key key, LockType locktype);

        void apply_tx(Transaction* tx);

        // TODO: allow other types (string, char, ...)
        // TODO: impl B+-tree (future work)
        map<Key, RecordInfo> table = {};

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
