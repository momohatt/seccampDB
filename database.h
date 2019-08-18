#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <condition_variable>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#include "utils.h"
using namespace std;

class Transaction;
class Scheduler;
class DataBase;

enum ChangeMode {
    New,    // set
    Delete  // delete
};

enum BaseOp {
    Read,
    Write,
};

using Key = string;
using DBDiff = map<Key, pair<ChangeMode, int>>;

class Transaction {
    public:
        using Logic = function<void(Transaction*)>;

        Transaction(int id, Logic logic, DataBase* db, Scheduler* scheduler);

        void begin();
        void commit();
        void abort();

        bool set(Key key, int val);  // insert & update
        optional<int> get(Key key);  // read
        bool del(Key key);           // delete
        // Returns a set of all the existing key names.
        // keys() does *not* support reader/writer lock.
        vector<string> keys();
        // Repeat 'get' until it succeeds (e.g. the return value is not nullopt)
        // and returns the content of the value.
        int get_until_success(Key key);

        void set_thread(thread&& th) { thread_ = move(th); }

        // schedulerが起こすときに呼ぶ
        void notify() { turn_ = true; cv_.notify_one(); }
        void terminate() { thread_.join(); }

        DBDiff write_set = {};
        vector<Key> lock_set = {};  // lockをもっているkeyの集合
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
        int id_;
        vector<Key> write_log_ = {};
        unique_lock<mutex> lock_;
        condition_variable cv_;
        thread thread_;
        DataBase* db_;
        Scheduler* scheduler_;
};

class Scheduler {
    public:
        iterable_queue<unique_ptr<Transaction>> transactions;

        ~Scheduler();

        void add_tx(Transaction::Logic logic);
        void set_db(DataBase* db) { db_ = db; }

        // starts spawning threads
        void start();

        void notify() { turn_ = true; cv_.notify_one(); }

        void log(int id, Key key, BaseOp rw) {
            io_log_.emplace_back(id, key, rw);
        }

    private:
        struct Log {
            int id;
            Key key;
            BaseOp op;

            Log(int id, Key key, BaseOp op);
        };

        // Runs round-robin schedule
        void run();

        void wait(Transaction* tx);
        bool turn_ = false;
        condition_variable cv_;
        unique_lock<mutex> lock_;
        vector<Log> io_log_ = {};
        DataBase* db_;
};

class DataBase {
    public:
        struct RecordInfo {
            // TODO: allow other types (string, char, ...)
            int value;

            // Reader/Writer lock
            // 0  -> no lock
            // -1 -> write lock
            // n > 0 -> read lock (by n threads)
            int nlock = 0;
        };

        DataBase(Scheduler* scheduler, string dumpfilename, string logfilename);
        ~DataBase();

        unique_ptr<Transaction> generate_tx(Transaction::Logic logic);
        bool get_lock(Transaction* tx, Key key, BaseOp locktype);

        void apply_tx(Transaction* tx);

        // TODO: impl B+-tree (future work)
        map<Key, RecordInfo> table = {};

    private:
        // Persistence
        void recover();

        string serialize(DBDiff diff);
        void deserialize(DBDiff& diff, vector<string> buf);
        string make_log_format(ChangeMode mode, Key key, int value);

        Scheduler* scheduler_;
        const string dumpfilename_;
        const string logfilename_;
        int fd_log_;
        int id_counter_ = 0;  // for transaction ID

        // format of output files:
        // DB file:  [key] [value]
        // log file: [checksum] [key] [0/1] [value] (valueはDeleteの場合0)
};

class ConflictGraph {
    public:
        ConflictGraph();

    private:
        vector<int> node_;
        vector<pair<int, int>> edge_;
};

#endif  // __DATABASE_H__
