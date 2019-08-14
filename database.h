#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>
using namespace std;

class DataBase;
class Scheduler;

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

        void commit();

        // Basic operations
        //
        // return values:
        //     true -> Error
        //     false -> OK
        bool set(Key key, int val);
        optional<int> get(Key key);
        bool del(Key key);
        vector<string> keys();

        void set_db(DataBase* db) { db_ = db; }
        void set_scheduler(Scheduler* scheduler) { scheduler_ = scheduler; }

        map<Key, pair<ChangeMode, int>> write_set = {};
        bool is_done = false;

        DataBase* db_;

    private:
        // returns if the database or the write set has the specified key
        bool has_key(Key key);
        Scheduler* scheduler_;

};

class Scheduler {
    public:
        vector<thread> threads;
        vector<Transaction*> transactions;

        void add_tx(thread th, Transaction* tx);

        void run();

        bool turn = false;
};

class DataBase {
    public:
        using Key = string;

        DataBase(Scheduler* scheduler, string dumpfilename, string logfilename);
        ~DataBase();

        Transaction* generate_tx();

        void commit(Transaction* tx);

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
