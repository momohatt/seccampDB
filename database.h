#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <map>
#include <optional>
#include <string>
#include <vector>
using namespace std;

class DataBase {
    public:
        using Key = string;

        DataBase(string dumpfilename, string logfilename);
        ~DataBase();

        // Transaction logics
        void begin();
        void commit();
        void abort();

        // Basic operations
        //
        // return values:
        //     true -> Error
        //     false -> OK
        bool set(Key key, int val);
        optional<int> get(Key key);
        bool del(Key key);
        vector<string> keys();

    private:
        enum ChangeMode {
            New,    // set
            Delete  // delete
        };

        // Persistence
        void recover();

        // returns if the database *or the write set* has the specified key
        bool has_key(Key key);

        // log single (non-transaction) commands to log file
        void log_non_transaction(ChangeMode mode, Key key, int val);

        string make_log_format(ChangeMode mode, Key key, int val);

        // TODO: allow other types (string, char, ...)
        // TODO: impl B+-tree (future work)
        map<Key, int> table_ = {};
        map<Key, pair<ChangeMode, int>> write_set_ = {};

        bool transaction_mode_ = false;
        string dumpfilename_;
        string logfilename_;
        int fd_log_;

        // format of output files:
        // DB file:  [key] [value]
        // log file: [checksum] [key] [0/1] [value]
};

#endif  // __DATABASE_H__
