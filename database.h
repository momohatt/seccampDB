#ifndef __DATABASE_H__
#define __DATABASE_H__

#include <map>
#include <optional>
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
        int insert(Key key, int val);
        int update(Key key, int val);
        optional<int> read(Key key);
        int del(Key key);

    private:
        // Persistence
        void recover(string logfilename);

        // returns if the database *or the write set* has the specified key
        bool has_key(Key key);

        // TODO: allow other types (string, char, ...)
        // TODO: impl B+-tree (future work)
        map<Key, int> table_ = {};

        enum ChangeMode {
            New,    // insert or update
            Delete  // delete
        };
        map<Key, pair<ChangeMode, int>> write_set_ = {};

        bool transaction_mode_ = false;
        string dumpfilename_;
        string logfilename_;
        ofstream ofs_log_;
};

#endif  // __DATABASE_H__
