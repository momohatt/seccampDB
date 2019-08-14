#include "database.h"
#include "utils.h"

#include <iostream>
#include <condition_variable>
#include <mutex>
#include <fstream>
#include <cstdlib>
#include <cassert>
#include <fcntl.h>  // open
#include <unistd.h>  // close
#include <stdio.h>
using namespace std;

#define LOG(x) cout << "[LOG](tid: " << this_thread::get_id() << ") " << x << endl

// ---------------------------- Transaction Logic ------------------------------

TransactionLogic::TransactionLogic(Func func)
  : func(func) {}

// -------------------------------- Transaction --------------------------------

mutex mtx_;
condition_variable cond_;

Transaction::Transaction(
        TransactionLogic&& logic, DataBase* db, Scheduler* scheduler)
  : logic(move(logic)),
    db_(db),
    scheduler_(scheduler) {}

void Transaction::begin() {
    // TODO: waitする
}

void Transaction::commit() {
    is_done = true;
    unique_lock<mutex> lock(mtx_);
    scheduler_->turn = true;
    cond_.notify_all();
    db_->commit(this);
    write_set = {};
}

void Transaction::abort() {
    is_done = true;
    unique_lock<mutex> lock(mtx_);
    scheduler_->turn = true;
    cond_.notify_all();
    // Don't call db_->commit()
    write_set = {};
}

// TODO: この中でyieldしてCPUを解放する
bool Transaction::set(Key key, int val) {
    write_set[key] = make_pair(New, val);
    return false;
}

optional<int> Transaction::get(Key key) {
    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return nullopt;
    }

    // read from the write set
    if (write_set.count(key) > 0) {
        assert(write_set[key].first == New);
        return write_set[key].second;
    }

    return db_->table[key];
}

bool Transaction::del(Key key) {
    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return true;
    }
    write_set[key] = make_pair(Delete, 0);
    return false;
}

vector<string> Transaction::keys() {
    vector<string> v;
    for (const auto& [key, _] : db_->table) {
        if (write_set.count(key) > 0 && write_set[key].first == Delete)
            continue;
        v.push_back(key);
    }
    for (const auto& [key, val] : write_set) {
        if (db_->table.count(key) <= 0 || val.first == Delete)
            continue;
        v.push_back(key);
    }
    return v;
}

bool Transaction::has_key(Key key) {
    if (write_set.count(key) <= 0) {
        return db_->table.count(key) > 0;
    }
    return (write_set[key].first == New);
}

// --------------------------------- Scheduler ---------------------------------

void Scheduler::add_tx(TransactionLogic&& logic) {
    // create transaction objects and store it
    Transaction* tx = db_->generate_tx(move(logic));
    transactions.push_back(tx);
}

void Scheduler::start() {
    // spawn transaction threads
    for (const auto& tx : transactions) {
        thread th(tx->logic.func, tx);
        threads.push_back(move(th));
    }
    unique_lock<mutex> lock(mtx_);
    cond_.wait(lock, [this]{ return turn; });

    // TODO: optimize by using queue
    for (int i = 0 ; i < transactions.size(); i++) {
        if (transactions[i]->is_done) {
            threads[i].join();
            transactions.erase(transactions.begin() + i);
            threads.erase(threads.begin() + i);
        }
    }
    turn = false;
}

// ---------------------------------- DataBase ---------------------------------

DataBase::DataBase(Scheduler* scheduler, string dumpfilename, string logfilename)
  : scheduler_(scheduler),
    dumpfilename_(dumpfilename),
    logfilename_(logfilename)
{
    LOG("Booting DB...");
    scheduler_->set_db(this);

    // 前回のDBファイルをメモリに読み出し
    ifstream ifs_dump(dumpfilename);
    string str;

    while (getline(ifs_dump, str)) {
        if (str == "") continue;
        vector<string> fields = words(str);
        assert(fields.size() == 2);
        table[fields[0]] = stoi(fields[1]);
    }

    ifs_dump.close();

    // (必要なら) crash recovery
    recover();

    fd_log_ = open(logfilename.c_str(), O_WRONLY | O_TRUNC);

    LOG("Succesfully booted DB");
}

DataBase::~DataBase() {
    LOG("Attempting a shut down of DB...");

    // checkpointing
    ofstream ofs_dump(dumpfilename_);  // dump file will be truncated
    for (const auto& [key, value] : table) {
        ofs_dump << key << " " << value << endl;
    }

    ofs_dump.close();
    close(fd_log_);

    // clear log file
    fd_log_ = open(logfilename_.c_str(), O_TRUNC);
    close(fd_log_);

    LOG("Successfully shut down DB.");
}

void DataBase::recover() {
    LOG("recover");
    ifstream ifs_log(logfilename_);
    string str;
    bool in_transaction = false;

    while (getline(ifs_log, str)) {
        if (str == "")
            continue;

       if (str == "{") {
            assert(in_transaction == false
                    && "nested transaction log is not allowed");
            in_transaction = true;
            continue;
        }
        if (str == "}") {
            assert(in_transaction == true
                    && "nested transaction log is not allowed");
            in_transaction = false;
            continue;
        }

        vector<string> fields = words(str);
        assert(fields.size() == 4);
        // checksum validation
        string str = fields[1] + fields[2] + fields[3];
        assert(((unsigned int) stol(fields[0])) == crc32(str));
        if (stoi(fields[2]) == 0) {
            // New
            table[fields[1]] = stoi(fields[3]);
        } else {
            // Delete
            table.erase(fields[1]);
        }
    }

    ifs_log.close();
}

Transaction* DataBase::generate_tx(TransactionLogic&& logic) {
    Transaction* tx = new Transaction(move(logic), this, scheduler_);
    return tx;
}

void DataBase::commit(Transaction* tx) {
    LOG("commit");

    string buf = "{\n";
    for (const auto& [key, value] : tx->write_set) {
        buf += make_log_format(value.first, key, value.second);
    }
    buf += "}\n";
    write(fd_log_, buf.c_str(), buf.size());
    fsync(fd_log_);

    // apply write_set to table
    for (const auto& [key, value] : tx->write_set) {
        if (value.first == New)
            table[key] = value.second;
        else
            table.erase(key);
    }
}

string DataBase::make_log_format(ChangeMode mode, Key key, int val) {
    string seed = key + to_string(mode) + to_string(val);
    string buf;
    buf += to_string(crc32(seed)) + " ";
    buf += key + " ";
    buf += to_string(mode) + " ";
    buf += to_string(val) + "\n";
    return buf;
}
