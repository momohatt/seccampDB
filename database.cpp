#include "database.h"

#include <iostream>
#include <mutex>
#include <fstream>
#include <cstdlib>
#include <cassert>
#include <fcntl.h>  // open
#include <unistd.h>  // close
#include <stdio.h>
using namespace std;

#define LOG cout << "[LOG](tid: " << this_thread::get_id() << ") " << __FUNCTION__ << "::" << __LINE__ << endl

// -------------------------------- Transaction --------------------------------

mutex giant_mtx_;

Transaction::Transaction(
        Transaction::Logic logic, DataBase* db, Scheduler* scheduler)
  : logic(move(logic)),
    db_(db),
    scheduler_(scheduler) {}

void Transaction::Begin() {
    LOG;
    unique_lock<mutex> lock(giant_mtx_);
    lock_ = move(lock);
    Wait();
}

void Transaction::Commit() {
    LOG;

    db_->apply_tx(this);
    write_set = {};
    is_done = true;

    scheduler_->turn = true;
    scheduler_->cond_tx_done_.notify_one();
    lock_.unlock();
}

void Transaction::Abort() {
    LOG;

    write_set = {};
    is_done = true;

    scheduler_->turn = true;
    scheduler_->cond_tx_done_.notify_one();
    lock_.unlock();
}

// TODO: この中でyieldしてCPUを解放する
bool Transaction::Set(Key key, int val) {
    LOG;

    write_set[key] = make_pair(New, val);
    Wait();
    return false;
}

optional<int> Transaction::Get(Key key) {
    LOG;

    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return nullopt;
    }

    // read from the write set
    if (write_set.count(key) > 0) {
        assert(write_set[key].first == New);
        return write_set[key].second;
    }

    Wait();
    return db_->table[key];
}

bool Transaction::Del(Key key) {
    LOG;

    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return true;
    }
    write_set[key] = make_pair(Delete, 0);
    Wait();
    return false;
}

vector<string> Transaction::Keys() {
    LOG;

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
    Wait();
    return v;
}

void Transaction::Wait() {
    scheduler_->turn = true;
    scheduler_->cond_tx_done_.notify_one();
    cv_.wait(lock_, [this]{ return turn; });
    turn = false;
}

bool Transaction::has_key(Key key) {
    if (write_set.count(key) <= 0) {
        return db_->table.count(key) > 0;
    }
    return (write_set[key].first == New);
}

// --------------------------------- Scheduler ---------------------------------

void Scheduler::add_tx(Transaction::Logic logic) {
    LOG;
    // create transaction objects and store it
    Transaction* tx = db_->generate_tx(move(logic));
    transactions.push(tx);
}

void Scheduler::Start() {
    // spawn transaction threads
    {
        unique_lock<mutex> lock(giant_mtx_);
        LOG;
        for (const auto& tx : transactions) {
            thread th(tx->logic, tx);
            tx->set_thread(move(th));
        }
    }
    Run();
}

void Scheduler::Run() {
    while (!transactions.empty()) {
        LOG;
        Transaction* tx = transactions.front();
        transactions.pop();
        tx->Notify();

        unique_lock<mutex> lock(giant_mtx_);
        cond_tx_done_.wait(lock, [this]{ return turn; });
        tx->turn = false;
        if (tx->is_done) {
            tx->Terminate();
            continue;
        }
        transactions.push(tx);
        turn = false;
    }
}

// ---------------------------------- DataBase ---------------------------------

DataBase::DataBase(Scheduler* scheduler, string dumpfilename, string logfilename)
  : scheduler_(scheduler),
    dumpfilename_(dumpfilename),
    logfilename_(logfilename)
{
    LOG;
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
}

DataBase::~DataBase() {
    LOG;

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
}

void DataBase::recover() {
    LOG;
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

Transaction* DataBase::generate_tx(Transaction::Logic logic) {
    Transaction* tx = new Transaction(move(logic), this, scheduler_);
    return tx;
}

void DataBase::apply_tx(Transaction* tx) {
    LOG;

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
