#include "database.h"

#include <iostream>
#include <mutex>
#include <fstream>
#include <cstdlib>
#include <fcntl.h>  // open
#include <unistd.h>  // close
#include <stdio.h>
using namespace std;

#define LOG   printf("[LOG]          %s::%d\n", __FUNCTION__, __LINE__);
#define TXLOG printf("[LOG](txid: %d) %s::%d\n", id_, __FUNCTION__, __LINE__);
#define UNREACHABLE \
    fprintf(stderr, "Shouldn't reach here: %s::%d\n", __FUNCTION__, __LINE__);

// -------------------------------- Transaction --------------------------------

mutex giant_mtx_;

Transaction::Transaction(
        int id, Transaction::Logic logic, DataBase* db, Scheduler* scheduler)
  : logic(move(logic)),
    id_(id),
    db_(db),
    scheduler_(scheduler) {}

void Transaction::begin() {
    TXLOG;
    unique_lock<mutex> lock(giant_mtx_);
    lock_ = move(lock);
    wait();
}

void Transaction::commit() {
    TXLOG;
    db_->apply_tx(this);
    for (const auto& key : write_log_) {
        scheduler_->log(id_, key, Write);
    }
    finish();
}

void Transaction::abort() {
    TXLOG;
    finish();
}

bool Transaction::set(Key key, int val) {
    TXLOG;

    if (db_->table.count(key) > 0) {
        while (!db_->get_lock(this, key, Write)) {
            wait();
        }
    }
    write_log_.push_back(key);
    write_set[key] = make_pair(New, val);
    wait();
    return false;
}

optional<int> Transaction::get(Key key) {
    TXLOG;

    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        wait();
        return nullopt;
    }

    // read from the write set
    if (write_set.count(key) > 0) {
        if (write_set[key].first != New) {
            UNREACHABLE;
            return nullopt;
        }
        // db_->get_lock(this, key, DataBase::Read);
        wait();
        scheduler_->log(id_, key, Read);
        return write_set[key].second;
    }

    while (!db_->get_lock(this, key, Read)) {
        wait();
    }
    lock_set.push_back(key);
    wait();
    scheduler_->log(id_, key, Read);
    return db_->table[key].value;
}

bool Transaction::del(Key key) {
    TXLOG;

    if (!has_key(key)) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return true;
    }
    while (!db_->get_lock(this, key, Write)) {
        wait();
    }
    write_log_.push_back(key);
    write_set[key] = make_pair(Delete, 0);
    wait();
    return false;
}

vector<string> Transaction::keys() {
    TXLOG;

    vector<string> v;
    for (const auto& entry : db_->table) {
        Key key = entry.first;
        if (write_set.count(key) > 0 && write_set[key].first == Delete)
            continue;
        v.push_back(key);
    }
    for (const auto& [key, val] : write_set) {
        if (db_->table.count(key) <= 0 || val.first == Delete)
            continue;
        v.push_back(key);
    }

    wait();
    return v;
}

int Transaction::get_until_success(Key key) {
    TXLOG;
    optional<int> tmp = get(key);
    while (!tmp.has_value()) {
        wait();
        tmp = get(key);
    }
    wait();
    return tmp.value();
}

void Transaction::wait() {
    scheduler_->notify();
    turn_ = false;
    cv_.wait(lock_, [this]{ return turn_; });
}

void Transaction::finish() {
    for (const auto& entry : write_set) {
        Key key = entry.first;
        db_->table[key].nlock = 0;
    }
    for (const auto& key : lock_set) {
        db_->table[key].nlock = 0;
    }
    write_set = {};
    is_done = true;
    turn_ = false;
    lock_.unlock();
    scheduler_->notify();
}

bool Transaction::has_key(Key key) {
    if (write_set.count(key) <= 0) {
        return db_->table.count(key) > 0;
    }
    return (write_set[key].first == New);
}

// --------------------------------- Scheduler ---------------------------------

Scheduler::~Scheduler() {
    // Emit conflict graph to a file
    ofstream ofs_graph(".seccampDB_graph");
    for (const auto& log : io_log_) {
        ofs_graph<< log.id << " " << log.key << " " <<
                ((log.op == Read) ? 'r' : 'w') << endl;
    }
    ofs_graph.close();
    ConflictGraph graph;
}

void Scheduler::add_tx(Transaction::Logic logic) {
    LOG;
    // create transaction objects and store it
    unique_ptr<Transaction> tx = db_->generate_tx(move(logic));
    transactions.push(move(tx));
}

void Scheduler::start() {
    // spawn transaction threads
    unique_lock<mutex> lock(giant_mtx_);
    lock_ = move(lock);
    LOG;
    for (const auto& tx : transactions) {
        thread th(tx->logic, tx.get());
        tx->set_thread(move(th));
    }
    run();
}

void Scheduler::run() {
    while (!transactions.empty()) {
        unique_ptr<Transaction> tx = move(transactions.front());
        transactions.pop();
        wait(tx.get());

        if (tx->is_done) {
            tx->terminate();
            tx.reset();
            continue;
        }
        transactions.push(move(tx));
    }
}

void Scheduler::wait(Transaction* tx) {
    tx->notify();
    turn_ = false;
    cv_.wait(lock_, [this]{ return turn_; });
}

Scheduler::Log::Log(int id, Key key, BaseOp op)
  : id(id), key(key), op(op) {}

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
        if (fields.size() != 2) {
            UNREACHABLE;
            exit(1);
        }
        table[fields[0]].value = stoi(fields[1]);
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
    for (const auto& [key, v] : table) {
        ofs_dump << key << " " << v.value << endl;
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
    string line;
    vector<string> buf;
    bool in_transaction = false;

    while (getline(ifs_log, line)) {
        if (line == "")
            continue;

       if (line == "{") {
            if (in_transaction) {
                UNREACHABLE;
                return;
            }
            in_transaction = true;
            continue;
        }
        if (line == "}") {
            if (!in_transaction) {
                UNREACHABLE;
                return;
            }
            in_transaction = false;
            continue;
        }

        buf.push_back(line);
    }

    DBDiff diff = {};
    deserialize(diff, buf);
    for (const auto& [key, value] : diff) {
        if (value.first == New) {
            table[key].value = value.second;
        } else {
            table.erase(key);
        }
    }

    ifs_log.close();
}

unique_ptr<Transaction> DataBase::generate_tx(Transaction::Logic logic) {
    unique_ptr<Transaction> tx(
            new Transaction(id_counter_++, move(logic), this, scheduler_));
    return tx;
}

bool DataBase::get_lock(Transaction* tx, Key key, BaseOp locktype) {
    if (table.count(key) <= 0) {
        UNREACHABLE;
        return false;
    }

    if (vexists(tx->lock_set, key)) {
        return ((locktype == Write && table[key].nlock == -1) ||
                (locktype == Read && table[key].nlock > 0));
    }

    if (locktype == Write) {
        if (table[key].nlock != 0)
            return false;
        table[key].nlock = -1;
        tx->lock_set.push_back(key);
        return true;
    }

    if (table[key].nlock < 0)
        return false;

    table[key].nlock++;
    tx->lock_set.push_back(key);
    return true;
}

void DataBase::apply_tx(Transaction* tx) {
    string buf = serialize(tx->write_set);
    size_t nbytes_written = 0;
    while (nbytes_written < buf.size()) {
        nbytes_written += write(fd_log_,
                buf.c_str() + nbytes_written,
                buf.size() - nbytes_written);
    }
    fsync(fd_log_);

    // apply write_set to table
    for (const auto& [key, value] : tx->write_set) {
        if (value.first == New)
            table[key].value = value.second;
        else
            table.erase(key);
    }
}

string DataBase::serialize(DBDiff diff) {
    string buf = "{\n";
    for (const auto& [key, value] : diff) {
        buf += make_log_format(value.first, key, value.second);
    }
    buf += "}\n";
    return buf;
}

void DataBase::deserialize(DBDiff& diff, vector<string> buf) {
    for (const auto& line : buf) {
        vector<string> fields = words(line);
        if (fields.size() != 4) {
            diff = {};
            return;
        }
        // checksum validation
        string str = fields[1] + fields[2] + fields[3];
        if (((unsigned int) stol(fields[0])) != crc32(str)) {
            diff = {};
            return;
        }
        diff[fields[1]] = make_pair(
                stoi(fields[2]) ? Delete : New,
                stoi(fields[3]));
    }
    return;
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

ConflictGraph::ConflictGraph() {
    const string filename = ".seccampDB_graph";
    ifstream ifs_graph(filename);
    string line;
    map<Key, vector<pair<int, BaseOp>>> tbl = {};

    while (getline(ifs_graph, line)) {
        vector<string> fields = words(line);
        if (fields.size() != 3) {
            UNREACHABLE;
            return;
        }
        if (!vexists(node_, stoi(fields[0])))
            node_.push_back(stoi(fields[0]));
        tbl[fields[1]].emplace_back(
                stoi(fields[0]),
                fields[2] == "w" ? Write : Read);
    }

    // TODO: construct edge_

    ifs_graph.close();
}
