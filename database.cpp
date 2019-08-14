#include "database.h"
#include "utils.h"

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cassert>
#include <fcntl.h>  // open
#include <unistd.h>  // close
#include <stdio.h>
using namespace std;

#define LOG(x) cout << "[LOG] " << x << endl

/*

  Transaction logic:
  * redo logのみを使う
    * undoできない -> commit/abort処理完了後にDBに変更を反映
    * DBへの変更は write set に一時保存し、トランザクションを実行中のスレッド
      からの read は write set の内容を反映した結果を返す
    * redo log は write set から作れる

  Checkpointing:
  * 遅延させていたDB本体への反映を実行して log を空にする
  * 正常終了時と起動時のみに限定
  * 起動時:
    * 前回のDBファイルをメモリに読み出し
    * (必要なら) crash recovery
      * redo log の中身をメモリ上のDBに反映させる
      * DBイメージをディスクに書き出し log を空にする
  * 正常終了時:
    * メモリ上のDBをディスクに書き出して log を空にする

  Crash Recovery:
  * logを先頭から見て、トランザクションを全て反映させる

 */

// TODO:
// * write data size to log when supporting other data types than int

// ------------------------------- Persistence ---------------------------------

DataBase::DataBase(string dumpfilename, string logfilename)
  : dumpfilename_(dumpfilename),
    logfilename_(logfilename)
{
    LOG("Booting DB...");

    // 前回のDBファイルをメモリに読み出し
    ifstream ifs_dump(dumpfilename);
    string str;

    while (getline(ifs_dump, str)) {
        vector<string> fields = words(str);
        assert(fields.size() == 2);
        table_[fields[0]] = stoi(fields[1]);
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
    for (const auto& [key, value] : table_) {
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
    ifstream ifs_log(logfilename_);
    string str;
    bool in_transaction = false;

    while (getline(ifs_log, str)) {
        if (str == "")
            continue;

       if (str == "{") {
            assert(in_transaction == false && "nested transaction log is not allowed");
            in_transaction = true;
            continue;
        }
        if (str == "}") {
            assert(in_transaction == true && "nested transaction log is not allowed");
            in_transaction = false;
            continue;
        }

        vector<string> fields = words(str);
        assert(fields.size() == 4);
        // checksum validation
        string str = fields[1] + fields[2] + fields[3];
        assert(stoi(fields[0]) == crc32(str));
        if (stoi(fields[2]) == 0) {
            // New
            table_[fields[1]] = stoi(fields[3]);
        } else {
            // Delete
            table_.erase(fields[1]);
        }

        LOG(fields[1]);
    }

    ifs_log.close();
}

// ---------------------------- Transaction Logic ------------------------------

void DataBase::begin() {
    write_set_ = {};
    transaction_mode_ = true;
}

void DataBase::commit() {
    // flush write_set_ to log file
    LOG("commit");
    string buf = "{\n";
    for (const auto& [key, value] : write_set_) {
        string str = key + to_string(value.first) + to_string(value.second);
        buf += make_log_format(value.first, key, value.second);
    }
    buf += "}\n";
    write(fd_log_, buf.c_str(), buf.size());

    // single threadなのでcommit処理が失敗することはない
    // -> すぐにDB本体に書き出して良い

    // apply write_set_ to table_
    for (const auto& [key, value] : write_set_) {
        if (value.first == New)
            table_[key] = value.second;
        else
            table_.erase(key);
    }

    write_set_ = {};
    transaction_mode_ = false;
}

void DataBase::abort() {
    // write setを捨てるだけ
    write_set_ = {};
    transaction_mode_ = false;
}

// --------------------------- DataBase operations -----------------------------

bool DataBase::set(Key key, int val) {
    if (transaction_mode_) {
        write_set_[key] = make_pair(New, val);
        return false;
    }

    log_non_transaction(New, key, val);
    table_[key] = val;
    return false;
}

optional<int> DataBase::get(Key key) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return nullopt;
        }

        // read from the write set
        if (write_set_.count(key) > 0) {
            assert(write_set_[key].first == New);
            return write_set_[key].second;
        }

        return table_[key];
    }

    if (table_.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return nullopt;
    }
    return table_[key];
}

bool DataBase::del(Key key) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return true;
        }
        write_set_[key] = make_pair(Delete, 0);
        return false;
    }

    if (table_.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return true;
    }
    log_non_transaction(Delete, key, 0);
    table_.erase(key);
    return false;
}

vector<string> DataBase::keys() {
    vector<string> v;
    if (transaction_mode_) {
        assert(false);
    }

    for (const auto& [key, value] : table_) {
        v.push_back(key);
    }
    return v;
}

bool DataBase::has_key(Key key) {
    if (write_set_.count(key) <= 0) {
        return table_.count(key) > 0;
    }
    return (write_set_[key].first == New);
}

void DataBase::log_non_transaction(ChangeMode mode, Key key, int val) {
    string str = key + to_string(mode) + to_string(val);
    string buf = "{\n" + make_log_format(mode, key, val) + "}\n";
    write(fd_log_, buf.c_str(), buf.size());
}

string DataBase::make_log_format(ChangeMode mode, Key key, int val) {
    string str = key + to_string(mode) + to_string(val);
    string buf;
    buf += to_string(crc32(str)) + " ";
    buf += key + " ";
    buf += to_string(mode) + " ";
    buf += to_string(val) + "\n";
    return buf;
}
