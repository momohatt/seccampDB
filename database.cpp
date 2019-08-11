#include "database.h"
#include "utils.h"

#include <iostream>
#include <cstdlib>
#include <cassert>
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

    ofs_log_.open(logfilename, ios_base::trunc);

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
    ofs_log_.close();

    // clear log file
    ofs_log_.open(logfilename_, ios_base::trunc);
    ofs_log_.close();

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
        assert(fields.size() == 3);
        if (stoi(fields[1]) == 0) {
            // New
            table_[fields[0]] = stoi(fields[2]);
        } else {
            // Delete
            table_.erase(fields[0]);
        }

        LOG(fields[0]);
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
    ofs_log_ << "{" << endl;
    for (const auto& [key, value] : write_set_) {
        ofs_log_ << key << " " << value.first << " " << value.second << endl;
    }
    ofs_log_ << "}" << endl << flush;

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

bool DataBase::insert(Key key, int val) {
    if (transaction_mode_) {
        if (has_key(key)) {
            cerr << "The key " << key << " already exists" << endl;
            return true;
        }
        write_set_[key] = make_pair(New, val);
        return false;
    }

    if (table_.count(key) > 0) {
        cerr << "The key " << key << " already exists" << endl;
        return true;
    }
    log_non_transaction(New, key, val);
    table_[key] = val;
    return false;
}

bool DataBase::update(Key key, int val) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return true;
        }
        write_set_[key] = make_pair(New, val);
        return false;
    }

    if (table_.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return true;
    }
    log_non_transaction(New, key, val);
    table_[key] = val;
    return false;
}

optional<int> DataBase::read(Key key) {
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
    ofs_log_ << "{\n" << key << " " << mode << " " << val << "\n}\n" << flush;
}
