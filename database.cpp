#include <iostream>
#include <fstream>
#include <optional>
#include <string>
#include "database.h"
using namespace std;

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
  * log を先頭からなめて，トランザクション毎にあった/なかったのどちらにするか決定
  * トランザクションがあったことにする条件:
    * トランザクションの commit log が記録されている
    * トランザクションが依存していた全てのトランザクションが commit 扱いとなっている

 */

// ------------------------------- Persistence ---------------------------------

DataBase::DataBase(string dumpfilename, string logfilename)
  : dumpfilename_(dumpfilename),
    ofs_log_(logfilename, ios_base::app)
{
    // 前回のDBファイルをメモリに読み出し
    ifstream ifs_dump(dumpfilename);
    string str;

    while (getline(ifs_dump, str)) {
        size_t pos = str.find(" ");
        assert(pos > 0);
        table_[str.substr(0, pos)] = stoi(str.substr(pos + 1, str.size() - pos));
    }

    ifs_dump.close();

    // (必要なら) crash recovery
    recover(logfilename);
}

DataBase::~DataBase() {
    ofstream ofs_dump(dumpfilename_);  // dump file will be truncated
    for (const auto& [key, value] : table_) {
        ofs_dump << key << " " << value << endl;
    }

    ofs_dump.close();
    ofs_log_.close();
}

void DataBase::recover(string logfilename) {
    ifstream ifs_log(logfilename);
    ifs_log.close();
}

// ---------------------------- Transaction Logic ------------------------------

void DataBase::begin() {
    write_set_ = {};
    transaction_mode_ = true;
}

void DataBase::commit() {
    // flush write_set_ to log file

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

int DataBase::insert(Key key, int val) {
    if (transaction_mode_) {
        if (has_key(key)) {
            cerr << "The key " << key << " already exists" << endl;
            return 1;
        }
        write_set_[key] = make_pair(New, val);
        return 0;
    }

    if (table_.count(key) > 0) {
        cerr << "The key " << key << " already exists" << endl;
        return 1;
    }
    table_[key] = val;
    return 0;
}

int DataBase::update(Key key, int val) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return 1;
        }
        write_set_[key] = make_pair(New, val);
        return 0;
    }

    if (table_.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return 1;
    }
    table_[key] = val;
    return 0;
}

optional<int> DataBase::read(Key key) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return 1;
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

int DataBase::del(Key key) {
    if (transaction_mode_) {
        if (!has_key(key)) {
            cerr << "The key " << key << " doesn't exist" << endl;
            return 1;
        }
        write_set_[key] = make_pair(Delete, 0);
        return 0;
    }

    if (table_.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        return 1;
    }
    table_.erase(key);
    return 0;
}

bool DataBase::has_key(Key key) {
    if (write_set_.count(key) <= 0) {
        return table_.count(key) > 0;
    }
    return (write_set_[key].first == New);
}
