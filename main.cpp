#include <iostream>
#include <optional>
#include <string>
#include <unistd.h>
#include <cassert>
#include <memory>
#include <thread>
#include "database.h"
#include "utils.h"

using namespace std;

const string dumpfilename = ".seccampDB_dump";
const string logfilename = ".seccampDB_log";
Scheduler scheduler = Scheduler();
DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

void transaction1(Transaction* tx) {
    tx->set("key1", 1);  // この中でyieldしたりする
    tx->set("key2", 1);
    tx->commit();
}

int main()
{
    string input;

    // トランザクションオブジェクトを生成
    // スケジューラに登録
    Transaction* tx = db.generate_tx();

    // トランザクションスレッドを生成
    thread th_tx(transaction1, tx);
    scheduler.add_tx(move(th_tx), tx);

    scheduler.run();
    return 0;
}
