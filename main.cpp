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
    tx->set("key2", 2);
    tx->commit();
    vector<string> keys = tx->keys();
    for (const auto& key : keys)
        cout << key << endl;
}

// void transaction2(Transaction* tx) {
//     tx->set("key3", 3);
//     tx->commit();
// }

int main()
{
    Transaction* tx1 = db.generate_tx();
    thread th_tx1(transaction1, tx1);
    scheduler.add_tx(move(th_tx1), tx1);

    // Transaction* tx2 = db.generate_tx();
    // thread th_tx2(transaction2, tx2);
    // scheduler.add_tx(move(th_tx2), tx2);

    scheduler.run();
    return 0;
}
