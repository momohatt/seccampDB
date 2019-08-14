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
    tx->begin();
    tx->set("key1", 1);
    tx->set("key2", 2);
    tx->commit();
    // vector<string> keys = tx->keys();
    // for (const auto& key : keys)
    //     cout << key << endl;
}

void transaction2(Transaction* tx) {
    tx->begin();
    tx->set("key3", 3);
    tx->commit();
}

// transaction logic
int main()
{
    TransactionLogic tl1 = TransactionLogic(transaction1);
    scheduler.add_tx(move(tl1));

    // TransactionLogic tl2 = TransactionLogic(transaction1);
    // scheduler.add_tx(move(tl2));

    scheduler.start();
    return 0;
}
