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
    tx->Begin();
    tx->Set("key1", 1);
    tx->Set("key2", 2);
    tx->Commit();
    // vector<string> keys = tx->keys();
    // for (const auto& key : keys)
    //     cout << key << endl;
}

void transaction2(Transaction* tx) {
    tx->Begin();
    tx->Set("key3", 3);
    tx->Commit();
}

void transaction3(Transaction* tx) {
    tx->Begin();
    tx->Set("key4", 4);
    tx->Abort();
}

// transaction logic
int main()
{
    scheduler.add_tx(move(transaction1));
    scheduler.add_tx(move(transaction2));
    scheduler.add_tx(move(transaction3));

    scheduler.Start();
    return 0;
}
