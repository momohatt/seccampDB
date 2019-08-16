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

void transaction3(Transaction* tx) {
    tx->begin();
    tx->set("key4", 4);
    tx->abort();
}

void transaction4(Transaction* tx) {
    tx->begin();
    int x = tx->get_until_success("key1");
    tx->set("key2", x + 10);
    tx->commit();
}

void transaction5(Transaction* tx) {
    tx->begin();
    int x = tx->get_until_success("key2");
    tx->set("key1", x + 1);
    tx->commit();
}

void transaction_huge(int n, Transaction* tx) {
    tx->begin();
    for (int i = 0; i < 100; i++) {
        tx->set("key" + to_string(i), n);
    }
    tx->commit();
}

void transaction_huge_validate(Transaction* tx) {
    tx->begin();
    for (int i = 0; i < 100; i++) {
        Key key = "key" + to_string(i);
        int x = tx->get_until_success(key);
        cout << key << " " << x << endl;
    }
    tx->commit();
}

// transaction logic
int main()
{
    // scheduler.add_tx(move(transaction1));
    // // scheduler.add_tx(move(transaction2));
    // // scheduler.add_tx(move(transaction3));
    // scheduler.add_tx(move(transaction4));
    // scheduler.add_tx(move(transaction4));

    for (int n = 0; n < 1000; n++) {
        scheduler.add_tx(move([n](Transaction* tx) { transaction_huge(n, tx); }));
    }

    scheduler.start();
    return 0;
}
