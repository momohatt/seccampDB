#include <iostream>
#include <fstream>
#include <cassert>
#include <memory>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <thread>
#include "utils.h"
#include "database.h"
using namespace std;

#define TEST(x) init(); x(); cout << "\e[32mpassed " << #x << "\e[m" << endl;

const string dumpfilename = ".seccampDB_dump";
const string logfilename = ".seccampDB_log";

void init() {
    // initialize backup files
    ofstream ofs_dump(dumpfilename, ofstream::trunc);
    ofstream ofs_log(logfilename, ofstream::trunc);
    ofs_dump.close();
    ofs_log.close();
}

void tx_basics1(Transaction* tx) {
    tx->set("key1", 1);  // この中でyieldしたりする
    tx->set("key2", 2);
    tx->commit();
}

void tx_basics2(Transaction* tx) {
    tx->get("key1");
}

void test_basics() {
    Scheduler scheduler = Scheduler();
    DataBase db = DataBase(&scheduler, dumpfilename, logfilename);
    Transaction* tx1 = db.generate_tx();
    thread th_tx1(tx_basics1, tx1);
    scheduler.add_tx(move(th_tx1), tx1);
    scheduler.run();

    // TODO: improve assertion
    assert(db.table["key1"] == 1);
    assert(db.table["key2"] == 2);
}

void test_persistence() {
    Scheduler scheduler = Scheduler();
    unique_ptr<DataBase> db1(new DataBase(&scheduler, dumpfilename, logfilename));
    Transaction* tx1 = db1->generate_tx();
    thread th_tx1(tx_basics1, tx1);
    scheduler.add_tx(move(th_tx1), tx1);
    scheduler.run();
    db1.reset();

    unique_ptr<DataBase> db2(new DataBase(&scheduler, dumpfilename, logfilename));
    // TODO: improve assertion
    assert(db2->table["key1"] == 1);
    db2.reset();
}

// void test_commit() {
//     unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
//     db->begin();
//     db->set("key1", 35);
//     assert(db->get("key1").value() == 35);
//     db->del("key1");
//     assert(db->get("key1").has_value() == false);
//     db->commit();
//     assert(db->get("key1").has_value() == false);
//     db.reset();
// }
//
// void test_abort() {
//     unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
//     db->begin();
//     db->set("key1", 23);
//     assert(db->get("key1").value() == 23);
//     db->abort();
//     assert(db->get("key1").has_value() == false);
//     db.reset();
// }
//
// void test_recover() {
//     pid_t pid;
//     pid = fork();
//     if (pid == -1) {
//         perror("cannot fork");
//         return;
//     }
//
//     if (pid == 0) {
//         // child process
//         unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
//         db->set("key1", 35);
//         db->set("key2", 40);
//         db->begin();
//         int x = db->get("key1").value();
//         db->set("key2", x);
//         db->del("key1");
//         db->commit();
//         exit(0);
//     } else {
//         // parent process (pid : pid of child proc)
//         sleep(1);
//         cat(logfilename);
//         unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
//         assert(db->get("key1").has_value() == false);
//         assert(db->get("key2").value() == 35);
//         db.reset();
//     }
// }

void test_utils_parse_query() {
}

int main()
{
    TEST(test_basics);
    TEST(test_persistence);
    // TEST(test_commit);
    // TEST(test_abort);
    // TEST(test_recover);
    init();
    return 0;
}
