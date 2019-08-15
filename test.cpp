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

void assert_value(DataBase* db, Key key, int expected_value) {
    assert(db->table[key].value == expected_value);
}

void tx_basics1(Transaction* tx) {
    tx->begin();
    tx->set("key1", 1);
    tx->set("key2", 2);
    tx->commit();
}

void tx_basics2(Transaction* tx) {
    tx->begin();
    tx->set("key1", 1);
    optional<int> result = tx->get("key1");
    tx->set("key1", result.value() + 1);
    tx->commit();
}

void tx_abort(Transaction* tx) {
    tx->begin();
    tx->set("key1", 1);
    tx->abort();
}

void tx_read_read_conflict1(Transaction* tx) {
    tx->begin();
    tx->get_until_success("key1");
    tx->get_until_success("key2");
    tx->commit();
}

void tx_read_read_conflict2(Transaction* tx) {
    tx->begin();
    tx->get_until_success("key2");
    tx->get_until_success("key1");
    tx->commit();
}

//------------------------------------------------------------------------------

void test_basics1() {
    Scheduler scheduler = Scheduler();
    DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

    scheduler.add_tx(move(tx_basics1));
    scheduler.start();

    assert_value(&db, "key1", 1);
    assert_value(&db, "key2", 2);
}

void test_basics2() {
    Scheduler scheduler = Scheduler();
    DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

    scheduler.add_tx(move(tx_basics2));
    scheduler.start();

    assert_value(&db, "key1", 2);
}

void test_persistence() {
    Scheduler scheduler = Scheduler();
    unique_ptr<DataBase> db1(new DataBase(&scheduler, dumpfilename, logfilename));

    scheduler.add_tx(move(tx_basics1));
    scheduler.start();
    db1.reset();

    unique_ptr<DataBase> db2(new DataBase(&scheduler, dumpfilename, logfilename));
    assert_value(db2.get(), "key1", 1);
    assert_value(db2.get(), "key2", 2);
    db2.reset();
}

void test_abort() {
    Scheduler scheduler = Scheduler();
    DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

    scheduler.add_tx(move(tx_abort));
    scheduler.start();

    assert_value(&db, "key1", 0);
    assert_value(&db, "key2", 0);
}

void test_recover() {
    pid_t pid;
    pid = fork();
    if (pid == -1) {
        perror("cannot fork");
        return;
    }

    if (pid == 0) {
        // child process
        Scheduler scheduler = Scheduler();
        DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

        scheduler.add_tx(move(tx_basics1));
        scheduler.start();
        exit(0);
    } else {
        // parent process (pid : pid of child proc)
        sleep(1);
        cat(logfilename);
        Scheduler scheduler = Scheduler();
        DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

        assert_value(&db, "key1", 1);
        assert_value(&db, "key2", 2);
    }
}

void test_read_read_conflict() {
    // shouldn't conflict
    Scheduler scheduler = Scheduler();
    DataBase db = DataBase(&scheduler, dumpfilename, logfilename);

    scheduler.add_tx(move(tx_basics1));
    scheduler.add_tx(move(tx_read_read_conflict1));
    scheduler.add_tx(move(tx_read_read_conflict2));
    scheduler.start();

    assert_value(&db, "key1", 1);
    assert_value(&db, "key2", 2);
}

int main()
{
    TEST(test_basics1);
    TEST(test_basics2);
    TEST(test_persistence);
    TEST(test_abort);
    TEST(test_recover);
    TEST(test_read_read_conflict);
    init();
    return 0;
}
