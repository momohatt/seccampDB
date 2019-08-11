#include <iostream>
#include <fstream>
#include <cassert>
#include <memory>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
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

void test_basics() {
    unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
    db->insert("key1", 1);
    assert(db->read("key1").value() == 1);
    db->update("key1", 2);
    assert(db->read("key1").value() == 2);
    db->del("key1");
    assert(db->read("key1").has_value() == false);
    db.reset();
}

void test_persistence() {
    unique_ptr<DataBase> db1(new DataBase(dumpfilename, logfilename));
    db1->insert("key1", 5);
    db1.reset();

    unique_ptr<DataBase> db2(new DataBase(dumpfilename, logfilename));
    assert(db2->read("key1").value() == 5);
    db2.reset();
}

void test_commit() {
    unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
    db->begin();
    db->insert("key1", 35);
    assert(db->read("key1").value() == 35);
    db->del("key1");
    assert(db->read("key1").has_value() == false);
    db->commit();
    assert(db->read("key1").has_value() == false);
    db.reset();
}

void test_abort() {
    unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
    db->begin();
    db->insert("key1", 23);
    assert(db->read("key1").value() == 23);
    db->abort();
    assert(db->read("key1").has_value() == false);
    db.reset();
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
        unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
        db->insert("key1", 35);
        db->insert("key2", 40);
        db->begin();
        int x = db->read("key1").value();
        db->update("key2", x);
        db->del("key1");
        db->commit();
        sleep(10);  // この間に死ぬ
    } else {
        // parent process (pid : pid of child proc)
        sleep(1);
        kill(pid, SIGKILL);
        cout << "killed child process" << endl;
        cat(logfilename);
        unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
        assert(db->read("key1").has_value() == false);
        assert(db->read("key2").value() == 35);
        db.reset();
    }
}

void test_utils_parse_query() {
}

int main()
{
    TEST(test_basics);
    TEST(test_persistence);
    TEST(test_commit);
    TEST(test_abort);
    TEST(test_recover);
    init();
    return 0;
}
