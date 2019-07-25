#include <iostream>
#include <fstream>
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
    DataBase* db = new DataBase(dumpfilename, logfilename);

    db->insert("key1", 1);
    assert(db->read("key1").value() == 1);
    db->update("key1", 2);
    assert(db->read("key1").value() == 2);
    db->del("key1");
    assert(db->read("key1").has_value() == false);
    delete db;
}

void test_persistence() {
    DataBase* db1 = new DataBase(dumpfilename, logfilename);
    db1->insert("key1", 5);
    delete db1;

    DataBase* db2 = new DataBase(dumpfilename, logfilename);
    assert(db2->read("key1").value() == 5);
    delete db2;
}

void test_commit() {
    DataBase* db = new DataBase(dumpfilename, logfilename);
    db->begin();
    db->insert("key1", 35);
    assert(db->read("key1").value() == 35);
    db->del("key1");
    assert(db->read("key1").has_value() == false);
    db->commit();
    assert(db->read("key1").has_value() == false);
    delete db;
}

void test_abort() {
    DataBase* db = new DataBase(dumpfilename, logfilename);
    db->begin();
    db->insert("key1", 23);
    assert(db->read("key1").value() == 23);
    db->abort();
    assert(db->read("key1").has_value() == false);
    delete db;
}

void tryread(DataBase* db, string key) {
    optional<int> tmp = db->read(key);
    if (tmp)
        cout << tmp.value() << endl;
}

int main()
{
    TEST(test_basics);
    TEST(test_persistence);
    TEST(test_commit);
    TEST(test_abort);
    return 0;
}
