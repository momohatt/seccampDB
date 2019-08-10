#include <iostream>
#include <fstream>
#include <optional>
#include <string>
#include <unistd.h>
#include <cassert>
#include <memory>
#include "database.h"

using namespace std;

// for debug
void cat(string filename) {
    ifstream ifs(filename);
    string str;
    while (getline(ifs, str))
        cout << str << endl;
}

void tryread(DataBase* db, string key) {
    optional<int> tmp = db->read(key);
    if (tmp)
        cout << tmp.value() << endl;
}

int main()
{
    const string dumpfilename = ".seccampDB_dump";
    const string logfilename = ".seccampDB_log";

    unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
    db->begin();
    db->insert("key1", 35);
    assert(db->read("key1").value() == 35);
    db->del("key1");
    assert(db->read("key1").has_value() == false);
    db->commit();
    assert(db->read("key1").has_value() == false);
    db->insert("key2", 40);
    sleep(30);
    assert(db->read("key2").value() == 40);
    db.reset();

    return 0;
}
