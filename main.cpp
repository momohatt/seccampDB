#include <iostream>
#include <fstream>
#include <optional>
#include <string>
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
    DataBase *db = new DataBase(dumpfilename, logfilename);

    db->begin();
    db->insert("key1", 1);
    db->update("key1", 2);
    tryread(db, "key1");
    db->commit();
    tryread(db, "key1");
    delete db;

    // cat(dumpfilename);

    // DataBase *db2 = new DataBase(dumpfilename, logfilename);
    // tryread(db2, "key1");
    // db2->del("key1");
    // db2->insert("key2", 0);
    // tryread(db2, "key1");

    // delete db2;

    return 0;
}
