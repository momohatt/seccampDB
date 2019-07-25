#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <map>
using namespace std;

class DataBase {
    public:
        using Key = string;

        void insert(Key key, int val);
        void update(Key key, int val);
        int read(Key key);
        void del(Key key);

    private:
        // TODO: 色々な型に対応
        map<Key, int> table;
};

void DataBase::insert(Key key, int val) {
    if (table.count(key) > 0) {
        cerr << "The key " << key << " already exists" << endl;
        exit(1);
    }
    table[key] = val;
}

void DataBase::update(Key key, int val) {
    if (table.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        exit(1);
    }
    table[key] = val;
}

int DataBase::read(Key key) {
    if (table.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        exit(1);
    }
    return table[key];
}

void DataBase::del(Key key) {
    if (table.count(key) <= 0) {
        cerr << "The key " << key << " doesn't exist" << endl;
        exit(1);
    }
    table.erase(key);
}

int main()
{
    DataBase db;
    db.insert("key1", 1);
    db.update("key1", 2);
    cout << db.read("key1") << endl;
    db.del("key1");
    cout << db.read("key1") << endl;

    return 0;
}
