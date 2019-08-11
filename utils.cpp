#include "utils.h"

#include <iostream>
#include <fstream>
using namespace std;

vector<string> words(const string &str) {
    vector<string> v;
    int start_pos = 0;
    bool in_word = false;

    for (int i = 0; i < str.size(); i++) {
        if (str[i] == ' ' || str[i] == '\n' || str[i] == '\t') {
            if (!in_word)
                continue;
            in_word = false;
            v.push_back(str.substr(start_pos, i - start_pos));
            continue;
        }

        if (in_word)
            continue;

        in_word = true;
        start_pos = i;
    }

    if (in_word) {
        v.push_back(str.substr(start_pos, str.size() - start_pos));
    }

    return v;
}

// for debug
void cat(string filename) {
    ifstream ifs(filename);
    string str;
    while (getline(ifs, str))
        cout << str << endl;
}

Query parse_query(string input) {
    Query q = Query();
    vector<string> fields = words(input);

    if (fields[0] == "insert") {
        assert(fields.size() == 3);
        q.cmd = Query::Insert;
        q.arg1 = fields[1];
        q.arg2 = stoi(fields[2]);
    } else if (fields[0] == "update") {
        assert(fields.size() == 3);
        q.cmd = Query::Update;
        q.arg1 = fields[1];
        q.arg2 = stoi(fields[2]);
    } else if (fields[0] == "read") {
        assert(fields.size() == 2);
        q.cmd = Query::Read;
        q.arg1 = fields[1];
    } else if (fields[0] == "delete") {
        assert(fields.size() == 2);
        q.cmd = Query::Delete;
        q.arg1 = fields[1];
    } else if (fields[0] == "begin") {
        assert(fields.size() == 1);
        q.cmd = Query::Begin;
    } else if (fields[0] == "commit") {
        assert(fields.size() == 1);
        q.cmd = Query::Commit;
    } else if (fields[0] == "abort") {
        assert(fields.size() == 2);
        q.cmd = Query::Abort;
    } else {
        cerr << "unsupported query: " << input << endl;
    }
    return q;
}