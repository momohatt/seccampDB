#include <iostream>
#include <fstream>
#include <optional>
#include <string>
#include <unistd.h>
#include <cassert>
#include <memory>
#include "database.h"
#include "utils.h"

using namespace std;

int main()
{
    const string dumpfilename = ".seccampDB_dump";
    const string logfilename = ".seccampDB_log";

    unique_ptr<DataBase> db(new DataBase(dumpfilename, logfilename));
    string input;

    while (1) {
        cout << "seccampDB> " << flush;
        getline(cin, input);
        if (cin.eof()) {
            cout << endl << flush;
            break;
        }
        if (input == "") {
            continue;
        }
        Query query = parse_query(input);

        switch (query.cmd) {
            case Query::Set:
                db->set(query.arg1, query.arg2);
                break;
            case Query::Get:
                {
                    optional<int> x = db->get(query.arg1);
                    if (x.has_value())
                        cout << x.value() << endl;
                    else
                        cout << "(nil)" << endl;
                }
                break;
            case Query::Del:
                db->del(query.arg1);
                break;
            case Query::Begin:
                db->begin();
                break;
            case Query::Commit:
                db->commit();
                break;
            case Query::Abort:
                db->abort();
                break;
            case Query::Keys:
                {
                    vector<string> keys = db->keys();
                    for (const auto& key : keys) {
                        cout << key << endl;
                    }
                }
                break;
        }
    }

    db.reset();
    return 0;
}
