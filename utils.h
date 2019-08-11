#ifndef __UTILS_H__
#define __UTILS_H__

#include <vector>
#include <string>

using namespace std;

vector<string> words(const string &str);

// for debug
void cat(string filename);

class Query {
    public:
        enum Commands {
            Insert,
            Update,
            Read,
            Delete,
            Begin,
            Commit,
            Abort,
            Keys
        };
        Commands cmd;
        string arg1 = "";
        int arg2 = 0;
};

Query parse_query(string input);

#endif  // __UTILS_H__
