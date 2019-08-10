#include "utils.h"

#include <iostream>
#include <sstream>
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
