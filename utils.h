#ifndef __UTILS_H__
#define __UTILS_H__

#include <queue>
#include <string>
#include <vector>

using namespace std;

vector<string> words(const string &str);

// for debug
void cat(string filename);

class Query {
    public:
        enum Commands {
            Set,
            Get,
            Del,
            Begin,
            Commit,
            Abort,
            Keys,
            Unknown,
        };
        Commands cmd = Commands::Unknown;
        string arg1 = "";
        int arg2 = 0;
};

Query parse_query(string input);

unsigned int crc32(string str);

// https://stackoverflow.com/questions/1259099/stdqueue-iteration
template<typename T, typename Container=std::deque<T> >
class iterable_queue : public std::queue<T,Container>
{
public:
    typedef typename Container::iterator iterator;
    typedef typename Container::const_iterator const_iterator;

    iterator begin() { return this->c.begin(); }
    iterator end() { return this->c.end(); }
    const_iterator begin() const { return this->c.begin(); }
    const_iterator end() const { return this->c.end(); }
};

#endif  // __UTILS_H__
