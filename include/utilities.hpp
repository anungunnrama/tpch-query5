#include <sstream>
#include <vector>
#include <string>
#include <fstream>
#include <map>

std::vector<std::string> splitPipe(const std::string& line)
{
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string item;

    while (std::getline(ss, item, '|'))
        fields.push_back(item);

    return fields;
}


bool readTable(const std::string& file, const std::vector<std::string>& columns, std::vector<std::map<std::string, std::string>>& out)
{
    std::ifstream in(file);
    if (!in) return false;

    std::string line;
    while (std::getline(in, line))
    {
        auto fields = splitPipe(line);
        if (fields.size() < columns.size()) return false;

        std::map<std::string, std::string> row;
        for (size_t i = 0; i < columns.size(); ++i)
            row[columns[i]] = fields[i];

        out.push_back(std::move(row));
    }
    return true;
}

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, last - first + 1);
}


std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(trim(token));
    }
    return tokens;
}


