#include "parser/SQLLexer.h"
#include "parser/SQLParser.h"
#include "parser/SQLBaseVisitor.h"
#include <iomanip>

using namespace antlr4;

auto parse(const std::string& sql, SQLBaseVisitor &visitor) {
    ANTLRInputStream inputStream(sql);
    SQLLexer lexer(&inputStream);
    CommonTokenStream tokenStream(&lexer);
    SQLParser parser(&tokenStream);
    auto tree = parser.program();
    auto result = visitor.visit(tree);
    return result;
}

int main() {
    MyBitMap::initConst();
    FileManager fileManager;
    BufPageManager bufPageManager(&fileManager);
    IndexManager indexManager(&bufPageManager, &fileManager);
    RecordManager recordManager(&bufPageManager, &fileManager);
    SystemManager systemManager(&bufPageManager, &indexManager, &recordManager);
    QueryManager queryManager(&bufPageManager, &indexManager, &recordManager, &systemManager);
    SQLBaseVisitor visitor(&systemManager, &queryManager);
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(2);
    while (true) {
        if (systemManager.getDBName().empty()) std::cout << "tongDB> ";
        else std::cout << "tongDB(" + systemManager.getDBName() + ")> ";
        std::string sql;
        int c;
        while (true) {
            c = std::getchar();
            sql.push_back((char) c);
            if (c == ';') {
                std::getchar();
                break;
            }
            else if (c == '\n') {
                if (systemManager.getDBName().empty()) std::cout << "    --> ";
                else std::cout << std::setw((int) systemManager.getDBName().length() + 10) << "--> ";
            }
        }
        if (sql == "exit;") {
            if (!systemManager.getDBName().empty()) systemManager.closeDB();
            break;
        }
        else parse(sql, visitor);
    }
    return 0;
}
